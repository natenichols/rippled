//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2020 Ripple Labs Inc.

    Permission to use, copy, modify, and/or distribute this software for any
    purpose  with  or without fee is hereby granted, provided that the above
    copyright notice and this permission notice appear in all copies.

    THE  SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
    WITH  REGARD  TO  THIS  SOFTWARE  INCLUDING  ALL  IMPLIED  WARRANTIES  OF
    MERCHANTABILITY  AND  FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
    ANY  SPECIAL ,  DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
    WHATSOEVER  RESULTING  FROM  LOSS  OF USE, DATA OR PROFITS, WHETHER IN AN
    ACTION  OF  CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
    OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/
//==============================================================================

#include <ripple/app/reporting/ReportingETL.h>

#include <ripple/core/Pg.h>
#include <ripple/json/json_reader.h>
#include <ripple/json/json_writer.h>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <cstdlib>
#include <iostream>
#include <string>

namespace ripple {

void
ReportingETL::startWriter()
{
    writer_ = std::thread{[this]() {
        std::shared_ptr<SLE> sle;
        size_t num = 0;
        // TODO: if this call blocks, flushDirty in the meantime
        while (not stopping_ and (sle = writeQueue_.pop()))
        {
            assert(sle);
            // TODO get rid of this conditional
            if (!ledger_->exists(sle->key()))
                ledger_->rawInsert(sle);

            if (flushInterval_ != 0 and (num % flushInterval_) == 0)
            {
                JLOG(journal_.debug())
                    << "Flushing! key = " << strHex(sle->key());
                ledger_->stateMap().flushDirty(
                    hotACCOUNT_NODE, ledger_->info().seq);
            }
            ++num;
        }
        /*
        if (not stopping_)
            ledger_->stateMap().flushDirty(
                hotACCOUNT_NODE, ledger_->info().seq);
                */
    }};
}

void
ReportingETL::loadInitialLedger()
{
    if (ledger_)
    {
        JLOG(journal_.info())
            << "Ledger was loaded from database. Skipping download";
        // The ledger was already loaded. This happens if --load is passed
        // on the command line
        return;
    }

    org::xrpl::rpc::v1::GetLedgerResponse response;
    if (not fetchLedger(response, false))
        return;
    std::vector<TxMeta> metas;
    updateLedger(response, metas);

    auto start = std::chrono::system_clock::now();

    startWriter();

    loadBalancer_.loadInitialLedger(ledger_, writeQueue_);
    joinWriter();
    // TODO handle case when there is a network error (other side dies)
    // Shouldn't try to flush in that scenario
    if (not stopping_)
    {
        flushLedger();
        storeLedger();
        if (app_.config().usePostgresTx())
            writeToPostgres(ledger_->info(), metas);
    }
    auto end = std::chrono::system_clock::now();
    JLOG(journal_.debug()) << "Time to download and store ledger = "
                           << ((end - start).count()) / 1000000000.0
                           << " nanoseconds";
}

void
ReportingETL::joinWriter()
{
    std::shared_ptr<SLE> null;
    writeQueue_.push(null);
    writer_.join();
}

void
ReportingETL::flushLedger()
{
    // These are recomputed in setImmutable
    auto& accountHash = ledger_->info().accountHash;
    auto& txHash = ledger_->info().txHash;
    auto& hash = ledger_->info().hash;

    auto start = std::chrono::system_clock::now();

    ledger_->setImmutable(app_.config(), false);

    auto numFlushed =
        ledger_->stateMap().flushDirty(hotACCOUNT_NODE, ledger_->info().seq);

    auto numTxFlushed =
        ledger_->txMap().flushDirty(hotTRANSACTION_NODE, ledger_->info().seq);

    JLOG(journal_.debug()) << "Flushed " << numFlushed
                           << " nodes to nodestore from stateMap";
    JLOG(journal_.debug()) << "Flushed " << numTxFlushed
                           << " nodes to nodestore from txMap";

    app_.getNodeStore().sync();
    JLOG(journal_.debug()) << "synced nodestore";

    assert(numFlushed != 0 or roundMetrics.objectCount == 0);
    assert(numTxFlushed != 0 or roundMetrics.txnCount == 0);

    auto end = std::chrono::system_clock::now();

    roundMetrics.flushTime = ((end - start).count()) / 1000000000.0;

    // Make sure calculated hashes are correct
    assert(ledger_->stateMap().getHash().as_uint256() == accountHash);

    assert(ledger_->txMap().getHash().as_uint256() == txHash);

    assert(ledger_->info().hash == hash);

    JLOG(journal_.debug()) << "Flush time for ledger " << ledger_->info().seq
                           << " = " << roundMetrics.flushTime;
}

void
ReportingETL::initNumLedgers()
{
    assert(app_.pgPool());
    std::shared_ptr<PgQuery> pgQuery = std::make_shared<PgQuery>(app_.pgPool());
    std::string sql = "select count(*) from ledgers;";

    auto res = pgQuery->querySync(sql.data());
    auto result = PQresultStatus(res.get());
    JLOG(journal_.debug()) << "initNumLedgers result : " << result;

    assert(result == PGRES_TUPLES_OK || result == PGRES_SINGLE_TUPLE);
    assert(PQntuples(res.get()) == 1);
    char const* count = PQgetvalue(res.get(), 0, 0);
    numLedgers_ = std::stoll(count);
    JLOG(journal_.debug()) << "initNumLedgers - count = " << count;
}

bool
ReportingETL::consistencyCheck()
{
    assert(checkConsistency_);
    bool isConsistent = true;
    assert(app_.pgPool());
    std::shared_ptr<PgQuery> pgQuery = std::make_shared<PgQuery>(app_.pgPool());

    // check that every ledger hash is present in nodestore
    std::string sql =
        "select ledger_seq, ledger_hash from ledgers left join objects on "
        "ledgers.ledger_hash = objects.key where objects.key is null;";

    auto res = pgQuery->querySync(sql.data());
    auto result = PQresultStatus(res.get());
    JLOG(journal_.debug()) << "consistency check - ledger hash result : "
                           << result;

    assert(result == PGRES_TUPLES_OK);

    if (PQntuples(res.get()) > 0)
    {
        isConsistent = false;
        for (size_t i = 0; i < PQntuples(res.get()); ++i)
        {
            char const* ledgerSeq = PQgetvalue(res.get(), i, 0);
            char const* ledgerHash = PQgetvalue(res.get(), i, 1);
            JLOG(journal_.error())
                << "consistencyCheck - "
                << "ledger hash not present in nodestore. sequence = "
                << ledgerSeq << " ledger hash = " << ledgerHash;
        }
    }

    // check that every state map root is present in nodestore
    sql =
        "select ledger_seq, account_set_hash from ledgers left join objects on "
        "ledgers.account_set_hash = objects.key where objects.key is null;";

    res = pgQuery->querySync(sql.data());
    result = PQresultStatus(res.get());
    JLOG(journal_.debug()) << "consistency check - state map result : "
                           << result;

    assert(result == PGRES_TUPLES_OK);

    if (PQntuples(res.get()) > 0)
    {
        isConsistent = false;
        for (size_t i = 0; i < PQntuples(res.get()); ++i)
        {
            char const* ledgerSeq = PQgetvalue(res.get(), i, 0);
            char const* stateRoot = PQgetvalue(res.get(), i, 1);
            JLOG(journal_.error())
                << "consistencyCheck - "
                << "state map root not present in nodestore. sequence = "
                << ledgerSeq << " state map root = " << stateRoot;
        }
    }

    // check that every tx map root is present in nodestore
    sql =
        "select ledger_seq, trans_set_hash from ledgers left join objects on "
        "ledgers.trans_set_hash = objects.key where objects.key is null;";

    res = pgQuery->querySync(sql.data());
    result = PQresultStatus(res.get());
    JLOG(journal_.debug()) << "consistency check - tx map result : " << result;

    assert(result == PGRES_TUPLES_OK);

    if (PQntuples(res.get()) > 0)
    {
        isConsistent = false;
        for (size_t i = 0; i < PQntuples(res.get()); ++i)
        {
            char const* ledgerSeq = PQgetvalue(res.get(), i, 0);
            char const* txRoot = PQgetvalue(res.get(), i, 1);
            JLOG(journal_.error())
                << "consistencyCheck - "
                << "tx map root not present in nodestore. sequence = "
                << ledgerSeq << " tx map root = " << txRoot;
        }
    }

    if (checkRange_)
    {
        sql = "select count(*) from ledgers;";

        res = pgQuery->querySync(sql.data());
        result = PQresultStatus(res.get());
        JLOG(journal_.debug())
            << "consistency check - range result : " << result;

        assert(result == PGRES_TUPLES_OK);
        assert(PQntuples(res.get()) == 1);
        char const* count = PQgetvalue(res.get(), 0, 0);
        if (std::stoll(count) != numLedgers_)
        {
            JLOG(journal_.error())
                << "consistencyCheck - ledger range mismatch : "
                << "numLedgers_ = " << numLedgers_ << "count = " << count;
            isConsistent = false;
        }
    }

    JLOG(journal_.info()) << "consistencyCheck - isConsistent = "
                          << isConsistent;

    return isConsistent;
}

void
ReportingETL::storeLedger()
{
    JLOG(journal_.debug()) << "Storing ledger = " << ledger_->info().seq;
    auto start = std::chrono::system_clock::now();

    {
        Serializer s(128);
        s.add32(HashPrefix::ledgerMaster);
        addRaw(ledger_->info(), s);
        app_.getNodeStore().store(
            hotLEDGER,
            std::move(s.modData()),
            ledger_->info().hash,
            ledger_->info().seq);
    }

    auto end = std::chrono::system_clock::now();

    roundMetrics.storeTime = ((end - start).count()) / 1000000000.0;

    numLedgers_++;

    JLOG(journal_.debug()) << "Store time for ledger " << ledger_->info().seq
                           << " = " << roundMetrics.storeTime;
}

void
ReportingETL::publishLedger()
{
    app_.getOPs().pubLedger(ledger_);
}

void
ReportingETL::publishLedger(uint32_t ledgerSequence)
{
    size_t numAttempts = 0;
    while (!stopping_)
    {
        auto ledger = app_.getLedgerMaster().getLedgerBySeq(ledgerSequence);

        if (!ledger)
        {
            JLOG(journal_.error())
                << "Trying to publish. Could not find ledger = "
                << ledgerSequence;
            // try once per second for 20 seconds
            // then back off to try once per 4 seconds
            auto seconds = numAttempts < 20 ? 1 : 4;
            std::this_thread::sleep_for(std::chrono::seconds(seconds));
            ++numAttempts;
            continue;
        }
        app_.getOPs().pubLedger(ledger);
        JLOG(journal_.info()) << "Published ledger - " << ledgerSequence;
        return;
    }
}

bool
ReportingETL::fetchLedger(
    org::xrpl::rpc::v1::GetLedgerResponse& out,
    bool getObjects)
{
    auto idx = indexQueue_.pop();
    // 0 represents the queue is shutting down
    if (idx == 0)
    {
        JLOG(journal_.debug()) << "Popped 0 from index queue. Stopping";
        return false;
    }

    if (ledger_)
        assert(idx == ledger_->info().seq + 1);

    JLOG(journal_.info()) << "Attempting to fetch ledger = " << idx;

    auto res = loadBalancer_.fetchLedger(out, idx, getObjects);

    JLOG(journal_.trace()) << "GetLedger reply : " << out.DebugString();
    return res;
}

void
ReportingETL::updateLedger(
    org::xrpl::rpc::v1::GetLedgerResponse& in,
    std::vector<TxMeta>& out)
{
    auto start = std::chrono::system_clock::now();

    LedgerInfo lgrInfo = deserializeHeader(
        makeSlice(in.ledger_header()), true);

    JLOG(journal_.trace()) << "Beginning update."
                           << " seq = " << lgrInfo.seq
                           << " hash = " << lgrInfo.hash
                           << " account hash = " << lgrInfo.accountHash
                           << " tx hash = " << lgrInfo.txHash;

    if (!ledger_)
    {
        ledger_ = std::make_shared<Ledger>(
            lgrInfo, app_.config(), app_.getNodeFamily());
    }
    else
    {
        ledger_ = std::make_shared<Ledger>(*ledger_, NetClock::time_point{});
        ledger_->setLedgerInfo(lgrInfo);
    }

    ledger_->stateMap().clearSynching();
    ledger_->txMap().clearSynching();

    for (auto& txn : in.transactions_list().transactions())
    {
        auto& raw = txn.transaction_blob();

        // TODO can this be done faster? Move?
        SerialIter it{raw.data(), raw.size()};
        STTx sttx{it};

        auto txSerializer = std::make_shared<Serializer>(sttx.getSerializer());

        TxMeta txMeta{
            sttx.getTransactionID(), ledger_->info().seq, txn.metadata_blob()};

        auto metaSerializer =
            std::make_shared<Serializer>(txMeta.getAsObject().getSerializer());

        JLOG(journal_.trace())
            << "Inserting transaction = " << sttx.getTransactionID();
        ledger_->rawTxInsert(
            sttx.getTransactionID(), txSerializer, metaSerializer);

        // TODO use emplace to avoid this copy
        out.push_back(txMeta);
    }

    JLOG(journal_.trace()) << "Inserted all transactions. "
                           << " ledger = " << lgrInfo.seq;

    for (auto& state : in.ledger_objects())
    {
        auto& index = state.index();
        auto& data = state.data();

        auto key = uint256::fromVoid(index.data());
        // indicates object was deleted
        if (data.size() == 0)
        {
            JLOG(journal_.trace()) << "Erasing object = " << key;
            if (ledger_->exists(key))
                ledger_->rawErase(key);
        }
        else
        {
            // TODO maybe better way to construct the SLE?
            // Is there any type of move ctor? Maybe use Serializer?
            // Or maybe just use the move cto?
            SerialIter it{data.data(), data.size()};
            std::shared_ptr<SLE> sle = std::make_shared<SLE>(it, key);

            // TODO maybe remove this conditional
            if (ledger_->exists(key))
            {
                JLOG(journal_.trace()) << "Replacing object = " << key;
                ledger_->rawReplace(sle);
            }
            else
            {
                JLOG(journal_.trace()) << "Inserting object = " << key;
                ledger_->rawInsert(sle);
            }
        }
    }
    JLOG(journal_.trace()) << "Inserted/modified/deleted all objects."
                           << " ledger = " << lgrInfo.seq;

    if (in.ledger_objects().size())
        ledger_->updateSkipList();

    // update metrics
    auto end = std::chrono::system_clock::now();

    roundMetrics.updateTime = ((end - start).count()) / 1000000000.0;
    roundMetrics.txnCount = in.transactions_list().transactions().size();
    roundMetrics.objectCount = in.ledger_objects().size();

    JLOG(journal_.debug()) << "Update time for ledger " << lgrInfo.seq << " = "
                           << roundMetrics.updateTime;
}

void
writeToLedgersDB(
    LedgerInfo const& info,
    std::shared_ptr<PgQuery>& pgQuery,
    std::shared_ptr<Pg>& conn,
    beast::Journal& journal)
{
    auto cmd = boost::format(
        R"(INSERT INTO ledgers 
                VALUES(%u,'\x%s', '\x%s',%u,%u,%u,%u,%u,'\x%s','\x%s')
                )");

    auto ledgerInsert = boost::str(
        cmd % info.seq % strHex(info.hash) % strHex(info.parentHash) %
        info.drops.drops() % info.closeTime.time_since_epoch().count() %
        info.parentCloseTime.time_since_epoch().count() %
        info.closeTimeResolution.count() % info.closeFlags %
        strHex(info.accountHash) % strHex(info.txHash));
    JLOG(journal.trace()) << "writeToTxDB - ledgerInsert = " << ledgerInsert;
    auto res = pgQuery->querySync(ledgerInsert.data());

    assert(res);
    auto result = PQresultStatus(res.get());
    assert(result == PGRES_COMMAND_OK);
}

void
ReportingETL::truncateDBs()
{
    assert(app_.pgPool());
    assert(!app_.config().reportingReadOnly());
    std::shared_ptr<PgQuery> pgQuery = std::make_shared<PgQuery>(app_.pgPool());

    auto res = pgQuery->querySync("truncate ledgers cascade;");
    auto result = PQresultStatus(res.get());
    JLOG(journal_.trace()) << "truncateDBs - result : " << result;
    assert(result == PGRES_COMMAND_OK);

    res = pgQuery->querySync("truncate account_transactions;");
    result = PQresultStatus(res.get());
    JLOG(journal_.trace()) << "truncateDBs - result : " << result;
    assert(result == PGRES_COMMAND_OK);

    res = pgQuery->querySync("truncate min_seq;");
    result = PQresultStatus(res.get());
    JLOG(journal_.trace()) << "truncateDBs - result : " << result;
    assert(result == PGRES_COMMAND_OK);

    res = pgQuery->querySync("truncate ancestry_verified;");
    result = PQresultStatus(res.get());
    JLOG(journal_.trace()) << "truncateDBs - result : " << result;
    assert(result == PGRES_COMMAND_OK);

    numLedgers_ = 0;
}

void
writeToAccountTransactionsDB(
    std::vector<TxMeta>& metas,
    std::shared_ptr<PgQuery>& pgQuery,
    std::shared_ptr<Pg>& conn,
    beast::Journal& journal)
{
    // Initiate COPY operation
    auto res = pgQuery->querySync("COPY account_transactions from STDIN", conn);
    assert(res);
    auto result = PQresultStatus(res.get());
    assert(result == PGRES_COPY_IN);

    JLOG(journal.trace()) << "writeToTxDB - COPY result = " << result;

    // Write data to stream
    std::stringstream copyBuffer;
    for (auto& m : metas)
    {
        std::string txHash = strHex(m.getTxID());
        auto idx = m.getIndex();
        auto ledgerSeq = m.getLgrSeq();

        for (auto& a : m.getAffectedAccounts(journal))
        {
            std::string acct = strHex(a);
            copyBuffer << "\\\\x" << acct << '\t' << std::to_string(ledgerSeq)
                       << '\t' << std::to_string(idx) << '\t' << "\\\\x"
                       << txHash << '\n';
            JLOG(journal.trace()) << acct;
            /*
            JLOG(journal.trace())
                << "writing to account_transactions - "
                << " account = " << acct << " ledgerSeq = " << ledgerSeq
                << " idx = " << idx;
                */
        }
    }

    PQsetnonblocking(conn->getConn(), 0);

    std::string bufString = copyBuffer.str();
    JLOG(journal.trace()) << "copy buffer = " << bufString;

    // write the data to Postgres
    auto resCode =
        PQputCopyData(conn->getConn(), bufString.c_str(), bufString.size());

    auto pqResult = PQgetResult(conn->getConn());
    auto pqResultStatus = PQresultStatus(pqResult);
    JLOG(journal.trace()) << "putCopyData - resultCode = " << resCode
                          << " result = " << pqResultStatus;
    assert(resCode != -1);
    assert(resCode != 0);
    assert(pqResultStatus == 4);

    // Tell Postgres we are done with the COPY operation
    resCode = PQputCopyEnd(conn->getConn(), nullptr);
    while (true)
    {
        pqResult = PQgetResult(conn->getConn());
        if (!pqResult)
            break;
        pqResultStatus = PQresultStatus(pqResult);
    }

    JLOG(journal.trace()) << "putCopyEnd - resultCode = " << resCode
                          << " result = " << pqResultStatus
                          << " error_msg = " << PQerrorMessage(conn->getConn());
    assert(resCode != -1);
    assert(resCode != 0);
    assert(pqResultStatus != 7);
}

void
ReportingETL::writeToPostgres(
    LedgerInfo const& info,
    std::vector<TxMeta>& metas)
{
    // TODO: clean this up a bit. use less auto, better error handling, etc
    JLOG(journal_.debug()) << "writeToPostgres";
    assert(app_.pgPool());
    std::shared_ptr<PgQuery> pg = std::make_shared<PgQuery>(app_.pgPool());
    std::shared_ptr<Pg> conn;
    JLOG(journal_.trace()) << "createdPqQuery";

    auto res = pg->querySync("BEGIN", conn);
    assert(res);
    auto result = PQresultStatus(res.get());
    JLOG(journal_.trace()) << "writeToTxDB - BEGIN result = " << result;
    assert(result == PGRES_COMMAND_OK);

    writeToLedgersDB(info, pg, conn, journal_);

    writeToAccountTransactionsDB(metas, pg, conn, journal_);

    res = pg->querySync("COMMIT", conn);
    assert(res);
    result = PQresultStatus(res.get());

    JLOG(journal_.trace()) << "writeToTxDB - COMMIT result = " << result;
    assert(result == PGRES_COMMAND_OK);
    PQsetnonblocking(conn->getConn(), 1);
    app_.pgPool()->checkin(conn);
}

void
ReportingETL::doETL()
{
    org::xrpl::rpc::v1::GetLedgerResponse fetchResponse;

    if (not fetchLedger(fetchResponse))
        return;

    std::vector<TxMeta> metas;
    updateLedger(fetchResponse, metas);

    flushLedger();

    if (app_.config().usePostgresTx())
        writeToPostgres(ledger_->info(), metas);

    storeLedger();

    publishLedger();

    outputMetrics();

    if (checkConsistency_)
    {
        // need to sync here, so ledger header is written to nodestore
        // before consistency check
        app_.getNodeStore().sync();
        assert(consistencyCheck());
    }
}

void
ReportingETL::outputMetrics()
{
    roundMetrics.printMetrics(journal_);

    totalMetrics.addMetrics(roundMetrics);
    totalMetrics.printMetrics(journal_);

    // reset round metrics
    roundMetrics = {};
}

void
ReportingETL::doWork()
{
    if (app_.config().reportingReadOnly())
    {
        // In readOnly mode, the only work that needs to be done is to publish
        // ledgers to subscription streams when validated
        worker_ = std::thread([this]() {
            auto idx = 0;
            while ((idx = indexQueue_.pop()) != 0)
            {
                publishLedger(idx);
            }
        });
    }
    else
    {
        worker_ = std::thread([this]() {
            JLOG(journal_.info()) << "Downloading initial ledger";

            loadInitialLedger();

            JLOG(journal_.info()) << "Done downloading initial ledger";

            // reset after first iteration
            totalMetrics = {};
            roundMetrics = {};

            size_t numLoops = 0;

            while (not stopping_)
            {
                doETL();
                numLoops++;
                if (numLoops == 10)
                    totalMetrics = {};
            }
        });
    }
}

ReportingETL::ReportingETL(Application& app, Stoppable& parent)
    : Stoppable("ReportingETL", parent)
    , app_(app)
    , journal_(app.journal("ReportingETL"))
    , loadBalancer_(*this)
    , indexQueue_(journal_)
{
    // if present, get endpoint from config
    if (app_.config().exists("reporting"))
    {
        Section section = app_.config().section("reporting");

        JLOG(journal_.debug()) << "Parsing config info";

        std::pair<std::string, bool> ro = section.find("read_only");
        if (ro.second)
            app_.config().setReportingReadOnly(ro.first == "true");

        auto& vals = section.values();
        for (auto& v : vals)
        {
            JLOG(journal_.debug()) << "val is " << v;
            Section source = app_.config().section(v);

            std::pair<std::string, bool> ipPair = source.find("source_ip");
            if (!ipPair.second)
                continue;

            std::pair<std::string, bool> wsPortPair =
                source.find("source_ws_port");
            if (!wsPortPair.second)
                continue;

            std::pair<std::string, bool> grpcPortPair =
                source.find("source_grpc_port");
            if (!grpcPortPair.second)
            {
                // add source without grpc port
                // used in read-only mode to detect when new ledgers have been
                // validated. Used for publishing
                if (app_.config().reportingReadOnly())
                    loadBalancer_.add(ipPair.first, wsPortPair.first);
                continue;
            }

            loadBalancer_.add(
                ipPair.first, wsPortPair.first, grpcPortPair.first);
        }

        std::pair<std::string, bool> pgTx = section.find("postgres_tx");
        if (pgTx.second)
            app_.config().setUsePostgresTx(pgTx.first == "true");

        // don't need to do any more work if we are in read only mode
        if (app_.config().reportingReadOnly())
            return;

        std::pair<std::string, bool> flushInterval =
            section.find("flush_interval");
        if (flushInterval.second)
        {
            flushInterval_ = std::stoi(flushInterval.first);
        }

        std::pair<std::string, bool> numMarkers = section.find("num_markers");
        if (numMarkers.second)
            numMarkers_ = std::stoi(numMarkers.first);

        std::pair<std::string, bool> checkConsistency =
            section.find("check_consistency");
        if (checkConsistency.second)
        {
            checkConsistency_ = (checkConsistency.first == "true");
        }

        if (checkConsistency_)
        {
            initNumLedgers();

            Section nodeDb = app_.config().section("node_db");
            std::pair<std::string, bool> onlineDelete =
                nodeDb.find("online_delete");
            if (onlineDelete.second)
                checkRange_ = false;
            else
                checkRange_ = true;

            std::pair<std::string, bool> postgresNodestore =
                nodeDb.find("type");
            // if the node_db is not using Postgres, we don't check for
            // consistency
            if (!postgresNodestore.second ||
                !boost::beast::iequals(postgresNodestore.first, "Postgres"))
                checkConsistency_ = false;
            // if we are not using postgres in place of SQLite, we don't
            // check for consistency
            if (!app_.config().usePostgresTx())
                checkConsistency_ = false;
        }
    }
}

}  // namespace ripple
