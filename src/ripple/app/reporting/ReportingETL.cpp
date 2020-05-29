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

#include <ripple/app/reporting/DBHelpers.h>
#include <ripple/app/reporting/ReportingETL.h>

#include <ripple/json/json_reader.h>
#include <ripple/json/json_writer.h>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <cstdlib>
#include <iostream>
#include <string>
#include <variant>

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
                    hotACCOUNT_NODE, ledger_->info().seq, true);
            }
            ++num;
        }
        /*
        if (not stopping_)
            ledger_->stateMap().flushDirty(
                hotACCOUNT_NODE, ledger_->info().seq, true);
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
    updateLedger(response, metas, false);

    auto start = std::chrono::system_clock::now();

    startWriter();

    loadBalancer_.loadInitialLedger(ledger_, writeQueue_);
    joinWriter();
    // TODO handle case when there is a network error (other side dies)
    // Shouldn't try to flush in that scenario
    if (not stopping_)
    {
        flushLedger();
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
    JLOG(journal_.debug()) << __func__ << " : "
                           << "Flushing ledger. " << toString(ledger_->info());
    // These are recomputed in setImmutable
    auto& accountHash = ledger_->info().accountHash;
    auto& txHash = ledger_->info().txHash;
    auto& ledgerHash = ledger_->info().hash;

    auto start = std::chrono::system_clock::now();

    ledger_->setImmutable(app_.config(), false);

    auto numFlushed =
        ledger_->stateMap().flushDirty(hotACCOUNT_NODE, ledger_->info().seq,
                                       true);

    auto numTxFlushed =
        ledger_->txMap().flushDirty(hotTRANSACTION_NODE, ledger_->info().seq,
                                    true);

    {
        Serializer s(128);
        s.add32(HashPrefix::ledgerMaster);
        addRaw(ledger_->info(), s);
        app_.getNodeStore().store(
            hotLEDGER,
            std::move(s.modData()),
            ledger_->info().hash,
            ledger_->info().seq,
            true);
    }

    app_.getNodeStore().sync();

    auto end = std::chrono::system_clock::now();

    JLOG(journal_.debug()) << __func__ << " : "
                           << "Flushed " << numFlushed
                           << " nodes to nodestore from stateMap";
    JLOG(journal_.debug()) << __func__ << " : "
                           << "Flushed " << numTxFlushed
                           << " nodes to nodestore from txMap";

    if (numFlushed == 0 && roundMetrics.objectCount != 0)
    {
        JLOG(journal_.fatal()) << __func__ << " : "
                               << "Failed to flush state map";
        assert(false);
    }
    if (numTxFlushed == 0 && roundMetrics.txnCount == 0)
    {
        JLOG(journal_.fatal()) << __func__ << " : "
                               << "Failed to flush tx map";
        assert(false);
    }

    roundMetrics.flushTime = ((end - start).count()) / 1000000000.0;

    // Make sure calculated hashes are correct
    if (ledger_->stateMap().getHash().as_uint256() != accountHash)
    {
        JLOG(journal_.fatal())
            << __func__ << " : "
            << "State map hash does not match. "
            << "Expected hash = " << strHex(accountHash) << "Actual hash = "
            << strHex(ledger_->stateMap().getHash().as_uint256());
        assert(false);
    }

    if (ledger_->txMap().getHash().as_uint256() != txHash)
    {
        JLOG(journal_.fatal())
            << __func__ << " : "
            << "Tx map hash does not match. "
            << "Expected hash = " << strHex(txHash) << "Actual hash = "
            << strHex(ledger_->txMap().getHash().as_uint256());
        assert(false);
    }

    if (ledger_->info().hash != ledgerHash)
    {
        JLOG(journal_.fatal())
            << __func__ << " : "
            << "Ledger hash does not match. "
            << "Expected hash = " << strHex(ledgerHash)
            << "Actual hash = " << strHex(ledger_->info().hash);
        assert(false);
    }

    JLOG(journal_.info()) << __func__ << " : "
                          << "Successfully flushed ledger! "
                          << toString(ledger_->info());
}

void
ReportingETL::publishLedger()
{
    app_.getOPs().pubLedger(ledger_);

    lastPublish_ = std::chrono::system_clock::now();
}

bool
ReportingETL::publishLedger(uint32_t ledgerSequence, uint32_t maxAttempts)
{
    JLOG(journal_.info()) << __func__ << " : "
                          << "Attempting to publish ledger = "
                          << ledgerSequence;
    size_t numAttempts = 0;
    while (!stopping_)
    {
        auto ledger = app_.getLedgerMaster().getLedgerBySeq(ledgerSequence);

        if (!ledger)
        {
            JLOG(journal_.warn())
                << __func__ << " : "
                << "Trying to publish. Could not find ledger with sequence = "
                << ledgerSequence;
            // We try maxAttempts times to publish the ledger, waiting one
            // second in between each attempt.
            // If the ledger is not present in the database after maxAttempts,
            // we attempt to take over as the writer. If the takeover fails,
            // doContinuousETL will return, and this node will go back to
            // publishing.
            // If the node is in strict read only mode, we simply
            // skip publishing this ledger and return false indicating the
            // publish failed
            if (numAttempts >= maxAttempts)
            {
                JLOG(journal_.error()) << __func__ << " : "
                                       << "Failed to publish ledger after "
                                       << numAttempts << " attempts.";
                if (!readOnly_)
                {
                    JLOG(journal_.info()) << __func__ << " : "
                                          << "Attempting to become ETL writer";

                    ledger_ = std::const_pointer_cast<Ledger>(
                        app_.getLedgerMaster().getLedgerBySeq(
                            ledgerSequence - 1));
                    doContinousETL();

                    if (!stopping_)
                    {
                        JLOG(journal_.info()) << __func__ << " : "
                                              << "Failed to become writer. "
                                              << "Falling back to publishing";
                    }
                }
                else
                {
                    JLOG(journal_.debug())
                        << __func__ << " : "
                        << "In strict read-only mode. "
                        << "Skipping publishing this ledger. "
                        << "Beginning fast forward.";
                    return false;
                }
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                ++numAttempts;
            }
            continue;
        }
        app_.getOPs().pubLedger(ledger);
        lastPublish_ = std::chrono::system_clock::now();
        JLOG(journal_.info())
            << __func__ << " : "
            << "Published ledger. " << toString(ledger->info());
        return true;
    }
    return false;
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

    if (ledger_ && idx != ledger_->info().seq + 1)
    {
        JLOG(journal_.fatal())
            << __func__ << " : "
            << "Sequence popped from index queue is not 1 greater than "
            << "previous sequence. Popped sequence = " << idx << ". "
            << "Expected sequence = " << (ledger_->info().seq + 1);
        assert(false);
    }

    JLOG(journal_.debug()) << __func__ << " : "
                           << "Attempting to fetch ledger with sequence = "
                           << idx;

    auto res = loadBalancer_.fetchLedger(out, idx, getObjects);

    JLOG(journal_.trace()) << __func__ << " : "
                           << "GetLedger reply = " << out.DebugString();
    return res;
}

void
ReportingETL::updateLedger(
    org::xrpl::rpc::v1::GetLedgerResponse& in,
    std::vector<TxMeta>& out,
    bool updateSkiplist)
{
    JLOG(journal_.info()) << __func__ << " : "
                          << "Beginning ledger update";
    auto start = std::chrono::system_clock::now();

    LedgerInfo lgrInfo = deserializeHeader(
        makeSlice(in.ledger_header()), true);

    JLOG(journal_.debug()) << __func__ << " : "
                           << "Deserialized ledger header. "
                           << toString(lgrInfo);

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
            << __func__ << " : "
            << "Inserting transaction = " << sttx.getTransactionID();
        ledger_->rawTxInsert(
            sttx.getTransactionID(), txSerializer, metaSerializer);

        // TODO use emplace to avoid this copy
        out.push_back(txMeta);
    }

    JLOG(journal_.debug())
        << __func__ << " : "
        << "Inserted all transactions. Number of transactions  = "
        << in.transactions_list().transactions_size();

    for (auto& state : in.ledger_objects())
    {
        auto& index = state.index();
        auto& data = state.data();

        auto key = uint256::fromVoid(index.data());
        // indicates object was deleted
        if (data.size() == 0)
        {
            JLOG(journal_.trace()) << __func__ << " : "
                                   << "Erasing object = " << key;
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
                JLOG(journal_.trace()) << __func__ << " : "
                                       << "Replacing object = " << key;
                ledger_->rawReplace(sle);
            }
            else
            {
                JLOG(journal_.trace()) << __func__ << " : "
                                       << "Inserting object = " << key;
                ledger_->rawInsert(sle);
            }
        }
    }
    JLOG(journal_.debug())
        << __func__ << " : "
        << "Inserted/modified/deleted all objects. Number of objects = "
        << in.ledger_objects_size();

    if (updateSkiplist)
        ledger_->updateSkipList();

    // update metrics
    auto end = std::chrono::system_clock::now();

    roundMetrics.updateTime = ((end - start).count()) / 1000000000.0;
    roundMetrics.txnCount = in.transactions_list().transactions().size();
    roundMetrics.objectCount = in.ledger_objects().size();

    JLOG(journal_.debug()) << __func__ << " : "
                           << "Finished ledger update";
}

bool
ReportingETL::writeToPostgres(
    LedgerInfo const& info,
    std::vector<TxMeta>& metas)
{
    // TODO: clean this up a bit. use less auto, better error handling, etc
    JLOG(journal_.debug()) << __func__ << " : "
                           << "Beginning write to Postgres";
    if (!app_.pgPool())
    {
        JLOG(journal_.fatal()) << __func__ << " : "
                               << "app_.pgPool is null";
        assert(false);
    }
    std::shared_ptr<PgQuery> pg = std::make_shared<PgQuery>(app_.pgPool());
    std::shared_ptr<Pg> conn;

    auto start = std::chrono::system_clock::now();

    executeUntilSuccess(pg, conn, "BEGIN", PGRES_COMMAND_OK, *this);

    // Writing to the ledgers db fails if the ledger already exists in the db.
    // In this situation, the ETL process has detected there is another writer,
    // and falls back to only publishing
    if (!writeToLedgersDB(info, pg, conn, *this))
    {
        JLOG(journal_.warn()) << __func__ << " : "
                              << "Failed to write to ledgers database.";
        return false;
    }

    writeToAccountTransactionsDB(metas, pg, conn, *this);

    executeUntilSuccess(pg, conn, "COMMIT", PGRES_COMMAND_OK, *this);

    PQsetnonblocking(conn->getConn(), 1);
    app_.pgPool()->checkin(conn);

    auto end = std::chrono::system_clock::now();

    roundMetrics.postgresTime = ((end - start).count()) / 1000000000.0;
    JLOG(journal_.info()) << __func__ << " : "
                          << "Successfully wrote to Postgres";
    return true;
}

bool
ReportingETL::doETL()
{
    org::xrpl::rpc::v1::GetLedgerResponse fetchResponse;

    if (not fetchLedger(fetchResponse))
        return false;

    std::vector<TxMeta> metas;
    updateLedger(fetchResponse, metas);

    flushLedger();

    if (app_.config().usePostgresTx())
        if (!writeToPostgres(ledger_->info(), metas))
            return false;

    publishLedger();

    outputMetrics();

    if (checkConsistency_)
    {
        assert(checkConsistency(*this));
    }
    return true;
}

void
ReportingETL::outputMetrics()
{
    roundMetrics.printMetrics(journal_, ledger_->info());

    totalMetrics.addMetrics(roundMetrics);
    totalMetrics.printMetrics(journal_);

    // reset round metrics
    roundMetrics = {};
}

void
ReportingETL::doContinousETL()
{
    writing_ = true;
    assert(!readOnly_);
    JLOG(journal_.info()) << "Downloading initial ledger";

    loadInitialLedger();

    JLOG(journal_.info()) << "Done downloading initial ledger";

    // reset after first iteration
    totalMetrics = {};
    roundMetrics = {};

    size_t numLoops = 0;

    while (not stopping_)
    {
        if (!doETL())
        {
            writing_ = false;
            break;
        }
        numLoops++;
        // At the rate of 1 ledger per 4 seconds, 21600 is one day of ledgers
        size_t constexpr ledgersPerDay = 21600;
        // Reset metrics after 100 iterations (let cache warm up)
        // Reset metrics once per day
        if (numLoops == 100 || (numLoops % ledgersPerDay) == 0)
        {
            JLOG(journal_.info()) << __func__ << " : "
                                  << "Resetting metrics";
            totalMetrics = {};
        }
    }
}

void
ReportingETL::monitor()
{
    auto idx = 0;
    bool success = true;
    // when the indexQueue returns 0, ETL is shutting down
    while ((idx = indexQueue_.front()) != 0)
    {
        JLOG(journal_.info()) << __func__ << " : "
                              << "Ledger with sequence = " << idx
                              << " has been validated by the network. "
                              << "Attempting to find in database and publish";
        // Attempt to take over responsibility of ETL writer after 10 failed
        // attempts to publish the ledger. publishLedger() fails if the ledger
        // that has been validated by the network is not found in the database
        // after the specified number of attempts. publishLedger() waits one
        // second between each attempt to read the ledger from the database
        //
        // In strict read-only mode, when the software fails to find a ledger
        // in the database that has been validated by the network, the software
        // will only try to publish subsequent ledgers once, until one of those
        // ledgers is found in the database. Once the software successfully
        // publishes a ledger, the software will fall back to the normal
        // behavior of trying several times to publish the ledger that has been
        // validated by the network. In this manner, a reporting processing
        // running in read-only mode does not need to restart if the database
        // is wiped.
        success = publishLedger(idx, success ? (readOnly_ ? 30 : 10) : 1);
        indexQueue_.pop();
    }
}

void
ReportingETL::doWork()
{
    worker_ = std::thread([this]() { monitor(); });
}

void
ReportingETL::setup()
{
    if (app_.config().START_UP == Config::StartUpType::FRESH && !readOnly_)
    {
        if (app_.config().usePostgresTx())
        {
            // if we don't load the ledger from disk, the dbs need to be
            // cleared out, since the db will not allow any gaps
            truncateDBs(*this);
        }
        assert(app_.config().exists("reporting"));
        Section section = app_.config().section("reporting");
        std::pair<std::string, bool> startIndexPair =
            section.find("start_index");

        if (startIndexPair.second)
        {
            indexQueue_.push(std::stoi(startIndexPair.first));
        }
    }
    else if (!readOnly_)
    {
        if (checkConsistency_)
            assert(checkConsistency(*this));
        // This ledger will not actually be mutated, but every ledger
        // after it will therefore ledger_ is not const
        ledger_ = std::const_pointer_cast<Ledger>(
            app_.getLedgerMaster().getValidatedLedger());
        if (ledger_)
        {
            JLOG(journal_.info()) << "Loaded ledger successfully. "
                                  << "seq = " << ledger_->info().seq;
            indexQueue_.push(ledger_->info().seq + 1);
        }
        else
        {
            JLOG(journal_.warn()) << "Failed to load ledger. Will download";
        }
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
        {
            readOnly_ = ro.first == "true";
            app_.config().setReportingReadOnly(readOnly_);
        }

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
            Section nodeDb = app_.config().section("node_db");

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
