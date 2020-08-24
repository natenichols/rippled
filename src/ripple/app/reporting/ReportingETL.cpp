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

#include <ripple/beast/core/CurrentThreadName.h>
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
ReportingETL::consumeLedgerData(
    std::shared_ptr<Ledger>& ledger,
    ThreadSafeQueue<std::shared_ptr<SLE>>& writeQueue)
{
    std::shared_ptr<SLE> sle;
    size_t num = 0;
    // TODO: if this call blocks, flushDirty in the meantime
    while (not stopping_ and (sle = writeQueue.pop()))
    {
        assert(sle);
        // TODO get rid of this conditional
        if (!ledger->exists(sle->key()))
            ledger->rawInsert(sle);

        if (flushInterval_ != 0 and (num % flushInterval_) == 0)
        {
            JLOG(journal_.debug()) << "Flushing! key = " << strHex(sle->key());
            ledger->stateMap().flushDirty(
                hotACCOUNT_NODE, ledger->info().seq, true);
        }
        ++num;
    }
}

std::vector<AccountTransactionsData>
ReportingETL::insertTransactions(
    std::shared_ptr<Ledger>& ledger,
    org::xrpl::rpc::v1::GetLedgerResponse& data)
{
    std::vector<AccountTransactionsData> accountTxData;
    for (auto& txn : data.transactions_list().transactions())
    {
        auto& raw = txn.transaction_blob();

        // TODO can this be done faster? Move?
        SerialIter it{raw.data(), raw.size()};
        STTx sttx{it};

        auto txSerializer = std::make_shared<Serializer>(sttx.getSerializer());

        TxMeta txMeta{
            sttx.getTransactionID(), ledger->info().seq, txn.metadata_blob()};

        auto metaSerializer =
            std::make_shared<Serializer>(txMeta.getAsObject().getSerializer());

        JLOG(journal_.trace())
            << __func__ << " : "
            << "Inserting transaction = " << sttx.getTransactionID();
        uint256 nodestoreHash = ledger->rawTxInsert(
            sttx.getTransactionID(), txSerializer, metaSerializer);
        accountTxData.emplace_back(txMeta, nodestoreHash, journal_);
    }
    return accountTxData;
}

// Downloads ledger in full from network. Returns empty shared_ptr on error
// @param sequence of ledger to download
// @return the full ledger. All data has been written to the database (key-value
// and relational). Empty shared_ptr on error
std::shared_ptr<Ledger>
ReportingETL::loadInitialLedger(uint32_t startingSequence)
{
    // check that database is actually empty
    auto ledger = std::const_pointer_cast<Ledger>(
        app_.getLedgerMaster().getValidatedLedger());
    if(ledger)
    {
        JLOG(journal_.fatal()) << __func__ << " : "
            << "Database is not empty";
        assert(false);
        return {};
    }

    // fetch the ledger from the network. This function will not return until
    // either the fetch is successful, or the server is being shutdown. This only
    // fetches the ledger header and the transactions+metadata
    std::optional<org::xrpl::rpc::v1::GetLedgerResponse> ledgerData{
        fetchLedgerData(startingSequence)};
    if (!ledgerData)
        return {};

    LedgerInfo lgrInfo = deserializeHeader(
        makeSlice(ledgerData->ledger_header()), true);

    JLOG(journal_.debug()) << __func__ << " : "
                           << "Deserialized ledger header. "
                           << toString(lgrInfo);

    ledger = std::make_shared<Ledger>(lgrInfo, app_.config(), app_.getNodeFamily());
    ledger->stateMap().clearSynching();
    ledger->txMap().clearSynching();
    std::vector<AccountTransactionsData> accountTxData =
        insertTransactions(ledger, *ledgerData);

    auto start = std::chrono::system_clock::now();

    ThreadSafeQueue<std::shared_ptr<SLE>> writeQueue;
    std::thread asyncWriter{[this, &ledger, &writeQueue]() {
        consumeLedgerData(ledger, writeQueue);
    }};

    // download the full account state map. This function downloads full ledger
    // data and pushes the downloaded data into the writeQueue. asyncWriter
    // consumes from the queue and inserts the data into the Ledger object.
    // Once the below call returns, all data has been pushed into the queue
    loadBalancer_.loadInitialLedger(startingSequence, writeQueue);
    // null is used to respresent the end of the queue
    std::shared_ptr<SLE> null;
    writeQueue.push(null);
    // wait for the writer to finish
    asyncWriter.join();
    // TODO handle case when there is a network error (other side dies)
    // Shouldn't try to flush in that scenario
    // Retry? Or just die?
    if (not stopping_)
    {
        //TODO handle write conflict
        flushLedger(ledger);
        if (app_.config().usePostgresLedgerTx())
        {
            writeToPostgres(
                ledger->info(),
                accountTxData,
                app_.pgPool(),
                app_.config().useTxTables(),
                journal_);
        }
    }
    auto end = std::chrono::system_clock::now();
    JLOG(journal_.debug()) << "Time to download and store ledger = "
                           << ((end - start).count()) / 1000000000.0;
    return ledger;
}

void
ReportingETL::flushLedger(std::shared_ptr<Ledger>& ledger)
{
    JLOG(journal_.debug()) << __func__ << " : "
                           << "Flushing ledger. " << toString(ledger->info());
    // These are recomputed in setImmutable
    auto& accountHash = ledger->info().accountHash;
    auto& txHash = ledger->info().txHash;
    auto& ledgerHash = ledger->info().hash;


    ledger->setImmutable(app_.config(), false);
    auto start = std::chrono::system_clock::now();

    auto numFlushed = ledger->stateMap().flushDirty(
        hotACCOUNT_NODE, ledger->info().seq, true);

    auto numTxFlushed = ledger->txMap().flushDirty(
        hotTRANSACTION_NODE, ledger->info().seq, true);

    {
        Serializer s(128);
        s.add32(HashPrefix::ledgerMaster);
        addRaw(ledger->info(), s);
        app_.getNodeStore().store(
            hotLEDGER,
            std::move(s.modData()),
            ledger->info().hash,
            ledger->info().seq,
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

    JLOG(journal_.debug()) << __func__ << " : "
                           << "Flush took "
                           << (end - start).count() / 1000000000.0
                           << " seconds";

    if (numFlushed == 0)
    {
        JLOG(journal_.fatal()) << __func__ << " : "
                               << "Flushed 0 nodes from state map";
        assert(false);
    }
    if (numTxFlushed == 0)
    {
        JLOG(journal_.warn()) << __func__ << " : "
                              << "Flushed 0 nodes from tx map";
    }


    // Make sure calculated hashes are correct
    if (ledger->stateMap().getHash().as_uint256() != accountHash)
    {
        JLOG(journal_.fatal())
            << __func__ << " : "
            << "State map hash does not match. "
            << "Expected hash = " << strHex(accountHash) << "Actual hash = "
            << strHex(ledger->stateMap().getHash().as_uint256());
        assert(false);
        throw std::runtime_error("state map hash mismatch");
    }

    if (ledger->txMap().getHash().as_uint256() != txHash)
    {
        JLOG(journal_.fatal())
            << __func__ << " : "
            << "Tx map hash does not match. "
            << "Expected hash = " << strHex(txHash) << "Actual hash = "
            << strHex(ledger->txMap().getHash().as_uint256());
        assert(false);
        throw std::runtime_error("tx map hash mismatch");
    }

    if (ledger->info().hash != ledgerHash)
    {
        JLOG(journal_.fatal())
            << __func__ << " : "
            << "Ledger hash does not match. "
            << "Expected hash = " << strHex(ledgerHash)
            << "Actual hash = " << strHex(ledger->info().hash);
        assert(false);
        throw std::runtime_error("ledger hash mismatch");
    }

    JLOG(journal_.info()) << __func__ << " : "
                          << "Successfully flushed ledger! "
                          << toString(ledger->info());
}

void
ReportingETL::publishLedger(std::shared_ptr<Ledger>& ledger)
{
    app_.getOPs().pubLedger(ledger);

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
                    return false;
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

        publishStrand_.post(
            [this, ledger]() { app_.getOPs().pubLedger(ledger); });
        lastPublish_ = std::chrono::system_clock::now();
        JLOG(journal_.info())
            << __func__ << " : "
            << "Published ledger. " << toString(ledger->info());
        return true;
    }
    return false;
}

std::optional<org::xrpl::rpc::v1::GetLedgerResponse>
ReportingETL::fetchLedgerData(uint32_t idx)
{

    JLOG(journal_.debug()) << __func__ << " : "
                           << "Attempting to fetch ledger with sequence = "
                           << idx;

    org::xrpl::rpc::v1::GetLedgerResponse response;
    auto res = loadBalancer_.fetchLedger(response, idx, false);
    JLOG(journal_.trace()) << __func__ << " : "
                           << "GetLedger reply = " << response.DebugString();
    if (!res)
        return {};
    return {std::move(response)};
}

std::optional<org::xrpl::rpc::v1::GetLedgerResponse>
ReportingETL::fetchLedgerDataAndDiff(uint32_t idx)
{
    JLOG(journal_.debug()) << __func__ << " : "
                           << "Attempting to fetch ledger with sequence = "
                           << idx;

    org::xrpl::rpc::v1::GetLedgerResponse response;
    auto res = loadBalancer_.fetchLedger(response, idx, true);
    JLOG(journal_.trace()) << __func__ << " : "
                           << "GetLedger reply = " << response.DebugString();
    if (!res)
        return {};
    return {std::move(response)};
}

std::pair<std::shared_ptr<Ledger>, std::vector<AccountTransactionsData>>
ReportingETL::buildNextLedger(
    std::shared_ptr<Ledger>& next,
    org::xrpl::rpc::v1::GetLedgerResponse& rawData)
{
    JLOG(journal_.info()) << __func__ << " : "
                          << "Beginning ledger update";

    LedgerInfo lgrInfo = deserializeHeader(
        makeSlice(rawData.ledger_header()), true);

    JLOG(journal_.debug()) << __func__ << " : "
                           << "Deserialized ledger header. "
                           << toString(lgrInfo);

    next->setLedgerInfo(lgrInfo);

    next->stateMap().clearSynching();
    next->txMap().clearSynching();

    std::vector<AccountTransactionsData> accountTxData{
        insertTransactions(next, rawData)};

    JLOG(journal_.debug())
        << __func__ << " : "
        << "Inserted all transactions. Number of transactions  = "
        << rawData.transactions_list().transactions_size();

    for (auto& state : rawData.ledger_objects())
    {
        auto& index = state.index();
        auto& data = state.data();

        auto key = uint256::fromVoid(index.data());
        // indicates object was deleted
        if (data.size() == 0)
        {
            JLOG(journal_.trace()) << __func__ << " : "
                                   << "Erasing object = " << key;
            if (next->exists(key))
                next->rawErase(key);
        }
        else
        {
            // TODO maybe better way to construct the SLE?
            // Is there any type of move ctor? Maybe use Serializer?
            // Or maybe just use the move cto?
            SerialIter it{data.data(), data.size()};
            std::shared_ptr<SLE> sle = std::make_shared<SLE>(it, key);

            // TODO maybe remove this conditional
            if (next->exists(key))
            {
                JLOG(journal_.trace()) << __func__ << " : "
                                       << "Replacing object = " << key;
                next->rawReplace(sle);
            }
            else
            {
                JLOG(journal_.trace()) << __func__ << " : "
                                       << "Inserting object = " << key;
                next->rawInsert(sle);
            }
        }
    }
    JLOG(journal_.debug())
        << __func__ << " : "
        << "Inserted/modified/deleted all objects. Number of objects = "
        << rawData.ledger_objects_size();

    if(!rawData.skiplist_included())
    {
        next->updateSkipList();
        JLOG(journal_.warn())
            << __func__ << " : "
            << "tx process is not sending skiplist. This indicates that the tx "
               "process is parsing metadata instead of doing a SHAMap diff. "
               "Make sure tx process is running the same code as reporting to "
               "use SHAMap diff instead of parsing metadata";
    }

    JLOG(journal_.debug()) << __func__ << " : "
                           << "Finished ledger update. "
                           << toString(next->info());
    return {std::move(next), std::move(accountTxData)};
}

// Database must be populated when this starts
std::optional<uint32_t>
ReportingETL::runETLPipeline(uint32_t startSequence)
{
    /*
     * Behold, mortals! This function spawns three separate threads, which talk
     * to each other via 2 different thread safe queues and 1 atomic variable.
     * All threads and queues are function local. This function returns when all
     * of the threads exit. There are two termination conditions: the first is
     * if the load thread encounters a write conflict. In this case, the load
     * thread sets writeConflict, an atomic bool, to true, which signals the
     * other threads to stop. The second termination condition is when the
     * entire server is shutting down, which is detected in one of two ways:
     * either the networkValidatedLedgers_.waitUntilValidatedByNetwork returns
     * false, signaling the wait was aborted. Or fetchLedgerDataAndDiff returns
     * an empty optional, signaling the fetch was aborted. In both cases, the
     * extract thread detects this condition, and pushes an empty optional onto
     * the transform queue. The transform thread, upon popping an empty
     * optional, pushes an empty optional onto the load queue, and then returns.
     * The load thread, upon popping an empty optional, returns. Note however,
     * each thread will first process any data pushed prior to the empty
     * optional before returning.
     */

    JLOG(journal_.debug()) << __func__ << " : "
                           << "Starting etl pipeline";
    writing_ = true;

    std::shared_ptr<Ledger> parent = std::const_pointer_cast<Ledger>(
            app_.getLedgerMaster().getLedgerBySeq(startSequence-1));
    assert(parent);

    std::atomic_bool writeConflict = false;
    std::optional<uint32_t> lastPublishedSequence;
    constexpr uint32_t maxQueueSize = 1000;

    ThreadSafeQueue<std::optional<org::xrpl::rpc::v1::GetLedgerResponse>>
        transformQueue{maxQueueSize};

    std::thread extracter{
        [this, &startSequence, &writeConflict, &transformQueue]() {
            beast::setCurrentThreadName("rippled: ReportingETL extract");
            uint32_t currentSequence = startSequence;

            // there are two stopping conditions here.
            // First, if there is a write conflict in the load thread, the ETL
            // mechanism should stop.
            // Second, if the entire server is shutting down,
            // waitUntilValidatedByNetwork() is going to return false.
            while (networkValidatedLedgers_.waitUntilValidatedByNetwork(
                       currentSequence) &&
                   !writeConflict && !isStopping())
            {
                auto start = std::chrono::system_clock::now();
                std::optional<org::xrpl::rpc::v1::GetLedgerResponse>
                    fetchResponse{fetchLedgerDataAndDiff(currentSequence)};
                auto end = std::chrono::system_clock::now();

                auto time = ((end - start).count()) / 1000000000.0;
                auto tps =
                    fetchResponse->transactions_list().transactions_size() /
                    time;

                JLOG(journal_.debug()) << "Extract phase time = " << time
                                       << " . Extract phase tps = " << tps;
                // if the fetch is unsuccessful, stop. fetchLedger only returns
                // false if the server is shutting down, or if the ledger was
                // found in the database. otherwise, fetchLedger will continue
                // trying to fetch the specified ledger until successful
                if (!fetchResponse)
                {
                    break;
                }

                transformQueue.push(std::move(fetchResponse));
                ++currentSequence;
            }
            // empty optional tells the transformer to shut down
            transformQueue.push({});
        }};

    ThreadSafeQueue<std::optional<std::pair<
        std::shared_ptr<Ledger>,
        std::vector<AccountTransactionsData>>>>
        loadQueue{maxQueueSize};
    std::thread transformer{[this,
                             &parent,
                             &writeConflict,
                             &loadQueue,
                             &transformQueue]() {
        beast::setCurrentThreadName("rippled: ReportingETL transform");

        assert(parent);
        parent = std::make_shared<Ledger>(*parent, NetClock::time_point{});
        while (!writeConflict)
        {
            std::optional<org::xrpl::rpc::v1::GetLedgerResponse> fetchResponse{
                transformQueue.pop()};
            // if fetchResponse is an empty optional, the extracter thread has
            // stopped and the transformer should stop as well
            if (!fetchResponse)
            {
                break;
            }
            if (isStopping())
                continue;

            auto [next, accountTxData] =
                buildNextLedger(parent, *fetchResponse);
            // The below line needs to execute before pushing to the queue, in
            // order to prevent this thread and the loader thread from accessing
            // the same SHAMap concurrently
            parent = std::make_shared<Ledger>(*next, NetClock::time_point{});
            loadQueue.push(
                std::make_pair(std::move(next), std::move(accountTxData)));
        }
        // empty optional tells the loader to shutdown
        loadQueue.push({});
    }};

    std::thread loader{
        [this, &lastPublishedSequence, &loadQueue, &writeConflict]() {
            beast::setCurrentThreadName("rippled: ReportingETL load");
            size_t totalTransactions = 0;
            double totalTime = 0;
            while (!writeConflict)
            {
                std::optional<std::pair<
                    std::shared_ptr<Ledger>,
                    std::vector<AccountTransactionsData>>>
                    result{loadQueue.pop()};
                // if result is an empty optional, the transformer thread has
                // stopped and the loader should stop as well
                if (!result)
                    break;
                if (isStopping())
                    continue;

                auto& ledger = result->first;
                auto& accountTxData = result->second;

                auto start = std::chrono::system_clock::now();
                // write to the key-value store
                flushLedger(ledger);

                auto mid = std::chrono::system_clock::now();
                // write to RDBMS
                // if there is a write conflict, some other process has already
                // written this ledger and has taken over as the ETL writer
                if (app_.config().usePostgresLedgerTx())
                    if (!writeToPostgres(
                            ledger->info(),
                            accountTxData,
                            app_.pgPool(),
                            app_.config().useTxTables(),
                            journal_))
                        writeConflict = true;

                auto end = std::chrono::system_clock::now();

                // still publish even if we are relinquishing ETL control
                publishLedger(ledger);
                lastPublishedSequence = ledger->info().seq;
                if (checkConsistency_)
                    assert(checkConsistency(app_.pgPool(), journal_));

                // print some performance numbers
                auto kvTime = ((mid - start).count()) / 1000000000.0;
                auto relationalTime = ((end - mid).count()) / 1000000000.0;

                size_t numTxns = accountTxData.size();
                totalTime += kvTime;
                totalTransactions += numTxns;
                JLOG(journal_.info())
                    << "Load phase of etl : "
                    << "Successfully published ledger! Ledger info: "
                    << toString(ledger->info()) << ". txn count = " << numTxns
                    << ". key-value write time = " << kvTime
                    << ". relational write time = " << relationalTime
                    << ". key-value tps = " << numTxns / kvTime
                    << ". relational tps = " << numTxns / relationalTime
                    << ". total key-value tps = "
                    << totalTransactions / totalTime;
            }
        }};

    // wait for all of the threads to stop
    loader.join();
    extracter.join();
    transformer.join();
    writing_ = false;

    JLOG(journal_.debug()) << __func__ << " : "
                           << "Stopping etl pipeline";

    return lastPublishedSequence;
}

// main loop. The software begins monitoring the ledgers that are validated
// by the nework. The member networkValidatedLedgers_ keeps track of the
// sequences of ledgers validated by the network. Whenever a ledger is validated
// by the network, the software looks for that ledger in the database. Once the
// ledger is found in the database, the software publishes that ledger to the
// ledgers stream. If a network validated ledger is not found in the database
// after a certain amount of time, then the software attempts to take over
// responsibility of the ETL process, where it writes new ledgers to the
// database. The software will relinquish control of the ETL process if it
// detects that another process has taken over ETL.
void
ReportingETL::monitor()
{
    auto ledger = std::const_pointer_cast<Ledger>(
        app_.getLedgerMaster().getValidatedLedger());
    if (!ledger)
    {
        JLOG(journal_.info()) << __func__ << " : "
                              << "Database is empty. Will download a ledger "
                                 "from the network.";
        if (startSequence_)
        {
            JLOG(journal_.info())
                << __func__ << " : "
                << "ledger sequence specified in config. "
                << "Will begin ETL process starting with ledger "
                << *startSequence_;
            ledger = loadInitialLedger(*startSequence_);
        }
        else
        {
            JLOG(journal_.info())
                << __func__ << " : "
                << "Waiting for next ledger to be validated by network...";
            std::optional<uint32_t> mostRecentValidated =
                networkValidatedLedgers_.getMostRecent();
            if (mostRecentValidated)
            {
                JLOG(journal_.info()) << __func__ << " : "
                                      << "Ledger " << *mostRecentValidated
                                      << " has been validated. "
                                      << "Downloading...";
                ledger = loadInitialLedger(*mostRecentValidated);
            }
            else
            {
                JLOG(journal_.info()) << __func__ << " : "
                                      << "The wait for the next validated "
                                      << "ledger has been aborted. "
                                      << "Exiting monitor loop";
                return;
            }
        }
    }
    else
    {
        if (app_.config().START_UP == Config::FRESH)
        {
            Throw<std::runtime_error>(
                "--startReporting passed via command line but db is already "
                "populated");
        }
        JLOG(journal_.info()) << __func__ << " : "
            << "Database already populated. Picking up from the tip of history";
    }
    if(!ledger)
    {
        JLOG(journal_.error()) << __func__ << " : "
            << "Failed to load initial ledger. Exiting monitor loop";
        return;
    }
    else
    {
        publishLedger(ledger);
    }
    uint32_t nextSequence = ledger->info().seq + 1;

    JLOG(journal_.debug()) << __func__ << " : "
                           << "Database is populated. "
                           << "Starting monitor loop. sequence = "
                           << nextSequence;
    while (
        !stopping_ &&
        networkValidatedLedgers_.waitUntilValidatedByNetwork(nextSequence))
    {
        JLOG(journal_.info()) << __func__ << " : "
                              << "Ledger with sequence = " << nextSequence
                              << " has been validated by the network. "
                              << "Attempting to find in database and publish";
        // Attempt to take over responsibility of ETL writer after 10 failed
        // attempts to publish the ledger. publishLedger() fails if the
        // ledger that has been validated by the network is not found in the
        // database after the specified number of attempts. publishLedger()
        // waits one second between each attempt to read the ledger from the
        // database
        //
        // In strict read-only mode, when the software fails to find a
        // ledger in the database that has been validated by the network,
        // the software will only try to publish subsequent ledgers once,
        // until one of those ledgers is found in the database. Once the
        // software successfully publishes a ledger, the software will fall
        // back to the normal behavior of trying several times to publish
        // the ledger that has been validated by the network. In this
        // manner, a reporting processing running in read-only mode does not
        // need to restart if the database is wiped.
        constexpr size_t timeoutSeconds = 10;
        bool success =
            publishLedger(nextSequence, timeoutSeconds);
        if (!success)
        {
            JLOG(journal_.warn()) << __func__ << " : "
                                  << "Failed to publish ledger with sequence = "
                                  << nextSequence << " . Beginning ETL";
            // doContinousETLPipelined returns the most recent sequence
            // published empty optional if no sequence was published
            std::optional<uint32_t> lastPublished =
                runETLPipeline(nextSequence);
            JLOG(journal_.info()) << __func__ << " : "
                                  << "Aborting ETL. Falling back to publishing";
            // if no ledger was published, don't increment nextSequence
            if (lastPublished)
                nextSequence = *lastPublished + 1;
        }
        else
        {
            ++nextSequence;
        }
    }
}

void
ReportingETL::monitorReadOnly()
{
    std::optional<uint32_t> mostRecent =
        networkValidatedLedgers_.getMostRecent();
    if (!mostRecent)
        return;
    uint32_t sequence = *mostRecent;
    bool success = true;
    while (!stopping_ &&
           networkValidatedLedgers_.waitUntilValidatedByNetwork(sequence))
    {
        success = publishLedger(sequence, success ? 30 : 1);
        ++sequence;
    }
}

void
ReportingETL::doWork()
{
    worker_ = std::thread([this]() {
        beast::setCurrentThreadName("rippled: ReportingETL worker");
        if (readOnly_)
            monitorReadOnly();
        else
            monitor();
    });
}

void
ReportingETL::setup()
{
    if (app_.config().START_UP == Config::StartUpType::FRESH && !readOnly_)
    {
        startSequence_ = std::stol(app_.config().START_LEDGER);
    }
    else if (!readOnly_)
    {
        if (checkConsistency_)
            assert(checkConsistency(app_.pgPool(), journal_));
    }
}

ReportingETL::ReportingETL(Application& app, Stoppable& parent)
    : Stoppable("ReportingETL", parent)
    , app_(app)
    , journal_(app.journal("ReportingETL"))
    , publishStrand_(app_.getIOService())
    , loadBalancer_(*this)
{
    // if present, get endpoint from config
    if (app_.config().exists("reporting"))
    {
        Section section = app_.config().section("reporting");

        JLOG(journal_.debug()) << "Parsing config info";

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
                // used in read-only mode to detect when new ledgers have
                // been validated. Used for publishing
                if (app_.config().reportingReadOnly())
                    loadBalancer_.add(ipPair.first, wsPortPair.first);
                continue;
            }

            loadBalancer_.add(
                ipPair.first, wsPortPair.first, grpcPortPair.first);
        }

        readOnly_ = app_.config().reportingReadOnly();

        // don't need to do any more work if we are in read only mode
        if (readOnly_)
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
            if (!app_.config().usePostgresLedgerTx())
                checkConsistency_ = false;
        }
    }
}

}  // namespace ripple
