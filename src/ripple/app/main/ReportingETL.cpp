
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

#include <ripple/app/main/ReportingETL.h>

#include <ripple/json/json_reader.h>
#include <ripple/json/json_writer.h>
#include <ripple/core/Pg.h>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <cstdlib>
#include <iostream>
#include <string>

namespace ripple {



std::vector<uint256> getMarkers(size_t numMarkers)
{
    assert(numMarkers <=  256);

    unsigned char incr = 256 / numMarkers;

    std::vector<uint256> markers;
    uint256 base{0};
    for(size_t i = 0; i < numMarkers; ++i)
    {
        markers.push_back(base);
        base.data()[0] += incr;
    }
    return markers;
}

void
ReportingETL::doSubscribe()
{
    namespace beast = boost::beast;          // from <boost/beast.hpp>
    namespace http = beast::http;            // from <boost/beast/http.hpp>
    namespace websocket = beast::websocket;  // from <boost/beast/websocket.hpp>
    namespace net = boost::asio;             // from <boost/asio.hpp>
    using tcp = boost::asio::ip::tcp;        // from <boost/asio/ip/tcp.hpp>

    subscriber_ = std::thread([this]() {
        // Sends a WebSocket message and prints the response
        // TODO only use async websocket API in this function. Will facilitate
        // cleaner shutdown. Currently, only the read is async, which allows
        // onStop() to terminate the async_read. However, the other websocket
        // calls in this function are synchronous, which doesn't play nice with
        // async_close (called in onStop()). Using the async API may simplify
        // the threading model as well (but maybe not).
        try
        {
            auto const host = ip_;
            auto const port = wsPort_;

            // The io_context is required for all I/O
            net::io_context ioc;

            // These objects perform our I/O
            tcp::resolver resolver{ioc};

            JLOG(journal_.debug()) << "Creating subscriber websocket";
            ws_ = std::make_unique<websocket::stream<tcp::socket>>(ioc);

            // Look up the domain name
            auto const results = resolver.resolve(host, port);

            JLOG(journal_.debug()) << "Connecting subscriber websocket";
            // Make the connection on the IP address we get from a lookup
            net::connect(ws_->next_layer(), results.begin(), results.end());

            // Set a decorator to change the User-Agent of the handshake
            ws_->set_option(websocket::stream_base::decorator(
                [](websocket::request_type& req) {
                    req.set(
                        http::field::user_agent,
                        std::string(BOOST_BEAST_VERSION_STRING) +
                            " websocket-client-coro");
                }));

            JLOG(journal_.debug())
                << "Performing subscriber websocket handshake";
            // Perform the websocket handshake
            ws_->handshake(host, "/");

            Json::Value jv;
            jv["command"] = "subscribe";

            jv["streams"] = Json::arrayValue;
            Json::Value stream("ledger");
            jv["streams"].append(stream);
            Json::FastWriter fastWriter;

            JLOG(journal_.debug()) << "Sending subscribe stream message";
            // Send the message
            ws_->write(net::buffer(fastWriter.write(jv)));

            JLOG(journal_.info()) << "Starting subscription stream loop";
            while (not stopping_)
            {
                // This buffer will hold the incoming message
                beast::flat_buffer buffer;

                JLOG(journal_.debug())
                    << "Calling async read on subscription websocket";

                // Read a message into our buffer
                ws_->async_read(buffer, [this](auto const& ec, auto size) {
                    JLOG(journal_.debug()) << "Subscription callback executed."
                                           << " ec : " << ec;
                });

                JLOG(journal_.debug())
                    << "Running io_context in subscription loop";
                ioc.restart();
                // Note, this returns when there is no more work to do. Usually,
                // the only outstanding work is the above async read. So when
                // ioc.run() returns, the async read has finished (Or the
                // websocket was closed)
                ioc.run();

                JLOG(journal_.debug()) << "ioc.run() returned. Reading message";
                Json::Value response;
                Json::Reader reader;
                if (!reader.parse(
                        static_cast<char const*>(buffer.data().data()),
                        response))
                {
                    JLOG(journal_.error()) << "Error parsing stream message."
                                           << "Exiting subscribe loop";
                    return;
                }
                JLOG(journal_.info()) << "Received a message on ledger "
                                      << " subscription stream. Message : "
                                      << response.toStyledString();

                uint32_t ledgerIndex = 0;
                // TODO is this index always validated?
                if (response.isMember("result"))
                {
                    if (response["result"].isMember(jss::ledger_index))
                    {
                        ledgerIndex =
                            response["result"][jss::ledger_index].asUInt();
                    }
                }
                else if(response.isMember(jss::ledger_index))
                {
                    ledgerIndex = response[jss::ledger_index].asUInt();
                }
                if(ledgerIndex != 0)
                    indexQueue_.push(ledgerIndex);
            }
            JLOG(journal_.info()) << "Exited suscribe loop. Stopping queue";

            // TODO should this be where the index queue is stopped from?
            // indexQueue_.stop();

            // Close the WebSocket connection
            ws_->close(websocket::close_code::normal);

            // If we get here then the connection is closed gracefully
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error())
                << "Error in subscribe loop. Error : " << e.what();
            return;
        }
    });
}

struct AsyncCallData
{
    std::unique_ptr<org::xrpl::rpc::v1::GetLedgerDataResponse> cur;
    std::unique_ptr<org::xrpl::rpc::v1::GetLedgerDataResponse> next;

    org::xrpl::rpc::v1::GetLedgerDataRequest request;
    std::unique_ptr<grpc::ClientContext> context;

    grpc::Status status;

    unsigned char nextPrefix;

    beast::Journal journal_;

    AsyncCallData(
        uint256& marker,
        std::optional<uint256> nextMarker,
        uint32_t seq,
        beast::Journal& j)
        : journal_(j)
    {

        request.mutable_ledger()->set_sequence(seq);
        if(marker.isNonZero())
        {
            request.set_marker(marker.data(), marker.size());
        }
        nextPrefix = 0x00;
        if(nextMarker)
            nextPrefix = nextMarker->data()[0];

        unsigned char prefix = marker.data()[0];


        JLOG(journal_.debug()) << "Setting up AsyncCallData. marker = "
            << strHex(marker) << " . prefix = "
            << strHex(prefix) << " . nextPrefix = "
            << strHex(nextPrefix);

        assert(nextPrefix > prefix || nextPrefix == 0x00); 

        cur = std::make_unique<org::xrpl::rpc::v1::GetLedgerDataResponse>();

        next = std::make_unique<org::xrpl::rpc::v1::GetLedgerDataResponse>();

        context = std::make_unique<grpc::ClientContext>();
    }

    // TODO change bool to enum. Three possible results. Success + more to do.
    // Success + finished. Error.
    bool
    process(
        std::shared_ptr<Ledger>& ledger,
        std::unique_ptr<org::xrpl::rpc::v1::XRPLedgerAPIService::Stub>& stub,
        grpc::CompletionQueue& cq,
        ReportingETL::ThreadSafeQueue<std::shared_ptr<SLE>> & queue)
    {
        JLOG(journal_.debug()) << "Processing calldata";
        if (!status.ok())
        {
            JLOG(journal_.debug()) << "AsyncCallData status not ok: "
                                   << " code = " << status.error_code()
                                   << " message = " << status.error_message();
            return false;
        }

        std::swap(cur, next);

        bool more = true;

        //if no marker returned, we are done
        if (cur->marker().size() == 0)
            more = false;

        //if returned marker is greater than our end, we are done
        unsigned char prefix = cur->marker()[0];
        if (nextPrefix != 0x00 and prefix >= nextPrefix)
            more = false;

        //if we are not done, make the next async call
        if(more)
        {
            request.set_marker(std::move(cur->marker()));
            call(stub, cq);
        }


        for (auto& state : cur->state_objects())
        {
            auto& index = state.index();
            auto& data = state.data();

            auto key = uint256::fromVoid(index.data());

            SerialIter it{data.data(), data.size()};
            std::shared_ptr<SLE> sle = std::make_shared<SLE>(it, key);

            queue.push(sle);
        }

        return more;
    }

    void
    call(
        std::unique_ptr<org::xrpl::rpc::v1::XRPLedgerAPIService::Stub>& stub,
        grpc::CompletionQueue& cq)
    {
        context = std::make_unique<grpc::ClientContext>();

        std::unique_ptr<grpc::ClientAsyncResponseReader<
            org::xrpl::rpc::v1::GetLedgerDataResponse>>
            rpc(stub->PrepareAsyncGetLedgerData(context.get(), request, &cq));
        
        rpc->StartCall();

        rpc->Finish(next.get(), &status, this);
    }
};

void
ReportingETL::startWriter()
{
    writer_ = std::thread{[this]() {
        std::shared_ptr<SLE> sle;
        size_t num = 0;
        //TODO: if this call blocks, flushDirty in the meantime
        while (not stopping_ and (sle = writeQueue_.pop()))
        {
            assert(sle);
            // TODO get rid of this conditional
            if(!ledger_->exists(sle->key()))
                ledger_->rawInsert(sle);

            if(flushInterval_ != 0 and (num % flushInterval_) == 0)
            {
                JLOG(journal_.debug()) << "Flushing! key = "
                    << strHex(sle->key());
                ledger_->stateMap().flushDirty(
                        hotACCOUNT_NODE, ledger_->info().seq);
            }
            ++num;
        }
        if (not stopping_)
            ledger_->stateMap().flushDirty(
                hotACCOUNT_NODE, ledger_->info().seq);
    }};
}

void
ReportingETL::loadInitialLedger()
{
    org::xrpl::rpc::v1::GetLedgerResponse response;
    if (not fetchLedger(response, false))
        return;
    updateLedger(response);

    grpc::CompletionQueue cq;

    void* tag;

    bool ok = false;

    startWriter();

    std::vector<AsyncCallData> calls;
    std::vector<uint256> markers{getMarkers(numMarkers_)};

    // TODO handle queue finishing
    auto idx = ledger_->info().seq;

    for(size_t i = 0; i < markers.size(); ++i)
    {
        std::optional<uint256> nextMarker;
        if(i+1 < markers.size())
            nextMarker = markers[i+1];
        calls.emplace_back(markers[i], nextMarker, idx, journal_);
    }

    JLOG(journal_.debug()) << "Starting data download for ledger " << idx;

    auto start = std::chrono::system_clock::now();
    for(auto& c : calls)
        c.call(stub_, cq);

    size_t numFinished = 0;
    while(numFinished < calls.size() and not stopping_ and cq.Next(&tag, &ok))
    {
        assert(tag);

        auto ptr = static_cast<AsyncCallData*>(tag);

        if(!ok)
        {
            //handle cancelled
        }
        else
        {
            if(ptr->next->marker().size() != 0)
            {
                std::string prefix{ptr->next->marker().data()[0]};
                JLOG(journal_.debug()) << "Marker prefix = " << strHex(prefix);
            }
            else
            {
                JLOG(journal_.debug()) << "Empty marker";
            }
            if (!ptr->process(ledger_, stub_, cq, writeQueue_))
            {
                numFinished++;
                JLOG(journal_.debug())
                    << "Finished a marker. "
                    << "Current number of finished = " << numFinished;
            }
        }
    }
    auto interim = std::chrono::system_clock::now();
    JLOG(journal_.debug()) << "Time to download ledger = "
                           << ((interim - start).count()) / 1000000000.0
                           << " seconds";
    joinWriter();
    // TODO handle case when there is a network error (other side dies)
    // Shouldn't try to flush in that scenario
    if (not stopping_)
    {
        flushLedger();
        storeLedger();
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

    auto numFlushed = ledger_->stateMap().flushDirty(
        hotACCOUNT_NODE, ledger_->info().seq);

    auto numTxFlushed = ledger_->txMap().flushDirty(
        hotTRANSACTION_NODE, ledger_->info().seq);

    JLOG(journal_.debug()) << "Flushed " << numFlushed
        << " nodes to nodestore from stateMap";
    JLOG(journal_.debug()) << "Flushed " << numTxFlushed
        << " nodes to nodestore from txMap";

    assert(numFlushed != 0 or roundMetrics.objectCount == 0);
    assert(numTxFlushed!= 0 or roundMetrics.txnCount == 0);
    
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
ReportingETL::storeLedger()
{
    auto start = std::chrono::system_clock::now();

    app_.getLedgerMaster().storeLedger(ledger_);

    app_.getLedgerMaster().switchLCL(ledger_);

    auto end = std::chrono::system_clock::now();

    roundMetrics.storeTime = ((end - start).count()) / 1000000000.0;

    JLOG(journal_.debug()) << "Store time for ledger " << ledger_->info().seq
                           << " = " << roundMetrics.storeTime;
}

bool
ReportingETL::fetchLedger(
    org::xrpl::rpc::v1::GetLedgerResponse& out,
    bool getObjects)
{
    // ledger header with txns and metadata
    org::xrpl::rpc::v1::GetLedgerRequest request;

    auto idx = indexQueue_.pop();
    // 0 represents the queue is shutting down
    if (idx == 0)
    {
        JLOG(journal_.debug()) << "Popped 0 from index queue. Stopping";
        return false;
    }

    if (ledger_)
        assert(idx = ledger_->info().seq + 1);

    request.mutable_ledger()->set_sequence(idx);
    request.set_transactions(true);
    request.set_expand(true);
    request.set_get_objects(getObjects);

    // TODO make sure this stopping logic is correct. Maybe return a bool?
    while (not stopping_)
    {
        grpc::ClientContext context;
        auto start = std::chrono::system_clock::now();
        grpc::Status status = stub_->GetLedger(&context, request, &out);
        auto end = std::chrono::system_clock::now();
        if (status.ok() and out.validated())
        {
            JLOG(journal_.debug()) << "Fetch time for ledger " << idx << " = "
                                   << (end - start).count();
            break;
        }
        else
        {
            JLOG(journal_.warn()) << "Error getting ledger = " << idx
                                  << " Reply : " << out.DebugString()
                                  << " error_code : " << status.error_code()
                                  << " error_msg : " << status.error_message()
                                  << " sleeping for two seconds...";
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }
    JLOG(journal_.trace()) << "GetLedger reply : " << out.DebugString();
    return not stopping_;
}

void
ReportingETL::updateLedger(org::xrpl::rpc::v1::GetLedgerResponse& in)
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
        ledger_ =
            std::make_shared<Ledger>(*ledger_, NetClock::time_point{});
        ledger_->setLedgerInfo(lgrInfo);
    }

    ledger_->stateMap().clearSynching();
    ledger_->txMap().clearSynching();
    std::vector<TxMeta> metas;

    for (auto& txn : in.transactions_list().transactions())
    {
        auto& raw = txn.transaction_blob();

        // TODO can this be done faster? Move?
        SerialIter it{raw.data(), raw.size()};
        STTx sttx{it};

        auto txSerializer = std::make_shared<Serializer>(sttx.getSerializer());

        TxMeta txMeta{sttx.getTransactionID(),
                      ledger_->info().seq,
                      txn.metadata_blob()};

        auto metaSerializer =
            std::make_shared<Serializer>(txMeta.getAsObject().getSerializer());

        JLOG(journal_.trace())
            << "Inserting transaction = " << sttx.getTransactionID();
        ledger_->rawTxInsert(
            sttx.getTransactionID(), txSerializer, metaSerializer);

        //TODO use emplace to avoid this copy
        metas.push_back(txMeta);
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

    writeToPostgres(lgrInfo, metas);

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
    LedgerInfo& info,
    std::shared_ptr<PgQuery>& pgQuery,
    std::shared_ptr<Pg>& conn,
    beast::Journal& journal)
{

    auto cmd = boost::format(
            R"(INSERT INTO ledgers 
                VALUES(%u,'\x%s', '\x%s',%u,%u,%u,%u,%u,'\x%s','\x%s')
                ;)");

    auto ledgerInsert = boost::str(cmd 
            % info.seq 
            % strHex(info.hash) 
            % strHex(info.parentHash)
            % info.drops.drops() 
            % info.closeTime.time_since_epoch().count() 
            % info.parentCloseTime.time_since_epoch().count()
            % info.closeTimeResolution.count() 
            % info.closeFlags 
            % strHex(info.accountHash)
            % strHex(info.txHash)
            );
    JLOG(journal.debug()) << "writeToTxDB - ledgerInsert = " << ledgerInsert;
    auto res = pgQuery->querySync(ledgerInsert.data());

    assert(res);
    auto result = PQresultStatus(res.get());
    assert(result == PGRES_COMMAND_OK);
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

    JLOG(journal.debug()) << "writeToTxDB - COPY result = " << result;

    // Write data to stream
    std::stringstream copyBuffer;
    for(auto& m : metas)
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
            JLOG(journal.debug()) << acct;
            /*
            JLOG(journal.debug())
                << "writing to account_transactions - "
                << " account = " << acct << " ledgerSeq = " << ledgerSeq
                << " idx = " << idx;
                */
        }
    }


    PQsetnonblocking(conn->getConn(), 0);

    std::string bufString = copyBuffer.str();
    JLOG(journal.debug()) << "copy buffer = " << bufString;

    // write the data to Postgres
    auto resCode =
        PQputCopyData(conn->getConn(), bufString.c_str(), bufString.size());

    auto pqResult = PQgetResult(conn->getConn());
    auto pqResultStatus = PQresultStatus(pqResult);
    JLOG(journal.debug()) << "putCopyData - resultCode = " << resCode
                          << " result = " << pqResultStatus;
    assert(resCode != -1);
    assert(resCode != 0);
    assert(pqResultStatus == 4);

    // Tell Postgres we are done with the COPY operation
    resCode = PQputCopyEnd(conn->getConn(), nullptr);
    pqResult = PQgetResult(conn->getConn());
    pqResultStatus = PQresultStatus(pqResult);

    JLOG(journal.debug()) << "putCopyEnd - resultCode = " << resCode
                          << " result = " << pqResultStatus
                          << " error_msg = " << PQerrorMessage(conn->getConn());
    assert(resCode != -1);
    assert(resCode != 0);
    assert(pqResultStatus != 7);
}

void
ReportingETL::writeToPostgres(LedgerInfo& info, std::vector<TxMeta>& metas)
{
    // TODO: clean this up a bit. use less auto, better error handling, etc
    JLOG(journal_.debug()) << "writeToTxDB";
    assert(app_.pgPool());
    std::shared_ptr<PgQuery> pg = std::make_shared<PgQuery>(app_.pgPool());
    std::shared_ptr<Pg> conn;
    JLOG(journal_.debug()) << "createdPqQuery";

    auto res = pg->querySync("BEGIN;");
    assert(res);
    auto result = PQresultStatus(res.get());
    JLOG(journal_.debug()) << "writeToTxDB - BEGIN result = " << result;
    assert(result == PGRES_COMMAND_OK);

    writeToLedgersDB(info, pg, conn, journal_);

    writeToAccountTransactionsDB(metas, pg, conn, journal_);

    // this works
    auto execRes = PQexec(conn->getConn(), "COMMIT;");
    assert(execRes);
    result = PQresultStatus(execRes);

    // this doesn't work
    // res = pg->querySync("COMMIT;", conn);
    // assert(res);
    // result = PQresultStatus(res.get());

    JLOG(journal_.debug()) << "writeToTxDB - COMMIT result = " << result;
    assert(result == PGRES_COMMAND_OK);
    PQsetnonblocking(conn->getConn(), 1);
}

void
ReportingETL::doETL()
{
    org::xrpl::rpc::v1::GetLedgerResponse fetchResponse;

    if (not fetchLedger(fetchResponse))
        return;

    updateLedger(fetchResponse);

    flushLedger();

    storeLedger();

    outputMetrics();
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
    worker_ = std::thread([this]() {

        JLOG(journal_.info()) << "Starting worker";

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
            if(numLoops == 10)
                totalMetrics = {};
        }
    });
}
}  // namespace ripple
