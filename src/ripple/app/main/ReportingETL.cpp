
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
        try
        {
            auto const host = ip_;
            auto const port = wsPort_;

            // The io_context is required for all I/O
            net::io_context ioc;

            // These objects perform our I/O
            tcp::resolver resolver{ioc};
            websocket::stream<tcp::socket> ws{ioc};

            // Look up the domain name
            auto const results = resolver.resolve(host, port);

            // Make the connection on the IP address we get from a lookup
            net::connect(ws.next_layer(), results.begin(), results.end());

            // Set a decorator to change the User-Agent of the handshake
            ws.set_option(websocket::stream_base::decorator(
                [](websocket::request_type& req) {
                    req.set(
                        http::field::user_agent,
                        std::string(BOOST_BEAST_VERSION_STRING) +
                            " websocket-client-coro");
                }));

            // Perform the websocket handshake
            ws.handshake(host, "/");

            Json::Value jv;
            jv["command"] = "subscribe";

            jv["streams"] = Json::arrayValue;
            Json::Value stream("ledger");
            jv["streams"].append(stream);
            Json::FastWriter fastWriter;

            // Send the message
            ws.write(net::buffer(fastWriter.write(jv)));

            while (not stopping_)
            {
                // This buffer will hold the incoming message
                beast::flat_buffer buffer;
                // Read a message into our buffer
                ws.read(buffer);
                Json::Value response;
                Json::Reader reader;
                reader.parse(
                    static_cast<char const*>(buffer.data().data()), response);
                JLOG(journal_.info()) << "sub message = "
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

            indexQueue_.stop();
            JLOG(journal_.info()) << "Stopped queue";

            // Close the WebSocket connection
            ws.close(websocket::close_code::normal);

            // If we get here then the connection is closed gracefully
        }
        catch (std::exception const& e)
        {
            std::cerr << "Error: " << e.what() << std::endl;
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
        std::cout << "setting marker = " << strHex(marker) << std::endl;

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
        while ((sle = writeQueue_.pop()))
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
        ledger_->stateMap().flushDirty(
                hotACCOUNT_NODE, ledger_->info().seq);
    }};
}

void
ReportingETL::loadInitialLedger()
{
    org::xrpl::rpc::v1::GetLedgerResponse response;
    fetchLedger(response, false);
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
    flushLedger();
    storeLedger();
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

    ledger_->setImmutable(app_.config());

    ledger_->stateMap().flushDirty(
        hotACCOUNT_NODE, ledger_->info().seq);

    ledger_->txMap().flushDirty(
        hotTRANSACTION_NODE, ledger_->info().seq);

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

void
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
        return;
    }

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
ReportingETL::doETL()
{
    org::xrpl::rpc::v1::GetLedgerResponse fetchResponse;

    fetchLedger(fetchResponse);

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
