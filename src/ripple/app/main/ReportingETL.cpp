
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


//TODO manipulate the characters instead of doing addition
//TODO take number of marekrs as argument
std::vector<uint256> getMarkers()
{
    std::vector<uint256> markers;
    uint256 key{0};
    markers.push_back(key);
    uint256 incr{1};
    for(size_t i = 0; i < 252; ++i)
    {
        incr += incr;
    }
    
    for(size_t i = 0; i < 15; ++i)
    {
        key += incr;
        markers.push_back(key);
    }
    return markers;
}

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

class RingBuffer
{
    struct Cell
    {
    private:
        std::mutex m;
        std::condition_variable cv;
        std::string index;
        std::string data;
        bool dirty = true;
        bool finished = false;

    public:
        template <class Func>
        bool
        read(Func f)
        {
            std::unique_lock<std::mutex> lck(m);
            cv.wait(lck, [this]() { return !dirty; });
            if (finished)
                return false;
            f(index, data);
            dirty = true;
            cv.notify_one();
            return true;
        }

        void
        write(std::string&& indexIn, std::string&& dataIn)
        {
            std::unique_lock<std::mutex> lck(m);
            cv.wait(lck, [this]() { return dirty; });
            index = std::move(indexIn);
            data = std::move(dataIn);
            dirty = false;
            cv.notify_one();
        }

        void
        writeFinished()
        {
            std::unique_lock<std::mutex> lck(m);
            cv.wait(lck, [this]() { return dirty; });
            finished = true;
            dirty = false;
            cv.notify_one();
        }
    };

    std::vector<Cell> cells_;
    size_t readIdx_;
    size_t writeIdx_;

public:
    RingBuffer(size_t size) : cells_(size), readIdx_(0), writeIdx_(0)
    {
    }

    void
    push(std::string&& index, std::string&& data)
    {
        cells_[writeIdx_].write(std::move(index), std::move(data));
        writeIdx_ = (writeIdx_ + 1) % cells_.size();
    }

    void
    writeFinished()
    {
        cells_[writeIdx_].writeFinished();
    }

    template <class Func>
    bool
    consume(Func f)
    {
        bool res = cells_[readIdx_].read(f);
        readIdx_ = (readIdx_ + 1) % cells_.size();
        return res;
    }
};

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

                uint32_t ledgerIndex = 0;
                if (response.isMember("result"))
                    ledgerIndex =
                        response["result"][jss::ledger_index].asUInt();
                else
                    ledgerIndex = response[jss::ledger_index].asUInt();
                queue_.push(ledgerIndex);
                JLOG(journal_.info()) << "received ledger = " << ledgerIndex
                   << " on subscription stream";
            }
            JLOG(journal_.info()) << "Exited suscribe loop. Stopping queue";

            queue_.stop();
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

std::vector<TxMeta>
ReportingETL::loadNextLedger()
{
    // ledger header with txns and metadata
    org::xrpl::rpc::v1::GetLedgerResponse reply;
    org::xrpl::rpc::v1::GetLedgerRequest request;
    currentIndex_ = queue_.pop();
    if (currentIndex_ == 0)
    {
        return std::vector<TxMeta>{};
    }
    request.mutable_ledger()->set_sequence(currentIndex_);
    request.set_transactions(true);
    request.set_expand(true);

    bool validated = false;

    int toWait = 1;
    while (not validated and not stopping_)
    {
        grpc::ClientContext grpcContext;
        grpc::Status status = stub_->GetLedger(&grpcContext, request, &reply);
        if (status.ok())
        {
            validated = reply.validated();
            if (!validated)
                std::this_thread::sleep_for(std::chrono::seconds(2));
            toWait = 1;
            continue;
        }
        JLOG(journal_.warn())
            << "Ledger = " << currentIndex_
            << " not validated yet. reply = "
            << reply.DebugString();
        //TODO is this waiting logic still needed?
        std::this_thread::sleep_for(std::chrono::seconds(toWait));
        toWait *= 2;
    }

    std::cout << reply.DebugString() << std::endl;

    std::cout << "deserialize header" << std::endl;
    LedgerInfo lgrInfo = deserializeHeader(
        makeSlice(reply.ledger_header()), true);
    currentIndex_ = lgrInfo.seq;

    JLOG(journal_.info()) << "Loading ledger header : "
                           << " seq = " << lgrInfo.seq << " hash - "
                           << to_string(lgrInfo.hash) << " account_hash = "
                           << to_string(lgrInfo.accountHash)
                           << " tx_hash = " << to_string(lgrInfo.txHash);

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

    JLOG(journal_.debug()) << "Loading transactions...";
    for (auto& txn : reply.transactions_list().transactions())
    {
        auto& raw = txn.transaction_blob();

        SerialIter it{raw.data(), raw.size()};
        STTx sttx{it};

        auto txSerializer = std::make_shared<Serializer>(sttx.getSerializer());

        TxMeta txMeta{sttx.getTransactionID(),
                      ledger_->info().seq,
                      txn.metadata_blob()};
        metas.push_back(txMeta);


        auto metaSerializer =
            std::make_shared<Serializer>(txMeta.getAsObject().getSerializer());


        if (!ledger_->txExists(sttx.getTransactionID()))
        {
            ledger_->rawTxInsert(
                sttx.getTransactionID(), txSerializer, metaSerializer);
            JLOG(journal_.debug()) << "Inserted transaction = "
                << strHex(sttx.getTransactionID())
                << " into ledger = " << currentIndex_;
        }
        else
        {
            JLOG(journal_.warn())
                << " Transaction = " << strHex(sttx.getTransactionID())
                << " already exists in ledger = " << currentIndex_;
        }
    }
    JLOG(journal_.debug())
        << "Loaded transactions for ledger = " << currentIndex_;
    return metas;
}

void ReportingETL::doInitialLedgerLoad()
{
    //TODO change to switch
    if(method_ == LoadMethod::ITERATIVE)
    {
        loadIterative();
    }
    else if(method_ == LoadMethod::BUFFER)
    {
        loadBuffer();
    }
    else if(method_ == LoadMethod::PARALLEL)
    {
        loadParallel();
    }
    else if(method_ == LoadMethod::ASYNC)
    {
        loadAsync();
    }
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
        bool flush,
        bool asyncFlush,
        ReportingETL::ThreadSafeQueue& queue)
    {
        /*
           std::cout << status.error_code() 
           << " : " << status.error_message()
           << " : " << next->DebugString() << std::endl;
           */
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
            if(asyncFlush)
                queue.push(sle);
            else
                if (!ledger->exists(sle->key()))
                    ledger->rawInsert(sle);
        }
        if(flush and not asyncFlush)
        {
            ledger->stateMap().flushDirty(
                    hotACCOUNT_NODE, ledger->info().seq);
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

void ReportingETL::doAsyncFlush()
{
    flusher_ = std::thread{[this](){
        
        std::shared_ptr<SLE> sle;
        size_t num = 0;
        //TODO: if this call blocks, flushDirty in the meantime
        while((sle = flushQueue_.pop()))
        {
            assert(sle);
            if(!ledger_->exists(sle->key()))
                ledger_->rawInsert(sle);

            if(flushInterval_ != 0 and (num % flushInterval_) == 0)
            {
                JLOG(journal_.debug()) << "Flushing! key = "
                    << strHex(sle->key());
                ledger_->stateMap().flushDirty(
                        hotACCOUNT_NODE, ledger_->info().seq);
            }
            if(num % 256 == 0)
            {
                JLOG(journal_.debug())
                    << "Downloaded " << num << " ledger objects";
            }
            ++num;
        }
        ledger_->stateMap().flushDirty(
                hotACCOUNT_NODE, ledger_->info().seq);
    }};

}

void ReportingETL::loadAsync()
{
    grpc::CompletionQueue cq;

    void* tag;

    bool ok = false;

    if(asyncFlush_)
    {
        doAsyncFlush();
    }


    std::vector<AsyncCallData> calls;
    std::vector<uint256> markers{getMarkers(parallelism_)};


    for(size_t i = 0; i < markers.size(); ++i)
    {
        std::optional<uint256> nextMarker;
        if(i+1 < markers.size())
            nextMarker = markers[i+1];
        calls.emplace_back(markers[i], nextMarker, currentIndex_, journal_);
    }

    JLOG(journal_.debug()) << "Starting data download. method == ASYNC";

    auto start = std::chrono::system_clock::now();
    for(auto& c : calls)
        c.call(stub_, cq);

    size_t numFinished = 0;
    size_t numCalls = 0;
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
            bool flush = false;
            if(flushDuringDownload_)
                if(flushInterval_ != 0 and (numCalls % flushInterval_) == 0)
                    flush = true;
            if(!ptr->process(ledger_, stub_, cq, flush, asyncFlush_, flushQueue_))
            {

                auto interim = std::chrono::system_clock::now();
                std::chrono::duration<double> diff = interim - start;
                numFinished++;
                JLOG(journal_.debug()) << "Finished a marker. "
                    << "Current number of finished = " << numFinished
                    << " . Seconds = " << diff.count();
            }
            numCalls++;
        }
    }
    auto interim = std::chrono::system_clock::now();
    std::chrono::duration<double> diff = interim - start;
    JLOG(journal_.debug()) << "Time to page through ledger via async = "
                           << diff.count() << "seconds";

    if(asyncFlush_)
    {
        flushQueue_.finish();
        flusher_.join();
    }
    auto end = std::chrono::system_clock::now();

    diff = end - start;
    JLOG(journal_.debug()) << "Time to download ledger via async = "
                           << diff.count() << "seconds";
    JLOG(journal_.debug()) << "Done downloading initial ledger";
}

//TODO insert into ledger in parallel
//Use ringbuffer? Or something simpler?
void ReportingETL::loadParallel()
{
    auto markers = getMarkers();
    std::vector<std::vector<std::shared_ptr<SLE>>> sles{markers.size()};
    std::vector<std::thread> threads;
    // std::mutex ledgerMutex;
    JLOG(journal_.debug()) << "starting data download. method == PARALLEL";
    auto start = std::chrono::system_clock::now();

    for (size_t i = 0; i < markers.size(); ++i)
    {
        auto& marker = markers[i];
        std::optional<uint256> nextMarker;
        if (i + 1 < markers.size())
        {
            nextMarker = markers[i + 1];
        }
        auto& vec = sles[i];

        threads.emplace_back([marker, nextMarker, &vec, this]() {
            // all of the ledger data
            org::xrpl::rpc::v1::GetLedgerDataResponse replyData;
            org::xrpl::rpc::v1::GetLedgerDataRequest requestData;
            grpc::ClientContext context;
            unsigned char nextPrefix = 0x00;
            if (nextMarker)
                nextPrefix = nextMarker->data()[0];

            requestData.mutable_ledger()->set_sequence(this->currentIndex_);
            if (marker != 0)
                requestData.set_marker(marker.data(), marker.size());

            // if (nextMarker)
            //    requestDataLcl.set_end_marker(
            //        nextMarker->data(), nextMarker->size());
            grpc::Status status = stub_->GetLedgerData(
                &context, requestData, &replyData);

            while (not stopping_)
            {
                if (replyData.marker().size() > 0)
                {
                    std::string firstChar{replyData.marker()[0]};
                    JLOG(journal_.debug())
                        << "marker char = " << strHex(firstChar);
                }
                else
                {
                    JLOG(journal_.debug()) << "empty marker";
                }

                for (auto& state : replyData.state_objects())
                {
                    auto& index = state.index();
                    auto& data = state.data();

                    auto key = uint256::fromVoid(index.data());

                    SerialIter it{data.data(), data.size()};
                    std::shared_ptr<SLE> sle = std::make_shared<SLE>(it, key);
                    vec.push_back(sle);
                }

                if (replyData.marker().size() == 0)
                    break;
                unsigned char prefix = replyData.marker()[0];
                if (nextPrefix != 0x00 and prefix >= nextPrefix)
                    break;

                grpc::ClientContext contextLocal;
                requestData.set_marker(std::move(replyData.marker()));
                status = stub_->GetLedgerData(
                    &contextLocal, requestData, &replyData);
            }
        });
    }

    for (auto& t : threads)
    {
        t.join();
    }

    auto interm = std::chrono::system_clock::now();

    std::chrono::duration<double> diff = interm - start;
    JLOG(journal_.debug()) << "Finished downloading raw data. "
        << "Time to download raw data via parallel = "
                           << diff.count() << "seconds";
    JLOG(journal_.debug())
        << "Finished downloaded raw data, inserting into ledger";

    //TODO do this while data is being received
    for (auto& vec : sles)
    {
        for (auto& sle : vec)
        {
            JLOG(journal_.trace()) << "Inserting SLE = " << strHex(sle->key());
            if (!ledger_->exists(sle->key()))
                ledger_->rawInsert(sle);
        }
    }
    auto end = std::chrono::system_clock::now();

    diff = end - start;
    JLOG(journal_.debug()) << "Time to download ledger via parallel = "
                           << diff.count() << "seconds";
}

void ReportingETL::loadBuffer()
{
    // all of the ledger data
    org::xrpl::rpc::v1::GetLedgerDataResponse replyData;
    org::xrpl::rpc::v1::GetLedgerDataRequest requestData;

    grpc::ClientContext context;
    requestData.mutable_ledger()->set_sequence(this->currentIndex_);
    RingBuffer buffer(25);

    std::thread reader{[&buffer, this]() {
        bool more = true;
        while (more and not stopping_)
        {
            more =
                buffer.consume([this](std::string& index, std::string& data) {
                    auto key = uint256::fromVoid(index.data());

                    SerialIter it{data.data(), data.size()};
                    std::shared_ptr<SLE> sle = std::make_shared<SLE>(it, key);
                    if (!this->ledger_->exists(key))
                        this->ledger_->rawInsert(sle);
                });
        }
    }};

    JLOG(journal_.debug()) << "starting data download. method == BUFFER";
    auto start = std::chrono::system_clock::now();
    grpc::Status status =
        stub_->GetLedgerData(&context, requestData, &replyData);

    // TODO: improve the performance of this
    while (not stopping_)
    {
        if (replyData.marker().size() > 0)
        {
            std::string firstChar{replyData.marker()[0]};
            JLOG(journal_.trace()) << "marker char = " << strHex(firstChar);
        }
        else
        {
            JLOG(journal_.trace()) << "empty marker";
        }
        for (auto& state : *replyData.mutable_state_objects())
        {
            auto& index = *state.mutable_index();
            auto& data = *state.mutable_data();
            buffer.push(std::move(index), std::move(data));
        }

        if (replyData.marker().size() == 0)
        {
            buffer.writeFinished();
            break;
        }

        grpc::ClientContext contextLocal;
        requestData.set_marker(std::move(replyData.marker()));
        status =
            stub_->GetLedgerData(&contextLocal, requestData, &replyData);
    }
    reader.join();
    auto end = std::chrono::system_clock::now();

    std::chrono::duration<double> diff = end - start;
    JLOG(journal_.debug()) << "Time to download ledger via buffer = "
                           << diff.count() << "seconds";
}

void ReportingETL::loadIterative()
{
    
    grpc::ClientContext context;
    org::xrpl::rpc::v1::GetLedgerDataResponse replyData;
    org::xrpl::rpc::v1::GetLedgerDataRequest requestData;

    requestData.mutable_ledger()->set_sequence(currentIndex_);
    JLOG(journal_.debug()) << "starting data download. method == ITERATIVE";
    auto start = std::chrono::system_clock::now();
    grpc::Status status =
        stub_->GetLedgerData(&context, requestData, &replyData);
    // TODO: improve the performance of this
    while (not stopping_)
    {
        if (replyData.marker().size() > 0)
        {
            std::string firstChar{replyData.marker()[0]};
            JLOG(journal_.trace()) << "marker char = " << strHex(firstChar);
        }
        else
        {
            JLOG(journal_.trace()) << "empty marker";
        }

        for (auto& state : replyData.state_objects())
        {
            auto& index = state.index();
            auto& data = state.data();

            auto key = uint256::fromVoid(index.data());

            SerialIter it{data.data(), data.size()};
            std::shared_ptr<SLE> sle = std::make_shared<SLE>(it, key);
            if (!ledger_->exists(key))
                ledger_->rawInsert(sle);
        }

        if (replyData.marker().size() == 0)
            break;

        grpc::ClientContext contextLocal;
        requestData.set_marker(std::move(replyData.marker()));
        status =
            stub_->GetLedgerData(&contextLocal, requestData, &replyData);
    }

    auto end = std::chrono::system_clock::now();

    std::chrono::duration<double> diff = end - start;
    JLOG(journal_.debug()) << "Time to download ledger via iterative = "
                           << diff.count() << "seconds";
}

void
ReportingETL::storeLedger()
{

    JLOG(journal_.debug()) << "Storing ledger = "
        << currentIndex_;
    ledger_->setImmutable(app_.config());


    auto start = std::chrono::system_clock::now();

    ledger_->stateMap().flushDirty(
        hotACCOUNT_NODE, ledger_->info().seq);

    ledger_->txMap().flushDirty(
        hotTRANSACTION_NODE, ledger_->info().seq);


    auto interim = std::chrono::system_clock::now();

    std::chrono::duration<double> diff = interim - start;

    JLOG(journal_.debug()) << "Time to flush dirty = "
                           << diff.count() << "seconds";

    JLOG(journal_.debug()) << "header account hash = "
        << strHex(ledger_->info().accountHash)
        << " . calculated account hash = "
        << strHex(ledger_->stateMap().getHash().as_uint256());

    assert(
        ledger_->txMap().getHash().as_uint256() ==
        ledger_->info().txHash);

    assert(
        ledger_->stateMap().getHash().as_uint256() ==
        ledger_->info().accountHash);

    app_.setOpenLedger(ledger_);

    app_.getLedgerMaster().storeLedger(ledger_);

    app_.getLedgerMaster().switchLCL(ledger_);
    auto end = std::chrono::system_clock::now();

    diff = end - start;
    JLOG(journal_.debug()) << "Time to store ledger = "
                           << diff.count() << "seconds";
    
    JLOG(journal_.info()) << "SUCCESS!!! Stored ledger = "
        << currentIndex_;
}

void
ReportingETL::continousUpdate()
{
    while (not stopping_)
    {
        auto metas = loadNextLedger();
        // signals stop
        if (metas.size() == 0)
            continue;
        std::set<uint256> indices;
        for (auto& meta : metas)
        {
            STArray& nodes = meta.getNodes();
            for (auto it = nodes.begin(); it != nodes.end(); ++it)
            {
                // ledger index
                indices.insert(it->getFieldH256(sfLedgerIndex));
            }
        }
        JLOG(journal_.info()) << "Ledger = " << currentIndex_
            << "transactions = " << metas.size()
            << "objects = " << indices.size();

        for (auto iter = indices.begin(); iter != indices.end();)
        {
            auto& idx = *iter;
            org::xrpl::rpc::v1::GetLedgerEntryResponse replyEntry;
            org::xrpl::rpc::v1::GetLedgerEntryRequest requestEntry;

            grpc::ClientContext grpcContextEntry;
            requestEntry.mutable_ledger()->set_sequence(currentIndex_);
            requestEntry.set_index(idx.data(), idx.size());
            grpc::Status status = stub_->GetLedgerEntry(
                &grpcContextEntry, requestEntry, &replyEntry);



            JLOG(journal_.trace()) 
                << "index = " << idx
                << "error_message = " << status.error_message()
                << " . error_code = " << status.error_code()
                << " reply = " << replyEntry.DebugString();

            if (status.error_code() == grpc::StatusCode::NOT_FOUND)
            {
                if (ledger_->exists(idx))
                {
                    JLOG(journal_.trace()) << "erasing = " << idx;
                    ledger_->rawErase(idx);
                }
                else
                {
                    JLOG(journal_.trace()) << "noop = " << idx;
                }
                ++iter;
            }
            else if (status.ok())
            {
                auto& objRaw = replyEntry.object_binary();
                SerialIter it{objRaw.data(), objRaw.size()};

                std::shared_ptr<SLE> sle = std::make_shared<SLE>(it, idx);
                if (ledger_->exists(idx))
                {

                    JLOG(journal_.trace()) << "replacing = " << idx;
                    ledger_->rawReplace(sle);
                }
                else
                {

                    JLOG(journal_.trace()) << "inserting = " << idx;
                    ledger_->rawInsert(sle);
                }
                ++iter;
            }
            else if (status.error_code() == 8)
            {
                JLOG(journal_.warn()) << "usage balance. pausing";

                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
            else
            {
                JLOG(journal_.warn()) << "unexpected error, trying again";
            }
        }
        JLOG(journal_.debug())
            << "Updated ledger = " << currentIndex_;

        ledger_->updateSkipList();
        storeLedger();
        JLOG(journal_.info()) << "SUCCESS! Stored ledger = "
                << currentIndex_;
    }
}

void
ReportingETL::doWork()
{
    worker_ = std::thread([this]() {

        JLOG(journal_.info()) << "Starting worker";
        loadNextLedger();

        JLOG(journal_.info()) << "Downloading initial ledger";

        doInitialLedgerLoad();

        JLOG(journal_.info()) << "Done downloading initial ledger";



        storeLedger();
        JLOG(journal_.info()) << "Stored initial ledger! "
        << "Starting continous update";
        if (onlyDownload_)
            return;

        continousUpdate();
    });
}
}  // namespace ripple
