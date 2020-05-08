
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

#include <ripple/app/main/ETLSource.h>
#include <ripple/app/main/ReportingETL.h>
#include <ripple/json/json_reader.h>
#include <ripple/json/json_writer.h>

namespace ripple {

// Create ETL source without grpc endpoint
// Fetch ledger and load initial ledger will fail for this source
// Primarly used in read-only mode, to monitor when ledgers are validated
ETLSource::ETLSource(std::string ip, std::string wsPort, ReportingETL& etl)
    : ip_(ip)
    , wsPort_(wsPort)
    , etl_(etl)
    , ws_(std::make_unique<
          boost::beast::websocket::stream<boost::beast::tcp_stream>>(
          boost::asio::make_strand(etl_.getIOContext())))
    , resolver_(boost::asio::make_strand(etl_.getIOContext()))
    , indexQueue_(etl_.getLedgerIndexQueue())
    , journal_(etl_.getApplication().journal("ReportingETL::ETLSource"))
    , app_(etl_.getApplication())
    , timer_(etl_.getIOContext())
{
}

ETLSource::ETLSource(
    std::string ip,
    std::string wsPort,
    std::string grpcPort,
    ReportingETL& etl)
    : ip_(ip)
    , wsPort_(wsPort)
    , grpcPort_(grpcPort)
    , etl_(etl)
    , ws_(std::make_unique<
          boost::beast::websocket::stream<boost::beast::tcp_stream>>(
          boost::asio::make_strand(etl_.getIOContext())))
    , resolver_(boost::asio::make_strand(etl_.getIOContext()))
    , indexQueue_(etl_.getLedgerIndexQueue())
    , journal_(etl_.getApplication().journal("ReportingETL::ETLSource"))
    , app_(etl_.getApplication())
    , timer_(etl_.getIOContext())
{
    try
    {
        stub_ = org::xrpl::rpc::v1::XRPLedgerAPIService::NewStub(
            grpc::CreateChannel(
                beast::IP::Endpoint(
                    boost::asio::ip::make_address(ip_), std::stoi(grpcPort_))
                    .to_string(),
                grpc::InsecureChannelCredentials()));
        JLOG(journal_.info()) << "Made stub for remote = " << toString();
    }
    catch (std::exception const& e)
    {
        JLOG(journal_.error()) << "Exception while creating stub = " << e.what()
                               << " . Remote = " << toString();
    }
}

void
ETLSource::restart(boost::beast::error_code ec)
{
    JLOG(journal_.error()) << "restart() : error_code = " << ec << " : "
                           << toString();

    if (etl_.isStopping())
    {
        JLOG(journal_.debug())
            << "restart() : " << toString() << " - etl is stopping. aborting";
        return;
    }

    size_t waitTime = std::min(pow(2, numFailures), 30.0);
    numFailures++;
    timer_.expires_after(boost::asio::chrono::seconds(waitTime));
    timer_.async_wait([this](auto ec) {
        JLOG(journal_.debug()) << "async_wait - ec " << ec;
        close(true);
    });
}

void
ETLSource::close(bool startAgain)
{
    timer_.cancel();
    etl_.getIOContext().post([this, startAgain]() {
        if (closing)
            return;

        if (ws_->is_open())
        {
            // onStop() also calls close(). If the async_close is called twice,
            // an assertion fails. Using closing makes sure async_close is only
            // called once
            closing = true;
            ws_->async_close(
                boost::beast::websocket::close_code::normal,
                [this, startAgain](auto ec) {
                    if (ec)
                    {
                        JLOG(journal_.error())
                            << "restart()-async_close : error_code = " << ec
                            << " : " << toString();
                    }
                    closing = false;
                    if (startAgain)
                        start();
                });
        }
        else if (startAgain)
        {
            start();
        }
    });
}

void
ETLSource::start()
{
    JLOG(journal_.debug()) << "start() : " << toString();

    auto const host = ip_;
    auto const port = wsPort_;

    resolver_.async_resolve(
        host, port, [this](auto ec, auto results) { onResolve(ec, results); });
}

void
ETLSource::onResolve(
    boost::beast::error_code ec,
    boost::asio::ip::tcp::resolver::results_type results)
{
    JLOG(journal_.debug()) << "onResolve() : ec = " << ec << " : "
                           << toString();
    if (ec)
    {
        // try again
        restart(ec);
    }
    else
    {
        boost::beast::get_lowest_layer(*ws_).expires_after(
            std::chrono::seconds(30));
        boost::beast::get_lowest_layer(*ws_).async_connect(
            results, [this](auto ec, auto ep) { onConnect(ec, ep); });
    }
}

void
ETLSource::onConnect(
    boost::beast::error_code ec,
    boost::asio::ip::tcp::resolver::results_type::endpoint_type endpoint)
{
    JLOG(journal_.debug()) << "onConnect() : ec = " << ec << " : "
                           << toString();
    if (ec)
    {
        // start over
        restart(ec);
    }
    else
    {
        numFailures = 0;
        // Turn off timeout on the tcp stream, because websocket stream has it's
        // own timeout system
        boost::beast::get_lowest_layer(*ws_).expires_never();

        // Set suggested timeout settings for the websocket
        ws_->set_option(
            boost::beast::websocket::stream_base::timeout::suggested(
                boost::beast::role_type::client));

        // Set a decorator to change the User-Agent of the handshake
        ws_->set_option(boost::beast::websocket::stream_base::decorator(
            [](boost::beast::websocket::request_type& req) {
                req.set(
                    boost::beast::http::field::user_agent,
                    std::string(BOOST_BEAST_VERSION_STRING) +
                        " websocket-client-async");
            }));

        // Update the host_ string. This will provide the value of the
        // Host HTTP header during the WebSocket handshake.
        // See https://tools.ietf.org/html/rfc7230#section-5.4
        auto host = ip_ + ':' + std::to_string(endpoint.port());
        // Perform the websocket handshake
        ws_->async_handshake(host, "/", [this](auto ec) { onHandshake(ec); });
    }
}

void
ETLSource::onHandshake(boost::beast::error_code ec)
{
    JLOG(journal_.debug()) << "onHandshake() : ec = " << ec << " : "
                           << toString();
    if (ec)
    {
        // start over
        restart(ec);
    }
    else
    {
        Json::Value jv;
        jv["command"] = "subscribe";

        jv["streams"] = Json::arrayValue;
        Json::Value stream("ledger");
        jv["streams"].append(stream);
        Json::FastWriter fastWriter;

        JLOG(journal_.debug()) << "Sending subscribe stream message";
        // Send the message
        ws_->async_write(
            boost::asio::buffer(fastWriter.write(jv)),
            [this](auto ec, size_t size) { onWrite(ec, size); });
    }
}

void
ETLSource::onWrite(boost::beast::error_code ec, size_t bytesWritten)
{
    JLOG(journal_.debug()) << "onWrite() : ec = " << ec << " : " << toString();
    if (ec)
    {
        // start over
        restart(ec);
    }
    else
    {
        ws_->async_read(
            readBuffer_, [this](auto ec, size_t size) { onRead(ec, size); });
    }
}

void
ETLSource::onRead(boost::beast::error_code ec, size_t size)
{
    JLOG(journal_.debug()) << "onRead() : ec = " << ec << " : " << toString();
    // if error or error reading message, start over
    if (ec || !handleMessage())
    {
        restart(ec);
    }
    else
    {
        boost::beast::flat_buffer buffer;
        swap(readBuffer_, buffer);

        JLOG(journal_.debug()) << "calling async_read : " << toString();
        ws_->async_read(
            readBuffer_, [this](auto ec, size_t size) { onRead(ec, size); });
    }
}

bool
ETLSource::handleMessage()
{
    JLOG(journal_.debug()) << "handleMessage() : " << toString();
    try
    {
        Json::Value response;
        Json::Reader reader;
        if (!reader.parse(
                static_cast<char const*>(readBuffer_.data().data()), response))
        {
            JLOG(journal_.error())
                << "Error parsing stream message."
                << " Message = " << readBuffer_.data().data();
            return false;
        }
        JLOG(journal_.info())
            << "Received a message on ledger "
            << " subscription stream. Message : " << response.toStyledString();

        uint32_t ledgerIndex = 0;
        // TODO is this index always validated?
        if (response.isMember("result"))

        {
            if (response["result"].isMember(jss::ledger_index))
            {
                ledgerIndex = response["result"][jss::ledger_index].asUInt();
            }
        }
        else if (response.isMember(jss::ledger_index))
        {
            ledgerIndex = response[jss::ledger_index].asUInt();
        }
        if (response.isMember(jss::validated_ledgers))
        {
            setValidatedRange(response[jss::validated_ledgers].asString());
        }

        if (response.isMember(jss::result) &&
            response[jss::result].isMember(jss::validated_ledgers))
            setValidatedRange(
                response[jss::result][jss::validated_ledgers].asString());
        if (ledgerIndex != 0)
        {
            JLOG(journal_.debug()) << "Pushing ledger index: " << toString();
            indexQueue_.push(ledgerIndex);
            JLOG(journal_.debug()) << "Pushed ledger index : " << toString();
        }
        return true;
    }
    catch (std::exception const& e)
    {
        JLOG(journal_.error()) << "Exception in handleMessage : " << e.what();
        return false;
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
        request.mutable_ledger()->set_sequence(seq);
        if (marker.isNonZero())
        {
            request.set_marker(marker.data(), marker.size());
        }
        nextPrefix = 0x00;
        if (nextMarker)
            nextPrefix = nextMarker->data()[0];

        unsigned char prefix = marker.data()[0];

        JLOG(journal_.debug())
            << "Setting up AsyncCallData. marker = " << strHex(marker)
            << " . prefix = " << strHex(prefix)
            << " . nextPrefix = " << strHex(nextPrefix);

        assert(nextPrefix > prefix || nextPrefix == 0x00);

        cur = std::make_unique<org::xrpl::rpc::v1::GetLedgerDataResponse>();

        next = std::make_unique<org::xrpl::rpc::v1::GetLedgerDataResponse>();

        context = std::make_unique<grpc::ClientContext>();
    }

    enum Status { MORE, DONE, ERROR };
    // TODO change bool to enum. Three possible results. Success + more to do.
    // Success + finished. Error.
    Status
    process(
        std::shared_ptr<Ledger>& ledger,
        std::unique_ptr<org::xrpl::rpc::v1::XRPLedgerAPIService::Stub>& stub,
        grpc::CompletionQueue& cq,
        ThreadSafeQueue<std::shared_ptr<SLE>>& queue,
        bool abort = false)
    {
        JLOG(journal_.debug()) << "Processing calldata";
        if (abort)
        {
            JLOG(journal_.error()) << "AsyncCallData aborted";
            return Status::ERROR;
        }
        if (!status.ok())
        {
            JLOG(journal_.debug()) << "AsyncCallData status not ok: "
                                   << " code = " << status.error_code()
                                   << " message = " << status.error_message();
            return Status::ERROR;
        }

        std::swap(cur, next);

        bool more = true;

        // if no marker returned, we are done
        if (cur->marker().size() == 0)
            more = false;

        // if returned marker is greater than our end, we are done
        unsigned char prefix = cur->marker()[0];
        if (nextPrefix != 0x00 and prefix >= nextPrefix)
            more = false;

        // if we are not done, make the next async call
        if (more)
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

        return more ? Status::MORE : Status::DONE;
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

bool
ETLSource::loadInitialLedger(
    std::shared_ptr<Ledger>& ledger,
    ThreadSafeQueue<std::shared_ptr<SLE>>& writeQueue)
{
    if (!stub_)
        return false;

    grpc::CompletionQueue cq;

    void* tag;

    bool ok = false;

    std::vector<AsyncCallData> calls;
    std::vector<uint256> markers{getMarkers(etl_.getNumMarkers())};
    uint32_t ledgerSequence = ledger->info().seq;

    for (size_t i = 0; i < markers.size(); ++i)
    {
        std::optional<uint256> nextMarker;
        if (i + 1 < markers.size())
            nextMarker = markers[i + 1];
        calls.emplace_back(markers[i], nextMarker, ledgerSequence, journal_);
    }

    JLOG(journal_.debug()) << "Starting data download for ledger "
                           << ledgerSequence;

    for (auto& c : calls)
        c.call(stub_, cq);

    size_t numFinished = 0;
    bool abort = false;
    while (numFinished < calls.size() and not etl_.isStopping() and
           cq.Next(&tag, &ok))
    {
        assert(tag);

        auto ptr = static_cast<AsyncCallData*>(tag);

        if (!ok)
        {
            JLOG(journal_.error()) << "loadInitialLedger - ok is false";
            return false;
            // handle cancelled
        }
        else
        {
            if (ptr->next->marker().size() != 0)
            {
                std::string prefix{ptr->next->marker().data()[0]};
                JLOG(journal_.debug()) << "Marker prefix = " << strHex(prefix);
            }
            else
            {
                JLOG(journal_.debug()) << "Empty marker";
            }
            auto result = ptr->process(ledger, stub_, cq, writeQueue, abort);
            if (result != AsyncCallData::Status::MORE)
            {
                numFinished++;
                JLOG(journal_.debug())
                    << "Finished a marker. "
                    << "Current number of finished = " << numFinished;
            }
            if (result == AsyncCallData::Status::ERROR)
            {
                abort = true;
            }
        }
    }
    return !abort;
}

grpc::Status
ETLSource::fetchLedger(
    org::xrpl::rpc::v1::GetLedgerResponse& out,
    uint32_t ledgerSequence,
    bool getObjects)
{
    if (!stub_)
        return {grpc::StatusCode::INTERNAL, "No Stub"};

    // ledger header with txns and metadata
    org::xrpl::rpc::v1::GetLedgerRequest request;
    grpc::ClientContext context;
    request.mutable_ledger()->set_sequence(ledgerSequence);
    request.set_transactions(true);
    request.set_expand(true);
    request.set_get_objects(getObjects);
    return stub_->GetLedger(&context, request, &out);
}

ETLLoadBalancer::ETLLoadBalancer(ReportingETL& etl)
    : etl_(etl)
    , journal_(etl_.getApplication().journal("ReportingETL::LoadBalancer"))
{
}

void
ETLLoadBalancer::add(
    std::string& host,
    std::string& websocketPort,
    std::string& grpcPort)
{
    std::unique_ptr<ETLSource> ptr =
        std::make_unique<ETLSource>(host, websocketPort, grpcPort, etl_);
    sources_.push_back(std::move(ptr));
}

void
ETLLoadBalancer::add(std::string& host, std::string& websocketPort)
{
    std::unique_ptr<ETLSource> ptr =
        std::make_unique<ETLSource>(host, websocketPort, etl_);
    sources_.push_back(std::move(ptr));
}

void
ETLLoadBalancer::loadInitialLedger(
    std::shared_ptr<Ledger> ledger,
    ThreadSafeQueue<std::shared_ptr<SLE>>& writeQueue)
{
    execute(
        [this, &ledger, &writeQueue](auto& source) {
            bool res = source->loadInitialLedger(ledger, writeQueue);
            if (!res)
            {
                JLOG(journal_.error()) << "Failed to download initial ledger. "
                                       << " Sequence = " << ledger->info().seq
                                       << " source = " << source->toString();
            }
            return res;
        },
        ledger->info().seq);
}

bool
ETLLoadBalancer::fetchLedger(
    org::xrpl::rpc::v1::GetLedgerResponse& out,
    uint32_t ledgerSequence,
    bool getObjects)
{
    return execute(
        [&out, ledgerSequence, getObjects, this](auto& source) {
            grpc::Status status =
                source->fetchLedger(out, ledgerSequence, getObjects);
            if (status.ok() and out.validated())
            {
                JLOG(journal_.info())
                    << "Successfully fetched ledger = " << ledgerSequence
                    << " from source = " << source->toString();
                return true;
            }
            else
            {
                JLOG(journal_.warn())
                    << "Error getting ledger = " << ledgerSequence
                    << " Reply : " << out.DebugString()
                    << " error_code : " << status.error_code()
                    << " error_msg : " << status.error_message()
                    << " source = " << source->toString();
                return false;
            }
        },
        ledgerSequence);
}

template <class Func>
bool
ETLLoadBalancer::execute(Func f, uint32_t ledgerSequence)
{
    srand((unsigned)time(0));
    auto sourceIdx = rand() % sources_.size();
    auto numAttempts = 0;

    // TODO make sure this stopping logic is correct. Maybe return a bool?
    while (!etl_.isStopping())
    {
        auto& source = sources_[sourceIdx];

        JLOG(journal_.debug())
            << "Attempting at source = " << source->toString();
        if (source->hasLedger(ledgerSequence))
        {
            bool res = f(source);
            if (res)
            {
                JLOG(journal_.debug())
                    << "Successfully executed func at source = "
                    << source->toString();
                break;
            }
            else
            {
                JLOG(journal_.warn()) << "Failed to execute func at source = "
                                      << source->toString();
            }
        }
        else
        {
            JLOG(journal_.warn())
                << "Ledger not present at source = " << source->toString();
        }
        sourceIdx = (sourceIdx + 1) % sources_.size();
        numAttempts++;
        if (numAttempts % sources_.size() == 0)
        {
            JLOG(journal_.warn())
                << "Error executing function."
                << " . Tried all sources. Sleeping and trying again";
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }
    return !etl_.isStopping();
}

void
ETLLoadBalancer::start()
{
    for (auto& source : sources_)
        source->start();
    worker_ = std::thread([this]() { etl_.getIOContext().run(); });
}

void
ETLLoadBalancer::stop()
{
    for (auto& source : sources_)
        source->stop();
    if (worker_.joinable())
        worker_.join();
}

}  // namespace ripple
