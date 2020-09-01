
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

#include <ripple/app/reporting/ETLSource.h>
#include <ripple/app/reporting/ReportingETL.h>
#include <ripple/beast/core/CurrentThreadName.h>
#include <ripple/json/json_reader.h>
#include <ripple/json/json_writer.h>

namespace ripple {

// Create ETL source without grpc endpoint
// Fetch ledger and load initial ledger will fail for this source
// Primarly used in read-only mode, to monitor when ledgers are validated
ETLSource::ETLSource(
    std::string ip,
    std::string wsPort,
    ReportingETL& etl)
    : ip_(ip)
    , wsPort_(wsPort)
    , etl_(etl)
    , ioc_(etl.getApplication().getIOService())
    , ws_(std::make_unique<
          boost::beast::websocket::stream<boost::beast::tcp_stream>>(
          boost::asio::make_strand(ioc_)))
    , resolver_(boost::asio::make_strand(ioc_))
    , networkValidatedLedgers_(etl_.getNetworkValidatedLedgers())
    , journal_(etl_.getApplication().journal("ReportingETL::ETLSource"))
    , app_(etl_.getApplication())
    , timer_(ioc_)
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
    , ioc_(etl.getApplication().getIOService())
    , ws_(std::make_unique<
          boost::beast::websocket::stream<boost::beast::tcp_stream>>(
          boost::asio::make_strand(ioc_)))
    , resolver_(boost::asio::make_strand(ioc_))
    , networkValidatedLedgers_(etl_.getNetworkValidatedLedgers())
    , journal_(etl_.getApplication().journal("ReportingETL::ETLSource"))
    , app_(etl_.getApplication())
    , timer_(ioc_)
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
ETLSource::reconnect(boost::beast::error_code ec)
{
    connected = false;
    // These are somewhat normal errors. operation_aborted occurs on shutdown,
    // when the timer is cancelled. connection_refused will occur repeatedly
    // if we cannot connect to the transaction processing process
    if (ec != boost::asio::error::operation_aborted &&
        ec != boost::asio::error::connection_refused)
    {
        JLOG(journal_.error()) << __func__ << " : "
                               << "error code = " << ec << " - " << toString();
    }
    else
    {
        JLOG(journal_.warn()) << __func__ << " : "
                              << "error code = " << ec << " - " << toString();
    }

    if (etl_.isStopping())
    {
        JLOG(journal_.debug()) << __func__ << " : " << toString()
                               << " - etl is stopping. aborting reconnect";
        return;
    }

    // exponentially increasing timeouts, with a max of 30 seconds
    size_t waitTime = std::min(pow(2, numFailures), 30.0);
    numFailures++;
    timer_.expires_after(boost::asio::chrono::seconds(waitTime));
    timer_.async_wait([this](auto ec) {
        bool startAgain = (ec != boost::asio::error::operation_aborted);
        JLOG(journal_.trace()) << __func__ << " async_wait : ec = " << ec;
        close(startAgain);
    });
}

void
ETLSource::close(bool startAgain)
{
    timer_.cancel();
    ioc_.post([this, startAgain]() {
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
                            << __func__ << " async_close : "
                            << "error code = " << ec << " - " << toString();
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
    JLOG(journal_.trace()) << __func__ << " : " << toString();

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
    JLOG(journal_.trace()) << __func__ << " : ec = " << ec << " - "
                           << toString();
    if (ec)
    {
        // try again
        reconnect(ec);
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
    JLOG(journal_.trace()) << __func__ << " : ec = " << ec << " - "
                           << toString();
    if (ec)
    {
        // start over
        reconnect(ec);
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
    JLOG(journal_.trace()) << __func__ << " : ec = " << ec << " - "
                           << toString();
    if (ec)
    {
        // start over
        reconnect(ec);
    }
    else
    {
        Json::Value jv;
        jv["command"] = "subscribe";

        jv["streams"] = Json::arrayValue;
        Json::Value ledgerStream("ledger");
        jv["streams"].append(ledgerStream);
        Json::Value txnStream("transactions_proposed");
        jv["streams"].append(txnStream);
        Json::FastWriter fastWriter;

        JLOG(journal_.trace()) << "Sending subscribe stream message";
        // Send the message
        ws_->async_write(
            boost::asio::buffer(fastWriter.write(jv)),
            [this](auto ec, size_t size) { onWrite(ec, size); });
    }
}

void
ETLSource::onWrite(boost::beast::error_code ec, size_t bytesWritten)
{
    JLOG(journal_.trace()) << __func__ << " : ec = " << ec << " - "
                           << toString();
    if (ec)
    {
        // start over
        reconnect(ec);
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
    JLOG(journal_.trace()) << __func__ << " : ec = " << ec << " - "
                           << toString();
    // if error or error reading message, start over
    if (ec)
    {
        reconnect(ec);
    }
    else
    {
        handleMessage();
        boost::beast::flat_buffer buffer;
        swap(readBuffer_, buffer);

        JLOG(journal_.trace())
            << __func__ << " : calling async_read - " << toString();
        ws_->async_read(
            readBuffer_, [this](auto ec, size_t size) { onRead(ec, size); });
    }
}

bool
ETLSource::handleMessage()
{
    JLOG(journal_.trace()) << __func__ << " : " << toString();

    setLastMsgTime();
    connected = true;
    try
    {
        Json::Value response;
        Json::Reader reader;
        if (!reader.parse(
                static_cast<char const*>(readBuffer_.data().data()), response))
        {
            JLOG(journal_.error())
                << __func__ << " : "
                << "Error parsing stream message."
                << " Message = " << readBuffer_.data().data();
            return false;
        }

        uint32_t ledgerIndex = 0;
        // TODO is this index always validated?
        if (response.isMember("result"))
        {
            if (response["result"].isMember(jss::ledger_index))
            {
                ledgerIndex = response["result"][jss::ledger_index].asUInt();
            }
            if (response[jss::result].isMember(jss::validated_ledgers))
            {
                setValidatedRange(
                    response[jss::result][jss::validated_ledgers].asString());
            }
            JLOG(journal_.debug())
                << __func__ << " : "
                << "Received a message on ledger "
                << " subscription stream. Message : "
                << response.toStyledString() << " - " << toString();
        }
        else
        {
            if (response.isMember(jss::transaction))
            {
                if (etl_.getLoadBalancer().shouldPropagateTxnStream(this))
                {
                    etl_.getApplication().getOPs().forwardProposedTransaction(
                        response);
                }
            }
            else
            {
                JLOG(journal_.debug())
                    << __func__ << " : "
                    << "Received a message on ledger "
                    << " subscription stream. Message : "
                    << response.toStyledString() << " - " << toString();
                if (response.isMember(jss::ledger_index))
                {
                    ledgerIndex = response[jss::ledger_index].asUInt();
                }
                if (response.isMember(jss::validated_ledgers))
                {
                    setValidatedRange(
                        response[jss::validated_ledgers].asString());
                }
            }
        }

        if (ledgerIndex != 0)
        {
            JLOG(journal_.trace())
                << __func__ << " : "
                << "Pushing ledger sequence = " << ledgerIndex << " - "
                << toString();
            networkValidatedLedgers_.push(ledgerIndex);
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
    Status
    process(
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
    uint32_t sequence,
    ThreadSafeQueue<std::shared_ptr<SLE>>& writeQueue)
{
    if (!stub_)
        return false;

    grpc::CompletionQueue cq;

    void* tag;

    bool ok = false;

    std::vector<AsyncCallData> calls;
    std::vector<uint256> markers{getMarkers(etl_.getNumMarkers())};

    for (size_t i = 0; i < markers.size(); ++i)
    {
        std::optional<uint256> nextMarker;
        if (i + 1 < markers.size())
            nextMarker = markers[i + 1];
        calls.emplace_back(markers[i], nextMarker, sequence, journal_);
    }

    JLOG(journal_.debug()) << "Starting data download for ledger " << sequence;

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
            auto result = ptr->process(stub_, cq, writeQueue, abort);
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
    JLOG(journal_.info()) << __func__ << " : added etl source - "
                          << sources_.back()->toString();
}

void
ETLLoadBalancer::add(std::string& host, std::string& websocketPort)
{
    std::unique_ptr<ETLSource> ptr =
        std::make_unique<ETLSource>(host, websocketPort, etl_);
    sources_.push_back(std::move(ptr));
    JLOG(journal_.info()) << __func__ << " : added etl source - "
                          << sources_.back()->toString();
}

void
ETLLoadBalancer::loadInitialLedger(
    uint32_t sequence,
    ThreadSafeQueue<std::shared_ptr<SLE>>& writeQueue)
{
    execute(
        [this, &sequence, &writeQueue](auto& source) {
            bool res = source->loadInitialLedger(sequence, writeQueue);
            if (!res)
            {
                JLOG(journal_.error()) << "Failed to download initial ledger. "
                                       << " Sequence = " << sequence
                                       << " source = " << source->toString();
            }
            return res;
        },
        sequence);
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

std::unique_ptr<org::xrpl::rpc::v1::XRPLedgerAPIService::Stub>
ETLLoadBalancer::getForwardingStub(RPC::Context& context)
{
    if (sources_.size() == 0)
        return nullptr;
    srand((unsigned)time(0));
    auto sourceIdx = rand() % sources_.size();
    auto numAttempts = 0;
    while (numAttempts < sources_.size())
    {
        auto stub = sources_[sourceIdx]->getForwardingStub(context);
        if (!stub)
        {
            sourceIdx = (sourceIdx + 1) % sources_.size();
            ++numAttempts;
            continue;
        }
        return stub;
    }
    return nullptr;
}

Json::Value
ETLLoadBalancer::forwardToTx(RPC::JsonContext& context)
{
    Json::Value res;
    if (sources_.size() == 0)
        return res;
    srand((unsigned)time(0));
    auto sourceIdx = rand() % sources_.size();
    auto numAttempts = 0;
    while (numAttempts < sources_.size())
    {
        res = sources_[sourceIdx]->forwardToTx(context);
        if (!res.isMember("forwarded") || res["forwarded"] != true)
        {
            sourceIdx = (sourceIdx + 1) % sources_.size();
            ++numAttempts;
            continue;
        }
        return res;
    }
    RPC::Status err = {rpcFAILED_TO_FORWARD};
    err.inject(res);
    return res;
}

std::unique_ptr<org::xrpl::rpc::v1::XRPLedgerAPIService::Stub>
ETLSource::getForwardingStub(RPC::Context& context)
{
    if (!connected)
        return nullptr;
    try
    {
        return org::xrpl::rpc::v1::XRPLedgerAPIService::NewStub(
            grpc::CreateChannel(
                beast::IP::Endpoint(
                    boost::asio::ip::make_address(ip_), std::stoi(grpcPort_))
                    .to_string(),
                grpc::InsecureChannelCredentials()));
    }
    catch (std::exception const& e)
    {
        JLOG(journal_.error()) << "Failed to create grpc stub";
        return nullptr;
    }
}

Json::Value
ETLSource::forwardToTx(RPC::JsonContext& context)
{
    JLOG(journal_.debug()) << "Attempting to forward request to tx. "
                           << "request = " << context.params.toStyledString();

    Json::Value response;
    if (!connected)
    {
        JLOG(journal_.error())
            << "Attempted to proxy but failed to connect to tx";
        return response;
    }
    namespace beast = boost::beast;          // from <boost/beast.hpp>
    namespace http = beast::http;            // from <boost/beast/http.hpp>
    namespace websocket = beast::websocket;  // from <boost/beast/websocket.hpp>
    namespace net = boost::asio;             // from <boost/asio.hpp>
    using tcp = boost::asio::ip::tcp;        // from <boost/asio/ip/tcp.hpp>
    Json::Value& request = context.params;
    try
    {
        // The io_context is required for all I/O
        net::io_context ioc;

        // These objects perform our I/O
        tcp::resolver resolver{ioc};

        JLOG(journal_.debug()) << "Creating websocket";
        auto ws = std::make_unique<websocket::stream<tcp::socket>>(ioc);

        // Look up the domain name
        auto const results = resolver.resolve(ip_, wsPort_);

        JLOG(journal_.debug()) << "Connecting websocket";
        // Make the connection on the IP address we get from a lookup
        net::connect(ws->next_layer(), results.begin(), results.end());

        // Set a decorator to change the User-Agent of the handshake
        // and to tell rippled to charge the client IP for RPC
        // resources. See "secure_gateway" in
        // https://github.com/ripple/rippled/blob/develop/cfg/rippled-example.cfg
        ws->set_option(
            websocket::stream_base::decorator(
                [&context](websocket::request_type& req) {
                req.set(
                    http::field::user_agent,
                    std::string(BOOST_BEAST_VERSION_STRING) +
                        " websocket-client-coro");
                req.set(
                    http::field::forwarded,
                    "for=" + context.consumer.to_string());
            }));
        JLOG(journal_.debug()) << "client ip: " << context.consumer.to_string();

        JLOG(journal_.debug()) << "Performing websocket handshake";
        // Perform the websocket handshake
        ws->handshake(ip_, "/");

        Json::FastWriter fastWriter;

        JLOG(journal_.debug()) << "Sending request";
        // Send the message
        ws->write(net::buffer(fastWriter.write(request)));

        beast::flat_buffer buffer;
        ws->read(buffer);

        Json::Reader reader;
        if (!reader.parse(
                static_cast<char const*>(buffer.data().data()), response))
        {
            JLOG(journal_.error()) << "Error parsing response";
            response[jss::error] = "Error parsing response from tx";
        }
        JLOG(journal_.debug()) << "Successfully forward request";

        response["forwarded"] = true;
        return response;
    }
    catch (std::exception const& e)
    {
        JLOG(journal_.error()) << "Encountered exception : " << e.what();
        return response;
    }
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
            << __func__ << " : "
            << "Attempting to execute func. ledger sequence = "
            << ledgerSequence << " - source = " << source->toString();
        if (source->hasLedger(ledgerSequence))
        {
            bool res = f(source);
            if (res)
            {
                JLOG(journal_.debug())
                    << __func__ << " : "
                    << "Successfully executed func at source = "
                    << source->toString()
                    << " - ledger sequence = " << ledgerSequence;
                break;
            }
            else
            {
                JLOG(journal_.warn())
                    << __func__ << " : "
                    << "Failed to execute func at source = "
                    << source->toString()
                    << " - ledger sequence = " << ledgerSequence;
            }
        }
        else
        {
            JLOG(journal_.warn())
                << __func__ << " : "
                << "Ledger not present at source = " << source->toString()
                << " - ledger sequence = " << ledgerSequence;
        }
        sourceIdx = (sourceIdx + 1) % sources_.size();
        numAttempts++;
        if (numAttempts % sources_.size() == 0)
        {
            // If another process loaded the ledger into the database, we can
            // abort trying to fetch the ledger from a transaction processing
            // process
            if (etl_.getApplication().getLedgerMaster().getLedgerBySeq(
                    ledgerSequence))
            {
                JLOG(journal_.warn())
                    << __func__ << " : "
                    << "Error executing function. "
                    << " Tried all sources, but ledger was found in db."
                    << " Sequence = " << ledgerSequence;
                break;
            }
            JLOG(journal_.error())
                << __func__ << " : "
                << "Error executing function "
                << " - ledger sequence = " << ledgerSequence
                << " - Tried all sources. Sleeping and trying again";
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
}

void
ETLLoadBalancer::stop()
{
    for (auto& source : sources_)
        source->stop();
}

}  // namespace ripple
