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

#include <ripple/app/reporting/TxProxy.h>
#include <ripple/json/json_reader.h>
#include <ripple/json/json_writer.h>

namespace ripple {

Json::Value
TxProxy::forwardToTx(RPC::JsonContext& context)
{
    JLOG(journal_.debug()) << "Attempting to forward request to tx. "
                           << "request = " << context.params.toStyledString();

    Json::Value response;
    if (!setup_)
    {
        JLOG(journal_.error()) << "Attempted to proxy but TxProxy is not setup";
        response[jss::error] = "Attmpted to proxy but TxProxy is not setup";
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
        ws->set_option(
            websocket::stream_base::decorator([](websocket::request_type& req) {
                req.set(
                    http::field::user_agent,
                    std::string(BOOST_BEAST_VERSION_STRING) +
                        " websocket-client-coro");
            }));

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
        response[jss::error] =
            ("Failed to forward to tx : " + std::string{e.what()});
        response["forwarded"] = true;
        return response;
    }
}

std::unique_ptr<org::xrpl::rpc::v1::XRPLedgerAPIService::Stub>
TxProxy::getForwardingStub(RPC::Context& context)
{
    if (!setup_)
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

// We only forward requests where ledger_index is "current" or "closed"
// otherwise, attempt to handle here
bool
TxProxy::shouldForwardToTx(RPC::JsonContext& context)
{
    if (!setup_)
        return false;

    Json::Value& params = context.params;
    std::string strCommand = params.isMember(jss::command)
        ? params[jss::command].asString()
        : params[jss::method].asString();

    JLOG(context.j.trace()) << "COMMAND:" << strCommand;
    JLOG(context.j.trace()) << "REQUEST:" << params;
    auto handler = RPC::getHandler(context.apiVersion, strCommand);
    if (!handler)
    {
        JLOG(journal_.error())
            << "Error getting handler. command = " << strCommand;
        return false;
    }

    if (handler->condition_ == RPC::NEEDS_CURRENT_LEDGER ||
        handler->condition_ == RPC::NEEDS_CLOSED_LEDGER)
    {
        return true;
    }
    // TODO consider forwarding sequence values greater than the
    // latest sequence we have
    if (params.isMember(jss::ledger_index))
    {
        auto indexValue = params[jss::ledger_index];
        if (!indexValue.isNumeric())
        {
            auto index = indexValue.asString();
            return index == "current" || index == "closed";
        }
    }
    return false;
}

}  // namespace ripple
