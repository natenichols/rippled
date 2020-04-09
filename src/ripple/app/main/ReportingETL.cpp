
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
            auto const host = "127.0.0.1";
            auto const port = "6007";

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
                std::cout << reader.parse(
                                 static_cast<char const*>(buffer.data().data()),
                                 response)
                          << std::endl;

                uint32_t ledgerIndex = 0;
                if (response.isMember("result"))
                    ledgerIndex =
                        response["result"][jss::ledger_index].asUInt();
                else
                    ledgerIndex = response[jss::ledger_index].asUInt();
                queue_.push(ledgerIndex);
                std::cout << "ledger = " << ledgerIndex << std::endl;

                std::cout << beast::make_printable(buffer.data()) << std::endl;
            }

            queue_.stop();

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

void
ReportingETL::doWork()
{
    worker_ = std::thread([this]() {
        // TODO move these lambdas to functions
        auto loadLedger = [this]() {
            // ledger header with txns and metadata
            org::xrpl::rpc::v1::GetLedgerResponse reply;
            org::xrpl::rpc::v1::GetLedgerRequest request;
            if (false and this->currentIndex_ == 0)
            {
                request.mutable_ledger()->set_shortcut(
                    org::xrpl::rpc::v1::LedgerSpecifier::SHORTCUT_VALIDATED);
            }
            else
            {
                this->currentIndex_ = queue_.pop();
                if (this->currentIndex_ == 0)
                {
                    return std::vector<TxMeta>{};
                }
                request.mutable_ledger()->set_sequence(this->currentIndex_);
            }
            request.set_transactions(true);
            request.set_expand(true);
            std::cout << "calling get ledger" << std::endl;
            std::cout << "ledger index = " << this->currentIndex_ << std::endl;

            bool validated = false;

            int toWait = 1;
            while (not validated)
            {
                grpc::ClientContext grpcContext;
                grpc::Status status =
                    stub_->GetLedger(&grpcContext, request, &reply);
                if (status.ok())
                {
                    validated = reply.validated();
                    if (!validated)
                        std::this_thread::sleep_for(std::chrono::seconds(2));
                    toWait = 1;
                    continue;
                }
                std::cout << "reply = " << reply.DebugString() << std::endl;
                std::cout << "status = " << status.error_code()
                          << " msg = " << status.error_message() << std::endl;
                std::cout << "request = " << request.DebugString() << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(toWait));
                toWait *= 2;
            }

            std::cout << reply.DebugString() << std::endl;

            std::cout << "deserialize header" << std::endl;
            LedgerInfo lgrInfo = deserializeHeader(
                makeSlice(reply.ledger_header()));
            std::cout << "make ledger" << std::endl;
            this->currentIndex_ = lgrInfo.seq;

            if (!this->ledger_)
            {
                this->ledger_ = std::make_shared<Ledger>(
                    lgrInfo, this->app_.config(), this->app_.getNodeFamily());
            }
            else
            {
                this->ledger_ = std::make_shared<Ledger>(
                    *this->ledger_, NetClock::time_point{});
                this->ledger_->setLedgerInfo(lgrInfo);
            }

            this->ledger_->stateMap().clearSynching();
            this->ledger_->txMap().clearSynching();
            std::vector<TxMeta> metas;

            std::cout << "process txns" << std::endl;
            for (auto& txn : reply.transactions_list().transactions())
            {
                std::cout << "process txn" << std::endl;
                auto& raw = txn.transaction_blob();

                SerialIter it{raw.data(), raw.size()};
                STTx sttx{it};

                std::cout << "made sttx" << std::endl;
                auto txSerializer =
                    std::make_shared<Serializer>(sttx.getSerializer());
                std::cout << "made sttx serializer" << std::endl;

                TxMeta txMeta{sttx.getTransactionID(),
                              this->ledger_->info().seq,
                              txn.metadata_blob()};
                metas.push_back(txMeta);

                std::cout << "made txMeta" << std::endl;

                auto metaSerializer = std::make_shared<Serializer>(
                    txMeta.getAsObject().getSerializer());

                std::cout << "made txMeta serializer" << std::endl;

                if (!this->ledger_->txExists(sttx.getTransactionID()))
                    this->ledger_->rawTxInsert(
                        sttx.getTransactionID(), txSerializer, metaSerializer);
            }
            return metas;
        };

        loadLedger();

        // all of the ledger data
        org::xrpl::rpc::v1::GetLedgerDataResponse replyData;
        org::xrpl::rpc::v1::GetLedgerDataRequest requestData;

        grpc::ClientContext grpcContextData;
        requestData.mutable_ledger()->set_sequence(this->currentIndex_);

        std::cout << "starting data" << std::endl;
        grpc::Status status =
            stub_->GetLedgerData(&grpcContextData, requestData, &replyData);

        /*
        std::mutex mtx;
        std::condition_variable cv;
        std::queue<std::pair<std::string, std::string>> data;
        std::atomic_bool done{false};
*/

        while (not stopping_)
        {
            std::cout << "marker = " << strHex(replyData.marker()) << std::endl;

            for (auto& state : replyData.state_objects())
            {
                auto& index = state.index();
                auto& data = state.data();

                auto key = uint256::fromVoid(index.data());

                SerialIter it{data.data(), data.size()};
                std::shared_ptr<SLE> sle = std::make_shared<SLE>(it, key);
                if (!this->ledger_->exists(key))
                    this->ledger_->rawInsert(sle);
            }

            if (replyData.marker().size() == 0)
                break;

            grpc::ClientContext grpcContextData2;
            requestData.set_marker(replyData.marker());
            status = stub_->GetLedgerData(
                &grpcContextData2, requestData, &replyData);
        }
        std::cout << "finished data. storing" << std::endl;

        auto storeLedger = [this]() {
            this->ledger_->setImmutable(this->app_.config());

            std::cout << strHex(
                             this->ledger_->stateMap().getHash().as_uint256())
                      << std::endl;
            std::cout << strHex(this->ledger_->info().accountHash) << std::endl;

            std::cout << strHex(this->ledger_->txMap().getHash().as_uint256())
                      << std::endl;
            std::cout << strHex(this->ledger_->info().txHash) << std::endl;

            this->ledger_->stateMap().flushDirty(
                hotACCOUNT_NODE, this->ledger_->info().seq);

            this->ledger_->txMap().flushDirty(
                hotTRANSACTION_NODE, this->ledger_->info().seq);

            std::cout << strHex(
                             this->ledger_->stateMap().getHash().as_uint256())
                      << std::endl;
            std::cout << strHex(this->ledger_->info().accountHash) << std::endl;

            std::cout << strHex(this->ledger_->txMap().getHash().as_uint256())
                      << std::endl;
            std::cout << strHex(this->ledger_->info().txHash) << std::endl;

            assert(
                this->ledger_->txMap().getHash().as_uint256() ==
                this->ledger_->info().txHash);

            assert(
                this->ledger_->stateMap().getHash().as_uint256() ==
                this->ledger_->info().accountHash);

            this->app_.setOpenLedger(this->ledger_);

            this->app_.getLedgerMaster().storeLedger(this->ledger_);

            this->app_.getLedgerMaster().switchLCL(this->ledger_);
        };

        storeLedger();
        std::cout << "stored initial ledger!" << std::endl;

        // sleep for ten seconds to prevent throttling
        std::this_thread::sleep_for(std::chrono::seconds(10));
        std::cout << "starting continous update" << std::endl;

        while (not stopping_)
        {
            auto metas = loadLedger();
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

            for (auto iter = indices.begin(); iter != indices.end();)
            {
                auto& idx = *iter;
                org::xrpl::rpc::v1::GetLedgerEntryResponse replyEntry;
                org::xrpl::rpc::v1::GetLedgerEntryRequest requestEntry;

                grpc::ClientContext grpcContextEntry;
                requestEntry.mutable_ledger()->set_sequence(
                    this->currentIndex_);
                requestEntry.set_index(idx.data(), idx.size());
                status = stub_->GetLedgerEntry(
                    &grpcContextEntry, requestEntry, &replyEntry);

                std::cout << status.error_message() << " : "
                          << status.error_code() << " : "
                          << replyEntry.DebugString()
                          << " index : " << strHex(idx) << std::endl;

                if (status.error_code() == grpc::StatusCode::NOT_FOUND)
                {
                    if (this->ledger_->exists(idx))
                    {
                        std::cout << "erasing" << std::endl;
                        this->ledger_->rawErase(idx);
                    }
                    else
                    {
                        std::cout << "noop" << std::endl;
                    }
                    ++iter;
                }
                else if (status.ok())
                {
                    auto& objRaw = replyEntry.object_binary();
                    SerialIter it{objRaw.data(), objRaw.size()};

                    std::shared_ptr<SLE> sle = std::make_shared<SLE>(it, idx);
                    if (this->ledger_->exists(idx))
                    {
                        std::cout << "replacing" << std::endl;
                        this->ledger_->rawReplace(sle);
                    }
                    else
                    {
                        std::cout << "inserting" << std::endl;
                        this->ledger_->rawInsert(sle);
                    }
                    ++iter;
                }
                else if (status.error_code() == 8)
                {
                    std::cout << "usage balance. pausing" << std::endl;

                    std::this_thread::sleep_for(std::chrono::seconds(2));
                }
                else
                {
                    std::cout << "unexpected error, trying again" << std::endl;
                }
            }

            this->ledger_->updateSkipList();
            storeLedger();
            std::cout << "Stored ledger " + std::to_string(this->currentIndex_)
                      << std::endl;
        }

        // save the ledger, create next

        // fetch ledger header with txns and metadata
        // store txns
        // process metadata to get list of object ids
        // fetch each object and store
        // repeat
    });
}
}  // namespace ripple
