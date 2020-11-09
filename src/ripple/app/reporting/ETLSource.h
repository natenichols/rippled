//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2012, 2013 Ripple Labs Inc.

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

#ifndef RIPPLE_CORE_ETLSOURCE_H_INCLUDED
#define RIPPLE_CORE_ETLSOURCE_H_INCLUDED
#include <ripple/app/main/Application.h>
#include <ripple/app/reporting/ETLHelpers.h>
#include <ripple/protocol/STLedgerEntry.h>
#include <ripple/rpc/Context.h>

#include <boost/algorithm/string.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/string.hpp>
#include <boost/beast/websocket.hpp>

#include "org/xrpl/rpc/v1/xrp_ledger.grpc.pb.h"
#include <grpcpp/grpcpp.h>

namespace ripple {

class ReportingETL;

struct ETLSource
{
    std::string ip_;

    std::string wsPort_;

    std::string grpcPort_;

    ReportingETL& etl_;

    boost::asio::io_context& ioc_;

    std::unique_ptr<org::xrpl::rpc::v1::XRPLedgerAPIService::Stub> stub_;

    std::unique_ptr<boost::beast::websocket::stream<boost::beast::tcp_stream>>
        ws_;
    boost::asio::ip::tcp::resolver resolver_;

    boost::beast::flat_buffer readBuffer_;

    // TODO: make this not a string?
    std::vector<std::pair<uint32_t, uint32_t>> validatedLedgers;

    std::string validatedLedgersRaw;

    NetworkValidatedLedgers& networkValidatedLedgers_;

    beast::Journal journal_;

    Application& app_;

    std::mutex mtx_;

    size_t numFailures = 0;

    std::atomic_bool closing = false;

    std::atomic_bool connected = false;

    std::atomic_bool forwardingStream = false;

    std::chrono::time_point<std::chrono::system_clock> lastMsgTime;
    std::mutex lastMsgTimeMtx_;

    std::chrono::time_point<std::chrono::system_clock>
    getLastMsgTime()
    {
        std::unique_lock<std::mutex> lck(lastMsgTimeMtx_);
        return lastMsgTime;
    }

    void
    setLastMsgTime()
    {
        std::unique_lock<std::mutex> lck(lastMsgTimeMtx_);
        lastMsgTime = std::chrono::system_clock::now();
    }

    boost::asio::steady_timer timer_;

    // Create ETL source without grpc endpoint
    // Fetch ledger and load initial ledger will fail for this source
    // Primarly used in read-only mode, to monitor when ledgers are validated
    ETLSource(
        std::string ip,
        std::string wsPort,
        ReportingETL& etl);

    ETLSource(
        std::string ip,
        std::string wsPort,
        std::string grpcPort,
        ReportingETL& etl);

    bool
    hasLedger(uint32_t sequence)
    {
        std::lock_guard<std::mutex> lck(mtx_);
        for (auto& pair : validatedLedgers)
        {
            if (sequence >= pair.first && sequence <= pair.second)
            {
                return true;
            }
            else if (sequence < pair.first)
            {
                // validatedLedgers is a sorted list of disjoint ranges
                // if the sequence comes before this range, the sequence will
                // come before all subsequent ranges
                return false;
            }
        }
        return false;
    }

    void
    setValidatedRange(std::string const& range)
    {
        std::vector<std::pair<uint32_t, uint32_t>> pairs;
        std::vector<std::string> ranges;
        boost::split(ranges, range, boost::is_any_of(","));
        for (auto& pair : ranges)
        {
            std::vector<std::string> minAndMax;

            boost::split(minAndMax, pair, boost::is_any_of("-"));

            if (minAndMax.size() == 1)
            {
                uint32_t sequence = std::stoll(minAndMax[0]);
                pairs.push_back(std::make_pair(sequence, sequence));
            }
            else
            {
                assert(minAndMax.size() == 2);
                uint32_t min = std::stoll(minAndMax[0]);
                uint32_t max = std::stoll(minAndMax[1]);
                pairs.push_back(std::make_pair(min, max));
            }
        }
        std::sort(pairs.begin(), pairs.end(), [](auto left, auto right) {
            return left.first < right.first;
        });

        std::unique_lock<std::mutex> lck(mtx_);
        validatedLedgers = std::move(pairs);
        validatedLedgersRaw = range;
    }

    std::string
    getValidatedRange()
    {
        std::lock_guard<std::mutex> lck(mtx_);

        return validatedLedgersRaw;
    }

    void
    stop()
    {
        JLOG(journal_.debug()) << __func__ << " : "
                               << "Closing websocket";

        assert(ws_);
        close(false);

    }

    grpc::Status
    fetchLedger(
        org::xrpl::rpc::v1::GetLedgerResponse& out,
        uint32_t ledgerSequence,
        bool getObjects = true);

    std::string
    toString()
    {
        return "{ validated_ledger : " + getValidatedRange() +
            " , ip : " + ip_ + " , web socket port : " + wsPort_ +
            ", grpc port : " + grpcPort_ + " }";
    }

    Json::Value
    toJson()
    {
        Json::Value result(Json::objectValue);
        result["connected"] = connected.load();
        result["validated_ledgers_range"] = getValidatedRange();
        result["ip"] = ip_;
        result["websocket_port"] = wsPort_;
        result["grpc_port"] = grpcPort_;
        auto last = getLastMsgTime();
        if (last.time_since_epoch().count() != 0)
            result["last_message_arrival_time"] =
                to_string(date::floor<std::chrono::microseconds>(last));
        return result;
    }

    bool
    loadInitialLedger(
        uint32_t ledgerSequence,
        ThreadSafeQueue<std::shared_ptr<SLE>>& writeQueue);

    void
    start();

    void
    reconnect(boost::beast::error_code ec);

    void
    onResolve(
        boost::beast::error_code ec,
        boost::asio::ip::tcp::resolver::results_type results);

    void
    onConnect(
        boost::beast::error_code ec,
        boost::asio::ip::tcp::resolver::results_type::endpoint_type endpoint);

    void
    onHandshake(boost::beast::error_code ec);

    void
    onWrite(boost::beast::error_code ec, size_t size);

    void
    onRead(boost::beast::error_code ec, size_t size);

    bool
    handleMessage();

    void
    close(bool startAgain);

    std::unique_ptr<org::xrpl::rpc::v1::XRPLedgerAPIService::Stub>
    getForwardingStub(RPC::Context& context);

    Json::Value
    forwardToTx(RPC::JsonContext& context);
};

// This class is used to manage connections to transaction processing processes
// This class spawns a listener for each etl source, which listens to messages
// on the ledgers stream (to keep track of which ledgers have been validated by
// the network, and the range of ledgers each etl source has). This class also
// allows requests for ledger data to be load balanced across all possible etl
// sources.
class ETLLoadBalancer
{
private:
    ReportingETL& etl_;

    beast::Journal journal_;

    std::vector<std::unique_ptr<ETLSource>> sources_;


public:
    ETLLoadBalancer(ReportingETL& etl);

    void
    add(std::string& host, std::string& websocketPort, std::string& grpcPort);

    void
    add(std::string& host, std::string& websocketPort);

    void
    loadInitialLedger(
        uint32_t sequence,
        ThreadSafeQueue<std::shared_ptr<SLE>>& writeQueue);

    bool
    fetchLedger(
        org::xrpl::rpc::v1::GetLedgerResponse& out,
        uint32_t ledgerSequence,
        bool getObjects);

    void
    start();

    void
    stop();

    bool
    shouldPropagateTxnStream(ETLSource* in)
    {
        for (auto& src : sources_)
        {
            assert(src);
            if (src->connected.load())
            {
                if (src.get() == in)
                    return true;
                else
                    return false;
            }
        }
        assert(false);
        return false;
    }

    Json::Value
    toJson()
    {
        Json::Value ret(Json::arrayValue);
        for (auto& src : sources_)
        {
            ret.append(src->toJson());
        }
        return ret;
    }

    std::unique_ptr<org::xrpl::rpc::v1::XRPLedgerAPIService::Stub>
    getForwardingStub(RPC::Context& context);

    Json::Value
    forwardToTx(RPC::JsonContext& context);

private:
    template <class Func>
    bool
    execute(Func f, uint32_t ledgerSequence);
};

}  // namespace ripple
#endif
