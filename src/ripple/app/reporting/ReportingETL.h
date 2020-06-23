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

#ifndef RIPPLE_CORE_REPORTINGETL_H_INCLUDED
#define RIPPLE_CORE_REPORTINGETL_H_INCLUDED

#include <ripple/app/main/Application.h>
#include <ripple/app/reporting/ETLHelpers.h>
#include <ripple/app/reporting/ETLSource.h>
#include <ripple/core/JobQueue.h>
#include <ripple/core/Stoppable.h>
#include <ripple/net/InfoSub.h>
#include <ripple/protocol/ErrorCodes.h>
#include <ripple/resource/Charge.h>
#include <ripple/rpc/Context.h>
#include <ripple/rpc/GRPCHandlers.h>
#include <ripple/rpc/Role.h>
#include <ripple/rpc/impl/Handler.h>
#include <ripple/rpc/impl/RPCHelpers.h>
#include <ripple/rpc/impl/Tuning.h>

#include <boost/algorithm/string.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/string.hpp>
#include <boost/beast/websocket.hpp>

#include "org/xrpl/rpc/v1/xrp_ledger.grpc.pb.h"
#include <grpcpp/grpcpp.h>

#include <condition_variable>
#include <mutex>
#include <queue>

#include <chrono>
namespace ripple {

class ReportingETL : Stoppable
{
private:
    Application& app_;

    beast::Journal journal_;

    std::thread worker_;

    boost::asio::io_context ioc_;

    ETLLoadBalancer loadBalancer_;

    NetworkValidatedLedgers networkValidatedLedgers_;

    std::thread writer_;

    ThreadSafeQueue<std::shared_ptr<SLE>> writeQueue_;

    std::thread extracter_;

    std::thread transformer_;

    ThreadSafeQueue<std::optional<org::xrpl::rpc::v1::GetLedgerResponse>>
        transformQueue_;

    std::thread loader_;

    ThreadSafeQueue<
        std::optional<std::pair<std::shared_ptr<Ledger>, std::vector<TxMeta>>>>
        loadQueue_;


    // TODO stopping logic needs to be better
    // There are a variety of loops and mutexs in play
    // Sometimes, the software can't stop
    std::atomic_bool stopping_ = false;

    size_t flushInterval_ = 0;

    size_t numMarkers_ = 2;

    bool checkConsistency_ = false;

    bool readOnly_ = false;

    bool writing_ = false;

    std::chrono::time_point<std::chrono::system_clock> lastPublish_;
    std::mutex publishTimeMtx_;

    std::chrono::time_point<std::chrono::system_clock>
    getLastPublish()
    {
        std::unique_lock<std::mutex> lck(publishTimeMtx_);
        return lastPublish_;
    }

    void
    setLastPublish()
    {
        std::unique_lock<std::mutex> lck(publishTimeMtx_);
        lastPublish_ = std::chrono::system_clock::now();
    }

    std::shared_ptr<Ledger>
    loadInitialLedger(uint32_t startingSequence);

    std::optional<uint32_t>
    doContinousETLPipelined(uint32_t startSequence);

    std::optional<uint32_t>
    runETLPipeline(std::shared_ptr<Ledger>& startLedger);

    void
    monitor();

    // returns true if a ledger was actually fetched
    // this will be false if the etl mechanism is shutting down
    // or the ledger was found in the database
    bool
    fetchLedger(
        uint32_t sequence,
        org::xrpl::rpc::v1::GetLedgerResponse& out,
        bool getObjects = true);

    std::shared_ptr<Ledger>
    updateLedger(
        org::xrpl::rpc::v1::GetLedgerResponse& in,
        std::shared_ptr<Ledger>& parent,
        std::vector<TxMeta>& out,
        bool updateSkiplist = true);

    void
    flushLedger(std::shared_ptr<Ledger>& ledger);

    bool
    writeToPostgres(LedgerInfo const& info, std::vector<TxMeta>& meta);

    // returns true if publish was successful (if ledger is in db)
    bool
    publishLedger(uint32_t ledgerSequence, uint32_t maxAttempts = 10);

    // Publishes the passed in ledger
    void
    publishLedger(std::shared_ptr<Ledger>& ledger);

    void
    outputMetrics(std::shared_ptr<Ledger>& ledger);

    void
    startWriter(std::shared_ptr<Ledger>& ledger);

    void
    joinWriter();

    Metrics totalMetrics;
    Metrics roundMetrics;
    Metrics lastRoundMetrics;

public:
    ReportingETL(Application& app, Stoppable& parent);

    ~ReportingETL()
    {
    }

    NetworkValidatedLedgers&
    getNetworkValidatedLedgers()
    {
        return networkValidatedLedgers_;
    }

    bool
    isStopping()
    {
        return stopping_;
    }

    uint32_t
    getNumMarkers()
    {
        return numMarkers_;
    }

    ThreadSafeQueue<std::shared_ptr<SLE>>&
    getWriteQueue()
    {
        return writeQueue_;
    }

    Application&
    getApplication()
    {
        return app_;
    }

    boost::asio::io_context&
    getIOContext()
    {
        return ioc_;
    }

    beast::Journal&
    getJournal()
    {
        return journal_;
    }

    Json::Value
    getInfo()
    {
        Json::Value result(Json::objectValue);

        result["most_recent_validated"] =
            std::to_string(networkValidatedLedgers_.getMostRecent());
        result["etl_sources"] = loadBalancer_.toJson();
        result["is_writer"] = writing_;
        auto last = getLastPublish();
        if (last.time_since_epoch().count() != 0)
            result["last_publish_time"] = to_string(
                date::floor<std::chrono::microseconds>(getLastPublish()));
        return result;
    }

    void
    setup();

    void
    run()
    {
        JLOG(journal_.info()) << "Starting reporting etl";
        assert(app_.config().reporting());
        assert(app_.config().standalone());
        assert(app_.config().reportingReadOnly() == readOnly_);

        stopping_ = false;

        setup();

        loadBalancer_.start();
        doWork();
    }

    void
    onStop() override
    {
        JLOG(journal_.info()) << "onStop called";
        JLOG(journal_.debug()) << "Stopping Reporting ETL";
        stopping_ = true;
        networkValidatedLedgers_.stop();
        loadBalancer_.stop();

        JLOG(journal_.debug()) << "Stopped loadBalancer";
        if (worker_.joinable())
            worker_.join();

        JLOG(journal_.debug()) << "Joined worker thread";
        stopped();
    }

private:
    void
    doWork();
};

}  // namespace ripple
#endif
