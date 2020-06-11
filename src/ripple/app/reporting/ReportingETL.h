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

    LedgerIndexQueue indexQueue_;

    std::thread writer_;

    std::thread extracter_;

    std::thread transformer_;

    ThreadSafeQueue<org::xrpl::rpc::v1::GetLedgerResponse> transformQueue_;

    std::thread loader_;

    ThreadSafeQueue<std::pair<std::shared_ptr<Ledger>, std::vector<TxMeta>>>
        loadQueue_;

    ThreadSafeQueue<std::shared_ptr<SLE>> writeQueue_;

    // TODO stopping logic needs to be better
    // There are a variety of loops and mutexs in play
    // Sometimes, the software can't stop
    std::atomic_bool stopping_ = false;

    std::shared_ptr<Ledger> ledger_;

    std::string ip_;

    std::string wsPort_;

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

    void
    loadInitialLedger();

    // returns true if the etl was successful. False could mean etl is
    // shutting down, or there was a write conflict
    bool
    doETL();

    void
    doContinousETL();

    void
    doContinousETLPipelined();

    void
    runETLPipeline();

    void
    monitor();

    // returns true if a ledger was actually fetched
    // this will only be false if the etl mechanism is shutting down
    bool
    fetchLedger(
        org::xrpl::rpc::v1::GetLedgerResponse& out,
        bool getObjects = true);

    void
    updateLedger(
        org::xrpl::rpc::v1::GetLedgerResponse& in,
        std::vector<TxMeta>& out,
        bool updateSkiplist = true);

    void
    flushLedger();

    void
    flushLedger(std::shared_ptr<Ledger>& ledger);

    bool
    writeToPostgres(LedgerInfo const& info, std::vector<TxMeta>& meta);

    // returns true if publish was successful (if ledger is in db)
    bool
    publishLedger(uint32_t ledgerSequence, uint32_t maxAttempts = 10);

    // Publishes the ledger held in ledger_ member variable
    void
    publishLedger();

    void
    outputMetrics();

    void
    startWriter();

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

    LedgerIndexQueue&
    getLedgerIndexQueue()
    {
        return indexQueue_;
    }

    bool
    isStopping()
    {
        return stopping_;
    }

    template <class Func>
    bool
    execute(Func f, uint32_t ledgerSequence);

    std::shared_ptr<Ledger>&
    getLedger()
    {
        return ledger_;
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

        result["queue_size"] = std::to_string(indexQueue_.size());
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
        indexQueue_.stop();
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
