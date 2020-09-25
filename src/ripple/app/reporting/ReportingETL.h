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
#include <ripple/app/reporting/CassandraBackend.h>
#include <ripple/app/reporting/FlatLedger.h>
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

struct AccountTransactionsData;

class ReportingETL : public Stoppable
{
private:
    Application& app_;

    beast::Journal journal_;

    std::thread worker_;

    boost::asio::io_context::strand publishStrand_;

    NodeStore::CassandraBackend cassandra_;

    ETLLoadBalancer loadBalancer_;

    NetworkValidatedLedgers networkValidatedLedgers_;

    // TODO stopping logic needs to be better
    // There are a variety of loops and mutexs in play
    // Sometimes, the software can't stop
    std::atomic_bool stopping_ = false;

    size_t flushInterval_ = 0;

    size_t numMarkers_ = 2;

    bool checkConsistency_ = false;

    bool readOnly_ = false;

    bool writing_ = false;

    std::optional<uint32_t> startSequence_;

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

    std::shared_ptr<FlatLedger>
    loadInitialLedger(uint32_t startingSequence);

    std::optional<uint32_t>
    runETLPipeline(uint32_t startSequence);

    void
    monitor();

    void
    monitorReadOnly();

    // @return ledger header and transaction+metadata blobs
    std::optional<org::xrpl::rpc::v1::GetLedgerResponse>
    fetchLedgerData(uint32_t sequence);

    // @return ledger header, transaction+metadata blobs, and all ledger
    // objects created, modified or deleted between this ledger and the parent
    std::optional<org::xrpl::rpc::v1::GetLedgerResponse>
    fetchLedgerDataAndDiff(uint32_t sequence);

    std::vector<AccountTransactionsData>
    insertTransactions(
        std::shared_ptr<FlatLedger>& ledger,
        org::xrpl::rpc::v1::GetLedgerResponse& data);

    std::pair<std::shared_ptr<FlatLedger>, std::vector<AccountTransactionsData>>
    buildNextLedger(
        std::shared_ptr<FlatLedger>& parent,
        org::xrpl::rpc::v1::GetLedgerResponse& rawData);

    void
    flushLedger(std::shared_ptr<FlatLedger>& ledger);

    // returns true if publish was successful (if ledger is in db)
    bool
    publishLedger(uint32_t ledgerSequence, uint32_t maxAttempts = 10);

    // Publishes the passed in ledger
    void
    publishLedger(std::shared_ptr<FlatLedger>& ledger);

    void
    outputMetrics(std::shared_ptr<Ledger>& ledger);

    void
    consumeLedgerData(
        std::shared_ptr<FlatLedger>& ledger,
        ThreadSafeQueue<std::shared_ptr<SLE>>& writeQueue);

    void
    joinWriter();

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

    NodeStore::CassandraBackend&
    getCassandra()
    {
        return cassandra_;
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

    Application&
    getApplication()
    {
        return app_;
    }

    beast::Journal&
    getJournal()
    {
        return journal_;
    }

    ETLLoadBalancer&
    getLoadBalancer()
    {
        return loadBalancer_;
    }

    Json::Value
    getInfo()
    {
        Json::Value result(Json::objectValue);

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
    sweep();

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

    ETLLoadBalancer&
    getETLLoadBalancer()
    {
        return loadBalancer_;
    }

private:
    void
    doWork();

};

}  // namespace ripple
#endif
