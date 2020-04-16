
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
#include <ripple/core/JobQueue.h>
#include <ripple/net/InfoSub.h>
#include <ripple/protocol/ErrorCodes.h>
#include <ripple/resource/Charge.h>
#include <ripple/rpc/Context.h>
#include <ripple/rpc/GRPCHandlers.h>
#include <ripple/rpc/Role.h>
#include <ripple/rpc/impl/Handler.h>
#include <ripple/rpc/impl/RPCHelpers.h>
#include <ripple/rpc/impl/Tuning.h>

#include "org/xrpl/rpc/v1/xrp_ledger.grpc.pb.h"
#include <grpcpp/grpcpp.h>

#include <condition_variable>
#include <mutex>
#include <queue>

#include <chrono>
namespace ripple {

class ReportingETL
{
    //TODO some logging in the queue
    class LedgerIndexQueue
    {
        std::queue<uint32_t> queue_;

        std::mutex mtx_;

        std::condition_variable cv_;

        std::atomic_bool stopping_ = false;

    public:
        void
        push(uint32_t idx)
        {
            std::unique_lock<std::mutex> lck(mtx_);
            if (queue_.size() > 0)
            {
                auto last = queue_.back();
                if (idx <= last)
                {
                    std::cout << "push old index = " << idx << std::endl;
                    return;
                }
                assert(idx > last);
                if (idx > last + 1)
                {
                    for (size_t i = last + 1; i < idx; ++i)
                    {
                        queue_.push(i);
                    }
                }
            }
            queue_.push(idx);
            cv_.notify_all();
        }

        uint32_t
        pop()
        {
            std::unique_lock<std::mutex> lck(mtx_);
            cv_.wait(
                lck, [this]() { return this->queue_.size() > 0 || stopping_; });
            if (stopping_)
                return 0;
            uint32_t next = queue_.front();
            queue_.pop();
            return next;
        }

        void
        stop()
        {
            std::unique_lock<std::mutex> lck(mtx_);
            stopping_ = true;
            cv_.notify_all();
        }
    };
    public:
template <class T>
struct ThreadSafeQueue
{

    std::queue<T> queue_;

    std::mutex m_;
    std::condition_variable cv_;

    void push(T const& elt)
    {
        std::unique_lock<std::mutex> lck(m_);
        queue_.push(elt);
        cv_.notify_all();
    }

    T pop()
    {
        std::unique_lock<std::mutex> lck(m_);
        //TODO: is this able to be aborted?
        cv_.wait(lck,[this](){ return !queue_.empty();});
        auto ret = queue_.front();
        queue_.pop();
        return ret;
    }
};

private:
    Application& app_;

    std::thread worker_;

    std::thread subscriber_;

    LedgerIndexQueue indexQueue_;

    std::thread writer_;

    ThreadSafeQueue<std::shared_ptr<SLE>> writeQueue_;

    std::unique_ptr<org::xrpl::rpc::v1::XRPLedgerAPIService::Stub> stub_;

    //TODO stopping logic needs to be better
    //There are a variety of loops and mutexs in play
    //Sometimes, the software can't stop
    std::atomic_bool stopping_ = false;

    std::shared_ptr<Ledger> ledger_;

    std::string ip_;

    std::string wsPort_;

    beast::Journal journal_;

    size_t flushInterval_ = 0;

    size_t numMarkers_ = 2;

    void
    loadInitialLedger();

    void
    doETL();

    void
    fetchLedger(
        org::xrpl::rpc::v1::GetLedgerResponse& out,
        bool getObjects = true);

    void
    updateLedger(org::xrpl::rpc::v1::GetLedgerResponse& in);

    void
    flushLedger();

    void
    storeLedger();

    void
    outputMetrics();

    void
    startWriter();

    void
    joinWriter();

    struct Metrics
    {
        size_t txnCount = 0;

        size_t objectCount = 0;

        double flushTime = 0;

        double updateTime = 0;

        double storeTime = 0;

        void
        printMetrics(beast::Journal& j)
        {
            auto totalTime = updateTime + flushTime + storeTime;
            auto kvTime = updateTime + flushTime;
            JLOG(j.info()) << " Metrics: "
                           << " txnCount = " << txnCount
                           << " objectCount = " << objectCount
                           << " updateTime = " << updateTime
                           << " flushTime = " << flushTime
                           << " storeTime = " << storeTime
                           << " update tps = " << txnCount / updateTime
                           << " flush tps = " << txnCount / flushTime
                           << " store tps = " << txnCount / storeTime
                           << " update ops = " << objectCount / updateTime
                           << " flush ops = " << objectCount / flushTime
                           << " store ops = " << objectCount / storeTime
                           << " total tps = " << txnCount / totalTime
                           << " total ops = " << objectCount / totalTime
                           << " key-value tps = " << txnCount / kvTime
                           << " key-value ops = " << objectCount / kvTime
                           << " (All times in seconds)";
        }

        void
        addMetrics(Metrics& round)
        {
            txnCount += round.txnCount;
            objectCount += round.objectCount;
            flushTime += round.flushTime;
            updateTime += round.updateTime;
            storeTime += round.storeTime;
        }
    };

    Metrics totalMetrics;
    Metrics roundMetrics;

public:
    ReportingETL(Application& app)
        : app_(app), journal_(app.journal("ReportingETL"))
    {
        // if present, get endpoint from config
        if (app_.config().exists("reporting"))
        {
            Section section = app_.config().section("reporting");

            std::pair<std::string, bool> ipPair = section.find("source_ip");
            if (!ipPair.second)
                return;

            std::pair<std::string, bool> portPair =
                section.find("source_grpc_port");
            if (!portPair.second)
                return;

            std::pair<std::string, bool> wsPortPair =
                section.find("source_ws_port");
            if (!wsPortPair.second)
                return;

            std::pair<std::string, bool> startIndexPair =
                section.find("start_index");

            if (startIndexPair.second)
            {
                indexQueue_.push(std::stoi(startIndexPair.first));
            }

            std::pair<std::string, bool> flushInterval =
                section.find("flush_interval");
            if (flushInterval.second)
            {
                flushInterval_ = std::stoi(flushInterval.first);
            }

            std::pair<std::string, bool> p = section.find("num_markers");
            if(p.second)
                numMarkers_ = std::stoi(p.first);

            try
            {
                stub_ = org::xrpl::rpc::v1::XRPLedgerAPIService::NewStub(
                    grpc::CreateChannel(
                        beast::IP::Endpoint(
                            boost::asio::ip::make_address(ipPair.first),
                            std::stoi(portPair.first))
                            .to_string(),
                        grpc::InsecureChannelCredentials()));
                std::cout << "made stub" << std::endl;
                ip_ = ipPair.first;
                wsPort_ = wsPortPair.first;
            }
            catch (std::exception const& e)
            {
                std::cout << "Exception while creating stub = " << e.what()
                          << std::endl;
            }
        }
    }

    ~ReportingETL()
    {
        JLOG(journal_.debug()) << "Stopping Reporting ETL";
        stopping_ = true;
        if (subscriber_.joinable())
            subscriber_.join();
        
        JLOG(journal_.debug()) << "Joined subscriber thread";
        if (worker_.joinable())
            worker_.join();

        JLOG(journal_.debug()) << "Joined worker thread";
    }

    void
    run()
    {
        std::cout << "starting reporting etl" << std::endl;
        assert(app_.config().reporting());
        assert(app_.config().standalone());
        if (!stub_)
        {
            std::cout << "stub not created. aborting reporting etl"
                      << std::endl;
            return;
        }
        stopping_ = false;
        doSubscribe();
        doWork();
    }

private:
    void
    doWork();

    void
    doSubscribe();
};

}  // namespace ripple
#endif
