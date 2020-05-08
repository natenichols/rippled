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
#include <ripple/app/main/ETLHelpers.h>
#include <ripple/app/main/ETLSource.h>
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

    std::thread subscriber_;

    LedgerIndexQueue indexQueue_;

    std::thread writer_;

    ThreadSafeQueue<std::shared_ptr<SLE>> writeQueue_;

    std::unique_ptr<org::xrpl::rpc::v1::XRPLedgerAPIService::Stub> stub_;

    std::unique_ptr<
        boost::beast::websocket::stream<boost::asio::ip::tcp::socket>>
        ws_;

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

    bool checkRange_ = false;

    uint32_t numLedgers_ = 0;

    void
    loadInitialLedger();

    void
    doETL();

    bool
    fetchLedger(
        org::xrpl::rpc::v1::GetLedgerResponse& out,
        bool getObjects = true);

    void
    updateLedger(
        org::xrpl::rpc::v1::GetLedgerResponse& in,
        std::vector<TxMeta>& out);

    void
    flushLedger();

    void
    storeLedger();

    void
    writeToPostgres(LedgerInfo const& info, std::vector<TxMeta>& meta);

    void
    publishLedger(uint32_t ledgerSequence);

    // Publishes the ledger held in ledger_ member variable
    void
    publishLedger();

    void
    truncateDBs();

    void
    outputMetrics();

    void
    startWriter();

    void
    joinWriter();

    bool
    consistencyCheck();

    void
    initNumLedgers();

    Metrics totalMetrics;
    Metrics roundMetrics;

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

    void
    run()
    {
        JLOG(journal_.info()) << "Starting reporting etl";
        assert(app_.config().reporting());
        assert(app_.config().standalone());
        assert(!app_.config().reportingReadOnly());

        stopping_ = false;

        if (app_.config().START_UP == Config::StartUpType::LOAD)
        {
            // This ledger will not actually be mutated, but every ledger
            // after it will therefore ledger_ is not const
            ledger_ = std::const_pointer_cast<Ledger>(
                app_.getLedgerMaster().getValidatedLedger());
            if (ledger_)
            {
                JLOG(journal_.info()) << "Loaded ledger successfully. "
                                      << "seq = " << ledger_->info().seq;
                indexQueue_.push(ledger_->info().seq + 1);
            }
            else
            {
                JLOG(journal_.warn()) << "Failed to load ledger. Will download";
            }
        }
        else if (app_.config().usePostgresTx())
        {
            // if we don't load the ledger from disk, the dbs need to be cleared
            // out, since the db will not allow any gaps
            truncateDBs();
        }

        // if we loaded the ledger from disk, don't use start_index
        if (!ledger_)
        {
            assert(app_.config().exists("reporting"));
            Section section = app_.config().section("reporting");
            std::pair<std::string, bool> startIndexPair =
                section.find("start_index");

            if (startIndexPair.second)
            {
                indexQueue_.push(std::stoi(startIndexPair.first));
            }
        }
        loadBalancer_.start();
        doWork();
    }

    void
    runReadOnly()
    {
        assert(app_.config().reportingReadOnly());
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
