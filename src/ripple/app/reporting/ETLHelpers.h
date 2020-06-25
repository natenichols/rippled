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

#ifndef RIPPLE_CORE_ETLHELPERS_H_INCLUDED
#define RIPPLE_CORE_ETLHELPERS_H_INCLUDED
#include <ripple/app/main/Application.h>
#include <ripple/ledger/ReadView.h>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <sstream>

namespace ripple {

// This datastructure is used to keep track of the sequence of the most recent
// ledger validated by the network. There are two methods that will wait until
// certain conditions are met. This datastructure is able to be "stopped". When
// the datastructure is stopped, any threads currently waiting are unblocked.
// Any later calls to methods of this datastructure will not wait. Once the
// datastructure is stopped, the datastructure remains stopped for the rest of
// it's lifetime.
class NetworkValidatedLedgers
{
    std::optional<uint32_t> max_;

    std::mutex mtx_;

    std::condition_variable cv_;

    bool stopping_ = false;

public:
    void
    push(uint32_t idx)
    {
        std::unique_lock<std::mutex> lck(mtx_);
        if (!max_ || idx > *max_)
            max_ = idx;
        cv_.notify_all();
    }

    // @return sequence of most recently validated ledger. will wait until a
    // sequence is available. empty optional indicates the datastructure has
    // been stopped
    std::optional<uint32_t>
    getMostRecent()
    {
        std::unique_lock<std::mutex> lck(mtx_);
        cv_.wait(lck, [this]() { return max_ || stopping_; });
        return max_;
    }

    // Waits for the sequence to be validated by the network
    // @param sequence to wait for
    // @returns true if sequence was validated, false otherwise
    // a return value of false means the datastructure has been stopped
    bool
    waitUntilValidatedByNetwork(uint32_t sequence)
    {
        std::unique_lock<std::mutex> lck(mtx_);
        cv_.wait(lck, [sequence, this]() {
            return (max_ && sequence <= *max_) || stopping_;
        });
        return !stopping_;
    }

    void
    stop()
    {
        std::unique_lock<std::mutex> lck(mtx_);
        stopping_ = true;
        cv_.notify_all();
    }
};

template <class T>
struct ThreadSafeQueue
{
    std::queue<T> queue_;

    std::mutex m_;
    std::condition_variable cv_;
    std::optional<uint32_t> maxSize_;

    ThreadSafeQueue(uint32_t maxSize) : maxSize_(maxSize)
    {
    }

    ThreadSafeQueue()
    {
    }

    // @param element to push onto queue
    // if maxSize is set, this method will block until free space is available
    void
    push(T const& elt)
    {
        std::unique_lock<std::mutex> lck(m_);
        // if queue has a max size, wait until not full
        if (maxSize_)
            cv_.wait(lck, [this]() { return queue_.size() <= *maxSize_; });
        queue_.push(elt);
        cv_.notify_all();
    }

    // @returns element popped from queue
    T
    pop()
    {
        std::unique_lock<std::mutex> lck(m_);
        // TODO: is this able to be aborted?
        cv_.wait(lck, [this]() { return !queue_.empty(); });
        auto ret = queue_.front();
        queue_.pop();
        // if queue has a max size, unblock any possible pushers
        if (maxSize_)
            cv_.notify_all();
        return ret;
    }
};

inline std::string
toString(LedgerInfo const& info)
{
    std::stringstream ss;
    ss << "LedgerInfo { Sequence : " << info.seq
       << " Hash : " << strHex(info.hash) << " TxHash : " << strHex(info.txHash)
       << " AccountHash : " << strHex(info.accountHash)
       << " ParentHash : " << strHex(info.parentHash) << " }";
    return ss.str();
}

struct Metrics
{
    size_t txnCount = 0;

    size_t objectCount = 0;

    double flushTime = 0;

    double updateTime = 0;

    double postgresTime = 0;

    void
    printMetrics(beast::Journal& j, LedgerInfo const& info)
    {
        auto totalTime = updateTime + flushTime + postgresTime;
        auto kvTime = updateTime + flushTime;
        auto dbTime = flushTime + postgresTime;
        JLOG(j.info()) << toString(info) << " Metrics: "
                       << " txnCount = " << txnCount
                       << " objectCount = " << objectCount
                       << " updateTime = " << updateTime
                       << " flushTime = " << flushTime
                       << " postgresTime = " << postgresTime
                       << " dbTime = " << dbTime
                       << " update tps = " << txnCount / updateTime
                       << " flush tps = " << txnCount / flushTime
                       << " postgres tps = " << txnCount / postgresTime
                       << " update ops = " << objectCount / updateTime
                       << " flush ops = " << objectCount / flushTime
                       << " postgres ops = " << objectCount / postgresTime
                       << " total tps = " << txnCount / totalTime
                       << " total ops = " << objectCount / totalTime
                       << " key-value tps = " << txnCount / kvTime
                       << " key-value ops = " << objectCount / kvTime
                       << " db tps = " << txnCount / dbTime
                       << " db ops = " << objectCount / dbTime
                       << " (All times in seconds)";
    }

    void
    printMetrics(beast::Journal& j)
    {
        auto totalTime = updateTime + flushTime + postgresTime;
        auto kvTime = updateTime + flushTime;
        auto dbTime = flushTime + postgresTime;
        JLOG(j.info()) << " Metrics: "
                       << " txnCount = " << txnCount
                       << " objectCount = " << objectCount
                       << " updateTime = " << updateTime
                       << " flushTime = " << flushTime
                       << " postgresTime = " << postgresTime
                       << " dbTime = " << dbTime
                       << " update tps = " << txnCount / updateTime
                       << " flush tps = " << txnCount / flushTime
                       << " postgres tps = " << txnCount / postgresTime
                       << " update ops = " << objectCount / updateTime
                       << " flush ops = " << objectCount / flushTime
                       << " postgres ops = " << objectCount / postgresTime
                       << " total tps = " << txnCount / totalTime
                       << " total ops = " << objectCount / totalTime
                       << " key-value tps = " << txnCount / kvTime
                       << " key-value ops = " << objectCount / kvTime
                       << " db tps = " << txnCount / dbTime
                       << " db ops = " << objectCount / dbTime
                       << " (All times in seconds)";
    }

    Json::Value
    toJson()
    {
        Json::Value res(Json::objectValue);
        auto totalTime = updateTime + flushTime + postgresTime;
        auto dbTime = flushTime + postgresTime;
        res["total_time"] = totalTime;
        res["kv_flush_time"] = flushTime;
        res["total_db_time"] = dbTime;
        res["update_time"] = updateTime;
        res["total_tps"] = txnCount / totalTime;
        res["kv_flush_tps"] = txnCount / flushTime;
        res["total_db_tps"] = txnCount / dbTime;
        res["update_tps"] = txnCount / updateTime;
        return res;
    }

    void
    addMetrics(Metrics& round)
    {
        txnCount += round.txnCount;
        objectCount += round.objectCount;
        flushTime += round.flushTime;
        updateTime += round.updateTime;
        postgresTime += round.postgresTime;
    }
};

inline std::vector<uint256>
getMarkers(size_t numMarkers)
{
    assert(numMarkers <= 256);

    unsigned char incr = 256 / numMarkers;

    std::vector<uint256> markers;
    uint256 base{0};
    for (size_t i = 0; i < numMarkers; ++i)
    {
        markers.push_back(base);
        base.data()[0] += incr;
    }
    return markers;
}

}  // namespace ripple
#endif
