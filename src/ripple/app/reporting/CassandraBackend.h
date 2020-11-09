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

#ifndef RIPPLE_CORE_CASSANDRABACKEND_H_INCLUDED
#define RIPPLE_CORE_CASSANDRABACKEND_H_INCLUDED

#include <cassandra.h>
#include <libpq-fe.h>

#include <ripple/basics/Slice.h>
#include <ripple/basics/StringUtilities.h>
#include <ripple/basics/contract.h>
#include <ripple/basics/strHex.h>
#include <ripple/nodestore/Backend.h>
#include <ripple/nodestore/Factory.h>
#include <ripple/nodestore/Manager.h>
#include <ripple/nodestore/impl/DecodedBlob.h>
#include <ripple/nodestore/impl/EncodedBlob.h>
#include <ripple/nodestore/impl/codec.h>
#include <ripple/protocol/digest.h>
#include <boost/asio/steady_timer.hpp>
#include <boost/filesystem.hpp>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <fstream>
#include <memory>
#include <mutex>
#include <nudb/nudb.hpp>
#include <queue>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>

namespace ripple {
namespace NodeStore {

enum class CassTable {
    TxTable,
    StateTable
};

void
readCallback(CassFuture* fut, void* cbData);

void
writeCallback(CassFuture* fut, void* cbData);

class CassandraBackend
{
private:
    CassStatement*
    makeStatement(char const* query, std::size_t params)
    {
        CassStatement* ret = cass_statement_new(query, params);
        CassError rc =
            cass_statement_set_consistency(ret, CASS_CONSISTENCY_QUORUM);
        if (rc != CASS_OK)
        {
            std::stringstream ss;
            ss << "nodestore: Error setting query consistency: " << query
               << ", result: " << rc << ", " << cass_error_desc(rc);
            Throw<std::runtime_error>(ss.str());
        }
        return ret;
    }

    beast::Journal const j_;
    size_t const keyBytes_;
    Section const config_;
    std::atomic<bool> open_{false};

    std::mutex mutex_;
    std::unique_ptr<CassSession, void (*)(CassSession*)> session_{
        nullptr,
        [](CassSession* session) {
            // Try to disconnect gracefully.
            CassFuture* fut = cass_session_close(session);
            cass_future_wait(fut);
            cass_future_free(fut);
            cass_session_free(session);
        }};
    const CassPrepared* insertTx_ = nullptr;
    const CassPrepared* insertEntry_ = nullptr;
    const CassPrepared* selectTx_ = nullptr;
    const CassPrepared* selectEntry_ = nullptr;
    boost::asio::io_context ioContext_;
    std::optional<boost::asio::io_context::work> work_;
    std::thread ioThread_;
    std::atomic_uint32_t numRequestsOutstanding_ = 0;
    uint32_t maxRequestsOutstanding = 10000000;

    std::mutex throttleMutex_;
    std::condition_variable throttleCv_;

    std::mutex syncMutex_;
    std::condition_variable syncCv_;
    Backend::Counters counters_;

public:
    CassandraBackend(
        size_t keyBytes,
        Section const& keyValues,
        beast::Journal journal)
        : j_(journal), keyBytes_(keyBytes), config_(keyValues)
    {}

    ~CassandraBackend()
    {
        close();
    }

    std::string
    getName()
    {
        return "cassandra";
    }

    void
    open(bool createIfMissing);

    void
    close();

    // TODO remove this
    bool
    truncate()
    {
        return true;
    }

    // TODO : retry logic?
    Status
    fetch(CassTable table, uint256 hash, uint32_t seq, std::shared_ptr<Blob>& obj);

    bool
    canFetchBatch()
    {
        return true;
    }

    struct ReadCallbackData
    {
        CassandraBackend& backend;
        CassTable table;

        uint256 hash;
        uint32_t seq;
        std::shared_ptr<Blob>& result;
        std::condition_variable& cv;

        std::atomic_uint32_t& numFinished;
        size_t batchSize;

        ReadCallbackData(
            CassandraBackend& backend,
            CassTable table,
            uint256 hash,
            std::uint32_t seq,
            std::shared_ptr<Blob>& result,
            std::condition_variable& cv,
            std::atomic_uint32_t& numFinished,
            size_t batchSize)
            : backend(backend)
            , table(table)
            , hash(hash)
            , seq(seq)
            , result(result)
            , cv(cv)
            , numFinished(numFinished)
            , batchSize(batchSize)
        {
        }

        ReadCallbackData(ReadCallbackData const& other) = default;
    };

    std::vector<std::shared_ptr<Blob>>
    fetchBatch(CassTable table, std::vector<uint256> hashes, uint32_t seq);

    std::vector<std::shared_ptr<Blob>>
    fetchBatch(CassTable table, std::vector<uint256> hashes, std::vector<uint32_t> sequences);

    void
    read(ReadCallbackData& data);

    struct WriteCallbackData
    {
        CassandraBackend* backend;
        CassTable table;
        
        uint256 hash;
        uint32_t seq;

        std::pair<void const*, std::size_t> compressed = { nullptr, 0 };
        std::chrono::steady_clock::time_point begin;
        // The data is stored in this buffer. The void* in the above member
        // is a pointer into the below buffer
        nudb::detail::buffer bf;
        std::atomic<std::uint64_t>& totalWriteRetries;

        uint32_t currentRetries = 0;

        WriteCallbackData(CassandraBackend* backend,
                     CassTable table,
                     uint256 const& hash,
                     std::uint32_t seq,
                     std::optional<Blob> const& obj,
                     std::atomic<std::uint64_t>& retries)
            : backend(backend)
            , table(table)
            , hash(hash)
            , seq(seq)
            , totalWriteRetries(retries)
        {
            if(obj)
                compressed = lz4_compress(obj->data(), obj->size(), bf);
        }
    };

    void
    write(WriteCallbackData& data, bool isRetry);

    void
    remove(CassTable table, uint256 hash, std::uint32_t seq);

    void
    store(CassTable table, uint256 hash, std::uint32_t seq, Blob object);

    void
    storeBatch(CassTable table, std::vector<uint256> hashes, std::uint32_t seq, std::vector<Blob> objs)
    {
        if(hashes.size() != objs.size())
            Throw<std::runtime_error>("nodestoreHashes and objects must be of equal size");

        for (auto i = 0; i < hashes.size(); ++i)
        {
            store(table, hashes[i], seq, objs[i]);
        }
    }

    void
    sync()
    {
        std::unique_lock<std::mutex> lck(syncMutex_);

        syncCv_.wait(lck, [this]() { return numRequestsOutstanding_ == 0; });
    }

    // Iterate through entire table and execute f(). Used for import only,
    // with database not being written to, so safe to paginate through
    // objects table with LIMIT x OFFSET y.
    void
    for_each(std::function<void(std::shared_ptr<NodeObject>)> f)
    {
        assert(false);
        Throw<std::runtime_error>("not implemented");
    }

    int
    getWriteLoad()
    {
        return 0;
    }

    void
    setDeletePath()
    {
    }

    void
    verify()
    {
    }

    int
    fdRequired() const
    {
        return 0;
    }

    Backend::Counters const&
    counters() const
    {
        return counters_;
    }

    friend void
    writeCallback(CassFuture* fut, void* cbData);

    friend void
    readCallback(CassFuture* fut, void* cbData);
};


}  // namespace NodeStore
}  // namespace ripple
#endif