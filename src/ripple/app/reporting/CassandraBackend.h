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
    const CassPrepared* insert_ = nullptr;
    const CassPrepared* select_ = nullptr;
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
    open(bool createIfMissing)
    {
        if (open_)
        {
            assert(false);
            JLOG(j_.error()) << "database is already open";
            return;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        CassCluster* cluster = cass_cluster_new();
        assert(cluster);

        std::string secureConnectBundle =
            get<std::string>(config_, "secure_connect_bundle");

        if (!secureConnectBundle.empty())
        {
            /* Setup driver to connect to the cloud using the secure connection
             * bundle */
            if (cass_cluster_set_cloud_secure_connection_bundle(
                    cluster, secureConnectBundle.c_str()) != CASS_OK)
            {
                JLOG(j_.error()) << "Unable to configure cloud using the "
                                    "secure connection bundle: "
                                 << secureConnectBundle;
                Throw<std::runtime_error>(
                    "nodestore: Failed to connect using secure connection "
                    "bundle");
                return;
            }
        }
        else
        {
            std::string contact_points =
                get<std::string>(config_, "contact_points");
            if (contact_points.empty())
            {
                Throw<std::runtime_error>(
                    "nodestore: Missing contact_points in Cassandra config");
            }
            CassError rc = cass_cluster_set_contact_points(
                cluster, contact_points.c_str());
            if (rc != CASS_OK)
            {
                std::stringstream ss;
                ss << "nodestore: Error setting Cassandra contact_points: "
                   << contact_points << ", result: " << rc << ", "
                   << cass_error_desc(rc);

                Throw<std::runtime_error>(ss.str());
            }

            int port = get<int>(config_, "port");
            if (port)
            {
                rc = cass_cluster_set_port(cluster, port);
                if (rc != CASS_OK)
                {
                    std::stringstream ss;
                    ss << "nodestore: Error setting Cassandra port: " << port
                       << ", result: " << rc << ", " << cass_error_desc(rc);

                    Throw<std::runtime_error>(ss.str());
                }
            }
        }
        cass_cluster_set_token_aware_routing(cluster, cass_true);
        CassError rc = cass_cluster_set_protocol_version(
            cluster, CASS_PROTOCOL_VERSION_V4);
        if (rc != CASS_OK)
        {
            std::stringstream ss;
            ss << "nodestore: Error setting cassandra protocol version: "
               << ", result: " << rc << ", " << cass_error_desc(rc);

            Throw<std::runtime_error>(ss.str());
        }

        std::string username = get<std::string>(config_, "username");
        if (username.size())
        {
            std::cout << "user = " << username.c_str() << " password = "
                      << get<std::string>(config_, "password").c_str()
                      << std::endl;
            cass_cluster_set_credentials(
                cluster,
                username.c_str(),
                get<std::string>(config_, "password").c_str());
        }

        unsigned int const workers = std::thread::hardware_concurrency();
        rc = cass_cluster_set_num_threads_io(cluster, workers);
        if (rc != CASS_OK)
        {
            std::stringstream ss;
            ss << "nodestore: Error setting Cassandra io threads to " << workers
               << ", result: " << rc << ", " << cass_error_desc(rc);
            Throw<std::runtime_error>(ss.str());
        }

        cass_cluster_set_request_timeout(cluster, 2000);

        rc = cass_cluster_set_queue_size_io(
            cluster,
            maxRequestsOutstanding);  // This number needs to scale w/ the
                                      // number of request per sec
        if (rc != CASS_OK)
        {
            std::stringstream ss;
            ss << "nodestore: Error setting Cassandra max core connections per "
                  "host"
               << ", result: " << rc << ", " << cass_error_desc(rc);
            std::cout << ss.str() << std::endl;
            return;
            ;
        }

        std::string certfile = get<std::string>(config_, "certfile");
        if (certfile.size())
        {
            std::ifstream fileStream(
                boost::filesystem::path(certfile).string(), std::ios::in);
            if (!fileStream)
            {
                std::stringstream ss;
                ss << "opening config file " << certfile;
                Throw<std::system_error>(
                    errno, std::generic_category(), ss.str());
            }
            std::string cert(
                std::istreambuf_iterator<char>{fileStream},
                std::istreambuf_iterator<char>{});
            if (fileStream.bad())
            {
                std::stringstream ss;
                ss << "reading config file " << certfile;
                Throw<std::system_error>(
                    errno, std::generic_category(), ss.str());
            }

            CassSsl* context = cass_ssl_new();
            cass_ssl_set_verify_flags(context, CASS_SSL_VERIFY_NONE);
            rc = cass_ssl_add_trusted_cert(context, cert.c_str());
            if (rc != CASS_OK)
            {
                std::stringstream ss;
                ss << "nodestore: Error setting Cassandra ssl context: " << rc
                   << ", " << cass_error_desc(rc);
                Throw<std::runtime_error>(ss.str());
            }

            cass_cluster_set_ssl(cluster, context);
            cass_ssl_free(context);
        }

        /*
        rc = cass_cluster_set_consistency(cluster,
                                          CASS_CONSISTENCY_LOCAL_QUORUM);
        if (rc != CASS_OK)
        {
            std::stringstream ss;
            ss << "nodestore: Error setting Cassandra cluster consistency: "
               << rc
               << ", " << cass_error_desc(rc);
            Throw<std::runtime_error> (ss.str());
        }
*/
        std::string keyspace = get<std::string>(config_, "keyspace");
        if (keyspace.empty())
        {
            Throw<std::runtime_error>(
                "nodestore: Missing keyspace in Cassandra config");
        }

        cass_cluster_set_connect_timeout(cluster, 10000);

        CassStatement* statement;
        CassFuture* fut;
        bool setupSessionAndTable = false;
        while (!setupSessionAndTable)
        {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            session_.reset(cass_session_new());
            assert(session_);

            fut = cass_session_connect_keyspace(
                session_.get(), cluster, keyspace.c_str());
            rc = cass_future_error_code(fut);
            cass_future_free(fut);
            if (rc != CASS_OK)
            {
                std::stringstream ss;
                ss << "nodestore: Error connecting Cassandra session keyspace: "
                   << rc << ", " << cass_error_desc(rc);
                JLOG(j_.error()) << ss.str();
                continue;
            }

            statement = makeStatement(
                "CREATE TABLE IF NOT EXISTS objects ("
                "    hash   blob, "
                "    seq    int, "
                "    object blob, "
                "    PRIMARY KEY (hash, seq) "
                ")",
                0);
            fut = cass_session_execute(session_.get(), statement);
            rc = cass_future_error_code(fut);
            cass_future_free(fut);
            cass_statement_free(statement);
            if (rc != CASS_OK && rc != CASS_ERROR_SERVER_INVALID_QUERY)
            {
                std::stringstream ss;
                ss << "nodestore: Error creating Cassandra objects table: "
                   << rc << ", " << cass_error_desc(rc);
                JLOG(j_.error()) << ss.str();
                continue;
            }

            statement = makeStatement("SELECT * FROM objects LIMIT 1", 0);
            fut = cass_session_execute(session_.get(), statement);
            rc = cass_future_error_code(fut);
            cass_future_free(fut);
            cass_statement_free(statement);
            if (rc != CASS_OK)
            {
                if (rc == CASS_ERROR_SERVER_INVALID_QUERY)
                {
                    JLOG(j_.warn())
                        << "objects table not here yet, sleeping 1s to "
                           "see if table creation propagates";
                    continue;
                }
                else
                {
                    std::stringstream ss;
                    ss << "nodestore: Error checking for objects table: " << rc
                       << ", " << cass_error_desc(rc);
                    JLOG(j_.error()) << ss.str();
                    continue;
                }
            }

            setupSessionAndTable = true;
        }

        cass_cluster_free(cluster);

        bool setupPreparedStatements = false;
        while (!setupPreparedStatements)
        {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            CassFuture* prepare_future = cass_session_prepare(
                session_.get(),
                "INSERT INTO objects (hash, seq, object) VALUES (?, ?, ?)");

            /* Wait for the statement to prepare and get the result */
            rc = cass_future_error_code(prepare_future);

            if (rc != CASS_OK)
            {
                /* Handle error */
                cass_future_free(prepare_future);

                std::stringstream ss;
                ss << "nodestore: Error preparing insert : " << rc << ", "
                   << cass_error_desc(rc);
                JLOG(j_.error()) << ss.str();
                continue;
            }

            /* Get the prepared object from the future */
            insert_ = cass_future_get_prepared(prepare_future);

            /* The future can be freed immediately after getting the prepared
             * object
             */
            cass_future_free(prepare_future);

            prepare_future = cass_session_prepare(
                session_.get(), "SELECT object FROM objects WHERE hash = ? AND seq <= ? ORDER BY seq DESC LIMIT 1");

            /* Wait for the statement to prepare and get the result */
            rc = cass_future_error_code(prepare_future);

            if (rc != CASS_OK)
            {
                /* Handle error */
                cass_future_free(prepare_future);

                std::stringstream ss;
                ss << "nodestore: Error preparing select : " << rc << ", "
                   << cass_error_desc(rc);
                JLOG(j_.error()) << ss.str();
                continue;
            }

            /* Get the prepared object from the future */
            select_ = cass_future_get_prepared(prepare_future);

            /* The future can be freed immediately after getting the prepared
             * object
             */
            cass_future_free(prepare_future);
            setupPreparedStatements = true;
        }

        work_.emplace(ioContext_);
        ioThread_ = std::thread{[this]() { ioContext_.run(); }};
        open_ = true;

        if (config_.exists("max_requests_outstanding"))
        {
            maxRequestsOutstanding =
                get<int>(config_, "max_requests_outstanding");
        }
    }

    // TODO remove this
    bool
    truncate()
    {
        return true;
    }

    void
    close()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (insert_)
            {
                cass_prepared_free(insert_);
                insert_ = nullptr;
            }
            if (select_)
            {
                cass_prepared_free(select_);
                select_ = nullptr;
            }
            work_.reset();
            if(ioThread_.joinable())
                ioThread_.join();
        }
        open_ = false;
    }

    // TODO : retry logic?
    Status
    fetch(void const* key, std::shared_ptr<NodeObject>* pno)
    {
        JLOG(j_.trace()) << "Fetching from cassandra";
        CassStatement* statement = cass_prepared_bind(select_);
        cass_statement_set_consistency(statement, CASS_CONSISTENCY_QUORUM);

        CassError rc = cass_statement_bind_bytes(
            statement, 0, static_cast<cass_byte_t const*>(key), keyBytes_);
        if (rc != CASS_OK)
        {
            cass_statement_free(statement);
            JLOG(j_.error()) << "Binding Cassandra fetch key query: " << rc << ", "
                             << cass_error_desc(rc);
            pno->reset();
            return backendError;
        }

        CassFuture* fut;
        do
        {
            fut = cass_session_execute(session_.get(), statement);
            rc = cass_future_error_code(fut);
            if (rc != CASS_OK)
            {
                std::stringstream ss;
                ss << "Cassandra fetch error";
                if (rc == CASS_ERROR_LIB_REQUEST_TIMED_OUT)
                {
                    ss << ", retrying";
                    ++counters_.readRetries;
                }
                ss << ": " << cass_error_desc(rc);
                JLOG(j_.warn()) << ss.str();
            }
        } while (rc == CASS_ERROR_LIB_REQUEST_TIMED_OUT);

        if (rc != CASS_OK)
        {
            cass_statement_free(statement);
            cass_future_free(fut);
            JLOG(j_.error()) << "Cassandra fetch error: " << rc << ", "
                             << cass_error_desc(rc);
            pno->reset();
            ++counters_.readErrors;
            return backendError;
        }

        CassResult const* res = cass_future_get_result(fut);
        cass_statement_free(statement);
        cass_future_free(fut);

        CassRow const* row = cass_result_first_row(res);
        if (!row)
        {
            cass_result_free(res);
            pno->reset();
            return notFound;
        }
        cass_byte_t const* buf;
        std::size_t bufSize;
        rc = cass_value_get_bytes(cass_row_get_column(row, 0), &buf, &bufSize);
        if (rc != CASS_OK)
        {
            cass_result_free(res);
            pno->reset();
            JLOG(j_.error()) << "Cassandra fetch result error: " << rc << ", "
                             << cass_error_desc(rc);
            ++counters_.readErrors;
            return backendError;
        }

        nudb::detail::buffer bf;
        auto [uncompressed, size] = nodeobject_decompress(buf, bufSize, bf);
        DecodedBlob decoded(key, uncompressed, size);
        cass_result_free(res);

        if (!decoded.wasOk())
        {
            pno->reset();
            JLOG(j_.error()) << "Cassandra error decoding result: " << rc
                             << ", " << cass_error_desc(rc);
            ++counters_.readErrors;
            return dataCorrupt;
        }
        *pno = decoded.createObject();
        return ok;
    }

    bool
    canFetchBatch()
    {
        return true;
    }

    struct ReadCallbackData
    {
        CassandraBackend& backend;
        uint256 hash;
        uint32_t seq;
        std::shared_ptr<Blob>& result;
        std::condition_variable& cv;

        std::atomic_uint32_t& numFinished;
        size_t batchSize;

        ReadCallbackData(
            CassandraBackend& backend,
            uint256 hash,
            std::uint32_t seq,
            std::shared_ptr<Blob>& result,
            std::condition_variable& cv,
            std::atomic_uint32_t& numFinished,
            size_t batchSize)
            : backend(backend)
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
    fetchBatch(std::vector<uint256> hashes, std::uint32_t seq)
    {
        std::size_t n = hashes.size();
        JLOG(j_.trace()) << "Fetching " << n << " records from Cassandra";
        std::atomic_uint32_t numFinished = 0;
        std::condition_variable cv;
        std::mutex mtx;
        std::vector<std::shared_ptr<Blob>> results{n};
        std::vector<std::shared_ptr<ReadCallbackData>> cbs{n};
        for (std::size_t i = 0; i < n; ++i)
        {
            cbs[i] = std::make_shared<ReadCallbackData>(
                *this, hashes[i], seq, results[i], cv, numFinished, n);
            read(*cbs[i]);
        }
        assert(results.size() == cbs.size());

        std::unique_lock<std::mutex> lck(mtx);
        cv.wait(lck, [&numFinished, &n]() { return numFinished == n; });

        JLOG(j_.trace()) << "Fetched " << n << " records from Cassandra";
        return results;
    }

    void
    read(ReadCallbackData& data)
    {
        CassStatement* statement = cass_prepared_bind(select_);
        cass_statement_set_consistency(statement, CASS_CONSISTENCY_QUORUM);
        CassError rc = cass_statement_bind_bytes(
            statement, 0, static_cast<cass_byte_t const*>(data.hash.begin()), keyBytes_);
        if (rc != CASS_OK)
        {
            cass_statement_free(statement);
            JLOG(j_.error()) << "Binding Cassandra fetch query hash: " << rc << ", "
                             << cass_error_desc(rc);
            return;
        }

        rc = cass_statement_bind_int32(statement, 1, data.seq);        
        if (rc != CASS_OK)
        {
            cass_statement_free(statement);
            JLOG(j_.error()) << "Binding Cassandra fetch query seq: " << rc << ", "
                             << cass_error_desc(rc);
            return;
        }

        CassFuture* fut = cass_session_execute(session_.get(), statement);

        cass_statement_free(statement);

        cass_future_set_callback(fut, readCallback, static_cast<void*>(&data));
        cass_future_free(fut);
    }

    struct WriteCallbackData
    {
        CassandraBackend* backend;
        
        uint256 hash;
        uint32_t seq;
        Blob obj;

        std::pair<void const*, std::size_t> compressed;
        std::chrono::steady_clock::time_point begin;
        // The data is stored in this buffer. The void* in the above member
        // is a pointer into the below buffer
        nudb::detail::buffer bf;
        std::atomic<std::uint64_t>& totalWriteRetries;

        uint32_t currentRetries = 0;

        WriteCallbackData(CassandraBackend* f,
                     uint256 const& h,
                     std::uint32_t s,
                     Blob const& o,
                     std::atomic<std::uint64_t>& retries)
            : backend(f)
            , hash(h)
            , seq(s)
            , obj(o)
            , totalWriteRetries(retries)
        {
            compressed =
                NodeStore::nodeobject_compress(obj.data(), obj.size(), bf);
        }
    };

    void
    write(WriteCallbackData& data, bool isRetry)
    {
        {
            // We limit the total number of concurrent inflight writes. This is
            // a client side throttling to prevent overloading the database.
            // This is mostly useful when the very first ledger is being written
            // in full, which is several millions records. On sufficiently large
            // Cassandra clusters, this throttling is not needed; the default
            // value of maxRequestsOutstanding is 10 million, which is more 
            // records than are present in any single ledger
            std::unique_lock<std::mutex> lck(throttleMutex_);
            if (!isRetry && numRequestsOutstanding_ > maxRequestsOutstanding)
            {
                JLOG(j_.trace()) << __func__ << " : "
                                << "Max outstanding requests reached. "
                                << "Waiting for other requests to finish";
                ++counters_.writesDelayed;
                throttleCv_.wait(lck, [this]() {
                    return numRequestsOutstanding_ < maxRequestsOutstanding;
                });
            }
        }

        CassStatement* statement = cass_prepared_bind(insert_);
        cass_statement_set_consistency(statement, CASS_CONSISTENCY_QUORUM);
        CassError rc = cass_statement_bind_bytes(
            statement,
            0,
            static_cast<cass_byte_t const*>(data.hash.begin()),
            keyBytes_);
        if (rc != CASS_OK)
        {
            cass_statement_free(statement);
            std::stringstream ss;
            ss << "Binding cassandra insert hash: " << rc << ", "
               << cass_error_desc(rc);
            JLOG(j_.error()) << __func__ << " : " << ss.str();
            Throw<std::runtime_error>(ss.str());
        }

        rc = cass_statement_bind_int32(statement, 1, data.seq);
        if (rc != CASS_OK)
        {
            cass_statement_free(statement);
            std::stringstream ss;
            ss << "Binding cassandra insert object: " << rc << ", "
               << cass_error_desc(rc);
            JLOG(j_.error()) << __func__ << " : " << ss.str();
            Throw<std::runtime_error>(ss.str());
        }

        rc = cass_statement_bind_bytes(
            statement,
            2,
            static_cast<cass_byte_t const*>(data.obj.data()),
            data.obj.size());
        if (rc != CASS_OK)
        {
            cass_statement_free(statement);
            std::stringstream ss;
            ss << "Binding cassandra insert object: " << rc << ", "
               << cass_error_desc(rc);
            JLOG(j_.error()) << __func__ << " : " << ss.str();
            Throw<std::runtime_error>(ss.str());
        }
        data.begin = std::chrono::steady_clock::now();
        CassFuture* fut = cass_session_execute(session_.get(), statement);
        cass_statement_free(statement);

        cass_future_set_callback(fut, writeCallback, static_cast<void*>(&data));
        cass_future_free(fut);
    }

    void
    store(uint256 hash, std::uint32_t seq, Blob object)
    {
        // JLOG(j_.trace()) << "Writing to cassandra";
        WriteCallbackData* data = new WriteCallbackData(this, hash, seq, object, counters_.writeRetries);

        ++numRequestsOutstanding_;
        write(*data, false);
    }

    // void
    // storeBatch(std::vector<uint256> hashes, std::uint32_t seq, std::vector<Blob> objs)
    // {
    //     for (auto const& no : batch)
    //     {
    //         store(no);
    //     }
    // }

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
