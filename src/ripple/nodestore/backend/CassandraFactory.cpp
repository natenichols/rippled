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

#include <cassandra.h>
#include <libpq-fe.h>

#include <ripple/basics/contract.h>
#include <ripple/basics/Slice.h>
#include <ripple/basics/strHex.h>
#include <ripple/basics/StringUtilities.h>
#include <ripple/core/Pg.h>
#include <ripple/nodestore/Factory.h>
#include <ripple/nodestore/Manager.h>
#include <ripple/nodestore/impl/codec.h>
#include <ripple/nodestore/impl/DecodedBlob.h>
#include <ripple/nodestore/impl/EncodedBlob.h>
#include <ripple/protocol/digest.h>
#include <nudb/nudb.hpp>
#include <boost/filesystem.hpp>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <exception>
#include <fstream>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <thread>
#include <vector>

namespace ripple {
namespace NodeStore {

void
writeCallback(CassFuture* fut, void* cbData);

class CassandraBackend
    : public Backend
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
    std::atomic<bool> open_ {false};

    std::mutex mutex_;
    std::unique_ptr<CassSession, void(*)(CassSession*)> session_{
        nullptr, [](CassSession* session){
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
    static constexpr uint32_t maxRequestsOutstanding = 1000000;

    std::mutex throttleMutex_;
    std::condition_variable throttleCv_;

    std::mutex syncMutex_;
    std::condition_variable syncCv_;

public:
    CassandraBackend (
        size_t keyBytes,
        Section const& keyValues,
        beast::Journal journal)
        : j_(journal)
        , keyBytes_ (keyBytes)
        , config_ (keyValues)
    {}


    ~CassandraBackend () override
    {
        close();
    }

    std::string
    getName() override
    {
        return "cassandra";
    }

    void
    open(bool createIfMissing) override
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
        std::string contact_points =
            get<std::string>(config_, "contact_points");
        if (contact_points.empty())
        {
            Throw<std::runtime_error> (
                "nodestore: Missing contact_points in Cassandra config");
        }
        CassError rc = cass_cluster_set_contact_points(
            cluster, contact_points.c_str());
        if (rc != CASS_OK)
        {
            std::stringstream ss;
            ss << "nodestore: Error setting Cassandra contact_points: "
                << contact_points << ", result: " << rc
                << ", " << cass_error_desc(rc);

            Throw<std::runtime_error> (ss.str());
        }
	cass_cluster_set_token_aware_routing(cluster, cass_true);
        rc = cass_cluster_set_protocol_version(cluster,
            CASS_PROTOCOL_VERSION_V4);
        if (rc != CASS_OK)
        {
            std::stringstream ss;
            ss << "nodestore: Error setting cassandra protocol version: "
               << contact_points << ", result: " << rc
               << ", " << cass_error_desc(rc);

            Throw<std::runtime_error> (ss.str());
        }

        int port = get<int>(config_, "port");
        if (port)
        {
            rc = cass_cluster_set_port(cluster, port);
            if (rc != CASS_OK)
            {
                std::stringstream ss;
                ss << "nodestore: Error setting Cassandra port: "
                   << port << ", result: " << rc
                   << ", " << cass_error_desc(rc);

                Throw<std::runtime_error> (ss.str());
            }
        }

        std::string username = get<std::string>(config_, "username");
        if (username.size())
        {
            cass_cluster_set_credentials(
                cluster, username.c_str(),
                get<std::string>(config_, "password").c_str());
        }

        unsigned int const workers = std::thread::hardware_concurrency();
        rc = cass_cluster_set_num_threads_io(cluster, workers);
        if (rc != CASS_OK)
        {
            std::stringstream ss;
            ss << "nodestore: Error setting Cassandra io threads to "
               << workers
               << ", result: " << rc
               << ", " << cass_error_desc(rc);
            Throw<std::runtime_error> (ss.str());
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

            std::ifstream fileStream(boost::filesystem::path(certfile).string(),
                                     std::ios::in);
            if (!fileStream)
            {
                std::stringstream ss;
                ss << "opening config file " << certfile;
                Throw<std::system_error>(errno, std::generic_category(),
                                         ss.str());
            }
            std::string cert(std::istreambuf_iterator<char>{fileStream},
                std::istreambuf_iterator<char>{});
            if (fileStream.bad())
            {
                std::stringstream ss;
                ss << "reading config file " << certfile;
                Throw<std::system_error>(errno, std::generic_category(),
                                         ss.str());
            }

            CassSsl* context = cass_ssl_new();
            cass_ssl_set_verify_flags(context, CASS_SSL_VERIFY_NONE);
            rc = cass_ssl_add_trusted_cert(context, cert.c_str());
            if (rc != CASS_OK)
            {
                std::stringstream ss;
                ss << "nodestore: Error setting Cassandra ssl context: "
                   << rc
                   << ", " << cass_error_desc(rc);
                Throw<std::runtime_error> (ss.str());
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
            Throw<std::runtime_error> (
                "nodestore: Missing keyspace in Cassandra config");
        }

        CassStatement* statement;
        CassFuture* fut;
        while (true)
        {
            session_.reset(cass_session_new());
            assert(session_);

            fut = cass_session_connect_keyspace(
                session_.get(), cluster, keyspace.c_str());
            rc = cass_future_error_code(fut);
            if (rc != CASS_OK)
            {
                std::stringstream ss;
                ss << "nodestore: Error connecting Cassandra session: " << rc
                   << ", " << cass_error_desc(rc);
                Throw<std::runtime_error> (ss.str());
            }
            cass_future_free(fut);

            statement = makeStatement(
                "CREATE TABLE IF NOT EXISTS objects ("
                "    hash   blob PRIMARY KEY, "
                "    object blob)",
                0);
            fut = cass_session_execute(session_.get(), statement);
            rc = cass_future_error_code(fut);
            if (rc != CASS_OK && rc != CASS_ERROR_SERVER_INVALID_QUERY)
            {
                std::stringstream ss;
                ss << "nodestore: Error creating Cassandra objects table: "
                   << rc << ", " << cass_error_desc(rc);
                Throw<std::runtime_error>(ss.str());
            }
            cass_future_free(fut);
            cass_statement_free(statement);

            statement = makeStatement("SELECT * FROM objects LIMIT 1", 0);
            fut = cass_session_execute(session_.get(), statement);
            rc = cass_future_error_code(fut);
            if (rc != CASS_OK)
            {
                if (rc == CASS_ERROR_SERVER_INVALID_QUERY)
                {
                    cass_future_free(fut);
                    cass_statement_free(statement);
                    std::cerr << "objects table not here yet, sleeping 1s to see if table creation propagates\n";
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    continue;
                }
                std::stringstream ss;
                ss << "nodestore: Error checking for objects table: "
                   << rc << ", " << cass_error_desc(rc);
                Throw<std::runtime_error>(ss.str());
            }
            cass_future_free(fut);
            cass_statement_free(statement);
            break;
        }
        cass_cluster_free(cluster);
	CassFuture* prepare_future
		  = cass_session_prepare(session_.get(), "INSERT INTO objects (hash, object) VALUES (?, ?)");

	/* Wait for the statement to prepare and get the result */
	rc = cass_future_error_code(prepare_future);

	printf("Prepare result: %s\n", cass_error_desc(rc));

	if (rc != CASS_OK) {
		  /* Handle error */
		  cass_future_free(prepare_future);

                std::stringstream ss;
                ss << "nodestore: Error preparing insert : "
                   << rc << ", " << cass_error_desc(rc);
                Throw<std::runtime_error>(ss.str());
	}

	/* Get the prepared object from the future */
	insert_ = cass_future_get_prepared(prepare_future);

	/* The future can be freed immediately after getting the prepared object */
	cass_future_free(prepare_future);


	prepare_future
		  = cass_session_prepare(session_.get(), "SELECT object FROM objects WHERE hash = ?");


	/* Wait for the statement to prepare and get the result */
	rc = cass_future_error_code(prepare_future);

	printf("Prepare result: %s\n", cass_error_desc(rc));

	if (rc != CASS_OK) {
		  /* Handle error */
		  cass_future_free(prepare_future);

                std::stringstream ss;
                ss << "nodestore: Error preparing select : "
                   << rc << ", " << cass_error_desc(rc);
                Throw<std::runtime_error>(ss.str());
	}

	/* Get the prepared object from the future */
	select_ = cass_future_get_prepared(prepare_future);

	/* The future can be freed immediately after getting the prepared object */
	cass_future_free(prepare_future);

    work_.emplace(ioContext_);
    ioThread_ = std::thread{[this]() { ioContext_.run(); }};
    open_ = true;
    }

    // TODO remove this
    bool
    truncate() override
    {
        return true;
    }

    void
    close() override
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
            ioThread_.join();
        }
        open_ = false;
    }

    Status
    fetch (void const* key, std::shared_ptr<NodeObject>* pno) override
    {
        CassStatement* statement = cass_prepared_bind(select_);
        cass_statement_set_consistency(statement, CASS_CONSISTENCY_QUORUM);
        CassError rc = cass_statement_bind_bytes(
            statement, 0, static_cast<cass_byte_t const*>(key), keyBytes_);
        if (rc != CASS_OK)
        {
            cass_statement_free(statement);
            JLOG(j_.error()) << "Binding Cassandra fetch query: " << rc
                << ", " << cass_error_desc(rc);
            pno->reset();
            return backendError;
        }
        CassFuture* fut;
        do
        {
            fut = cass_session_execute(session_.get(), statement);
            rc = cass_future_error_code(fut);
        } while (rc == CASS_ERROR_LIB_REQUEST_TIMED_OUT);

        if (rc != CASS_OK)
        {
            cass_statement_free(statement);
            cass_future_free(fut);
            JLOG(j_.error()) << "Cassandra fetch error: " << rc
                << ", " << cass_error_desc(rc);
            pno->reset();
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
            JLOG(j_.error()) << "Cassandra fetch result error: " << rc
                << ", " << cass_error_desc(rc);
            return backendError;
        }

        nudb::detail::buffer bf;
        std::pair<void const*, std::size_t> uncompressed =
            nodeobject_decompress(buf, bufSize, bf);
        DecodedBlob decoded(key, uncompressed.first, uncompressed.second);
        cass_result_free(res);

        if (!decoded.wasOk())
        {
            pno->reset();
            JLOG(j_.error()) << "Cassandra error decoding result: " << rc
                << ", " << cass_error_desc(rc);
            return dataCorrupt;
        }
        *pno = decoded.createObject();
        return ok;


    }

    bool
    canFetchBatch() override
    {
        return false;
    }

    std::vector<std::shared_ptr<NodeObject>>
    fetchBatch (std::size_t n, void const* const* keys) override
    {
        Throw<std::runtime_error> ("pure virtual called");
        return {};
    }

    struct CallbackData
    {
        CassandraBackend* backend;
        NodeStore::EncodedBlob e;
        std::pair<void const*, std::size_t> compressed;
        // The data is stored in this buffer. The void* in the above member
        // is a pointer into the below buffer
        nudb::detail::buffer bf;

        CallbackData(CassandraBackend* f, std::shared_ptr<NodeObject> const& no)
            : backend(f)
        {
            e.prepare(no);

            compressed =
                NodeStore::nodeobject_compress(e.getData(), e.getSize(), bf);
            }
    };

    void
    write(
        CallbackData& data,
        bool isRetry)
    {
        {
            std::unique_lock<std::mutex> lck(throttleMutex_);
            if (!isRetry && numRequestsOutstanding_ > maxRequestsOutstanding)
            {
                std::cout
                    << "max outstanding reached. waiting for others to finish"
                    << std::endl;
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
            static_cast<cass_byte_t const*>(data.e.getKey()),
            keyBytes_);
        if (rc != CASS_OK)
        {
            cass_statement_free(statement);
            std::stringstream ss;
            ss << "Binding cassandra insert hash: " << rc << ", "
               << cass_error_desc(rc);
            Throw<std::runtime_error>(ss.str());
        }
        rc = cass_statement_bind_bytes(
            statement,
            1,
            static_cast<cass_byte_t const*>(data.compressed.first),
            data.compressed.second);
        if (rc != CASS_OK)
        {
            cass_statement_free(statement);
            std::stringstream ss;
            ss << "Binding cassandra insert object: " << rc << ", "
               << cass_error_desc(rc);
            Throw<std::runtime_error>(ss.str());
        }
        CassFuture* fut = cass_session_execute(session_.get(), statement);
        cass_statement_free(statement);

        cass_future_set_callback(fut, writeCallback, static_cast<void*>(&data));
        cass_future_free(fut);
    }


    void
    store (std::shared_ptr <NodeObject> const& no) override
    {
        CallbackData* data = new CallbackData(this, no);

        ++numRequestsOutstanding_;
        write(*data, false);
    }

    void
    storeBatch (Batch const& batch) override
    {
        for (auto const& no : batch)
        {
            store(no);
        }
    }

    void
    sync() override
    {
        std::unique_lock<std::mutex> lck(syncMutex_);

        syncCv_.wait(lck, [this]() { return numRequestsOutstanding_ == 0; });
    }

    // Iterate through entire table and execute f(). Used for import only,
    // with database not being written to, so safe to paginate through
    // objects table with LIMIT x OFFSET y.
    void
    for_each (std::function <void(std::shared_ptr<NodeObject>)> f) override
    {
        assert(false);
        Throw<std::runtime_error> ("not implemented");
    }

    int
    getWriteLoad () override
    {
        return 0;
    }

    void
    setDeletePath() override
    {
    }

    void
    verify() override
    {}

    int
    fdRequired() const override
    {
        return 0;
    }
    friend void
    writeCallback(CassFuture* fut, void* cbData);
};

void
writeCallback(CassFuture* fut, void* cbData)
{
    CassandraBackend::CallbackData& requestParams =
        *static_cast<CassandraBackend::CallbackData*>(cbData);
    CassandraBackend& backend = *requestParams.backend;
    auto rc = cass_future_error_code(fut);
    if (rc != CASS_OK)
    {
        std::cout << "ERROR!!! Cassandra insert error: " << rc << ", "
                  << cass_error_desc(rc) << ", retrying ";

        std::shared_ptr<boost::asio::steady_timer> timer =
            std::make_shared<boost::asio::steady_timer>(
                backend.ioContext_,
                std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(100));
        timer->async_wait([timer, &requestParams, &backend](
                              const boost::system::error_code& error) {
            backend.write(requestParams, true);
        });
    }
    else
    {
        --(backend.numRequestsOutstanding_);

        backend.throttleCv_.notify_all();
        if (backend.numRequestsOutstanding_ == 0)
            backend.syncCv_.notify_all();
        delete &requestParams;
    }
}

//------------------------------------------------------------------------------

class CassandraFactory : public Factory
{
public:
    CassandraFactory()
    {
        Manager::instance().insert(*this);
    }

    ~CassandraFactory() override
    {
        Manager::instance().erase(*this);
    }

    std::string
    getName() const override
    {
        return "cassandra";
    }

    std::unique_ptr <Backend>
    createInstance (
        size_t keyBytes,
        Section const& keyValues,
        Scheduler& scheduler,
        beast::Journal journal) override
    {
        return std::make_unique <CassandraBackend> (
            keyBytes, keyValues, journal);
    }

    std::unique_ptr <Backend>
    createInstance (
        size_t keyBytes,
        Section const& keyValues,
        Scheduler& scheduler,
        nudb::context& context,
        beast::Journal journal) override
    {
        return std::make_unique <CassandraBackend> (
            keyBytes, keyValues, journal);
    }
};

static CassandraFactory cassandraFactory;

}
}
