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

class CassandraBackend
    : public Backend
{
private:
    void
    storeLocked(std::shared_ptr<NodeObject> const& no)
    {
        if (batches_.empty() || batches_.back().size() == batchSize_)
        {
            batches_.push(
                std::vector<std::pair<std::shared_ptr<NodeObject>,
                    CassFuture*>>());
            batches_.back().reserve(batchSize_);
        }
        batches_.back().push_back({no, nullptr});
    }

public:
    static constexpr std::size_t currentType = 1;
    static constexpr std::size_t default_batch_size = 10000;

    beast::Journal const j_;
    size_t const keyBytes_;
    Section const config_;
    std::size_t const batchSize_;
    std::string const name_;
    nudb::store db_;
    std::atomic <bool> deletePath_;
    Scheduler& scheduler_;
    std::shared_ptr<PgPool> pool_;
    std::atomic<bool> open_ {false};
    std::shared_ptr<PgQuery> pgQuery_ {std::make_shared<PgQuery>(pool_)};

    std::mutex mutex_;
    std::unique_ptr<CassSession, void(*)(CassSession*)> session_{
        nullptr, [](CassSession* session){
            // Try to disconnect gracefully.
            CassFuture* fut = cass_session_close(session);
            cass_future_wait(fut);
            cass_future_free(fut);
            cass_session_free(session);
        }};
    CassPrepared* insert_ = nullptr;
    CassPrepared* select_ = nullptr;
    CassPrepared* truncate_ = nullptr;

//    std::vector<std::pair<std::shared_ptr<NodeObject>, CassFuture*>> batch_;
    std::queue<std::vector<
        std::pair<std::shared_ptr<NodeObject>, CassFuture*>>> batches_;

    CassandraBackend (
        size_t keyBytes,
        Section const& keyValues,
        Scheduler& scheduler,
        beast::Journal journal,
        std::shared_ptr<PgPool>& pool)
        : j_(journal)
        , keyBytes_ (keyBytes)
        , config_ (keyValues)
        , batchSize_ (get<std::size_t>(config_, "io_threads",
                                      default_batch_size))
        , deletePath_(false)
        , scheduler_ (scheduler)
        , pool_ (pool)
    {}

    CassandraBackend (
        size_t keyBytes,
        Section const& keyValues,
        Scheduler& scheduler,
        nudb::context& context,
        beast::Journal journal)
        : j_(journal)
        , keyBytes_ (keyBytes)
        , config_ (keyValues)
        , batchSize_ (get<std::size_t>(config_, "io_threads",
                                      default_batch_size))
        , db_ (context)
        , deletePath_(false)
        , scheduler_ (scheduler)
    {}

    ~CassandraBackend () override
    {
        close();
    }

    std::string
    getName() override
    {
        return name_;
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

            statement = cass_statement_new(
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

            statement = cass_statement_new(
                "SELECT * FROM objects LIMIT 1", 0);
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
        
        statement = cass_statement_new(
            "INSERT INTO objects (hash, object) VALUES (?, ?)", 2);
        rc = cass_statement_set_consistency(statement,
                                            CASS_CONSISTENCY_LOCAL_QUORUM);
        if (rc != CASS_OK)
        {
            std::stringstream ss;
            ss << "nodestore: Error setting consistency for insert: "
               << rc << ", " << cass_error_desc(rc);
            Throw<std::runtime_error>(ss.str());
        }
        fut = cass_session_prepare_from_existing(session_.get(), statement);
        rc = cass_future_error_code(fut);
        if (rc != CASS_OK)
        {
            std::stringstream ss;
            ss << "nodestore: Error preparing insert: " << rc
               << ", " << cass_error_desc(rc);
            Throw<std::runtime_error>(ss.str());
        }
        cass_future_free(fut);
        cass_statement_free(statement);

        statement = cass_statement_new(
            "SELECT object FROM objects WHERE hash = ?", 1);
        rc = cass_statement_set_consistency(statement,
            CASS_CONSISTENCY_LOCAL_QUORUM);
        if (rc != CASS_OK)
        {
            std::stringstream ss;
            ss << "nodestore: Error setting consistency for select: "
               << rc << ", " << cass_error_desc(rc);
            Throw<std::runtime_error>(ss.str());
        }
        fut = cass_session_prepare_from_existing(session_.get(), statement);
        rc = cass_future_error_code(fut);
        if (rc != CASS_OK)
        {
            std::stringstream ss;
            ss << "nodestore: Error preparing select: " << rc
               << ", " << cass_error_desc(rc);
            Throw<std::runtime_error>(ss.str());
        }
        cass_future_free(fut);
        cass_statement_free(statement);

        /*
        fut = cass_session_prepare(
            session_.get(),
            "INSERT INTO objects (hash, object) VALUES (?, ?)");
        rc = cass_future_error_code(fut);
        if (rc != CASS_OK)
        {
            std::stringstream ss;
            ss << "nodestore: Error preparing Cassandra insert: " << rc
               << ", " << cass_error_desc(rc);
            Throw<std::runtime_error>(ss.str());
        }
        insert_ = const_cast<CassPrepared*>(cass_future_get_prepared(fut));
        cass_future_free(fut);

        fut = cass_session_prepare(
            session_.get(), "SELECT object FROM objects WHERE hash = ?");
        rc = cass_future_error_code(fut);
        if (rc != CASS_OK)
        {
            std::stringstream ss;
            ss << "nodestore: Error preparing Cassandra select: " << rc
               << ", " << cass_error_desc(rc);
            Throw<std::runtime_error>(ss.str());
        }
        select_ = const_cast<CassPrepared*>(cass_future_get_prepared(fut));
        cass_future_free(fut);
         */

        /*
        fut = cass_session_prepare(session_.get(), "TRUNCATE TABLE objects");
        rc = cass_future_error_code(fut);
        if (rc != CASS_OK)
        {
            std::stringstream ss;
            ss << "nodestore: Error preparing Cassandra truncate: " << rc
               << ", " << cass_error_desc(rc);
            Throw<std::runtime_error>(ss.str());
        }
        truncate_ = const_cast<CassPrepared*>(cass_future_get_prepared(fut));
        cass_future_free(fut);
         */

        open_ = true;
    }

    bool
    truncate() override
    {
        return true;
        /*
        CassStatement* statement = cass_prepared_bind(truncate_);

        CassFuture* fut = cass_session_execute(session_.get(), statement);
        CassError rc = cass_future_error_code(fut);
        if (rc != CASS_OK)
        {
            cass_statement_free(statement);
            cass_future_free(fut);
            JLOG(j_.error()) << "Cassandra truncate error: " << rc;
            return false;
        }

        cass_statement_free(statement);
        cass_future_free(fut);
        return true;
         */
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
            if (truncate_)
            {
                cass_prepared_free(truncate_);
                truncate_ = nullptr;
            }
        }
        open_ = false;
    }

    Status
    fetch (void const* key, std::shared_ptr<NodeObject>* pno) override
    {
        CassStatement* statement = cass_prepared_bind(select_);
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
        CassFuture* fut = cass_session_execute(session_.get(), statement);
        rc = cass_future_error_code(fut);
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

        /*
        pg_params params = {"SELECT value"
                            "  FROM objects"
                            " WHERE key = $1::bytea", {}};
        params.second.push_back("\\x" + strHex(static_cast<char const*>(key),
            static_cast<char const*>(key) + keyBytes_));
        auto res = pgQuery_->querySync(params);

        if (PQntuples(res.get()))
        {
            char const *pgValue = PQgetvalue(res.get(), 0, 0);
            if (strlen(pgValue) < 3 || strncmp("\\x", pgValue, 2))
            {
                pno->reset();
                return dataCorrupt;
            }

            char const* value = pgValue + 2;
            auto valueBlob = strUnHex(strlen(value), value,
                value + strlen(value));

            nudb::detail::buffer bf;
            std::pair<void const*, std::size_t> uncompressed =
                nodeobject_decompress(&valueBlob.get().at(0),
                    valueBlob->size(), bf);
            DecodedBlob decoded(key, uncompressed.first, uncompressed.second);

            if (!decoded.wasOk())
            {
                pno->reset();
                return dataCorrupt;
            }
            *pno = decoded.createObject();
            return ok;
        }
        else
        {
            pno->reset();
            return notFound;
        }
         */
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

    void
    store (std::shared_ptr <NodeObject> const& no) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
//        batch_.push_back({no, nullptr});
        JLOG(j_.debug()) << "store " << no->getHash();
        storeLocked(no);

//        pgQuery_->store(no, keyBytes_);
    }

    void
    storeBatch (Batch const& batch) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto const& no : batch)
        {
            //            batch_.push_back({no, nullptr});
            JLOG(j_.debug()) << "storeBatch " << no->getHash();
            storeLocked(no);
        }

//        pgQuery_->store(batch, keyBytes_);
    }

    void
    sync() override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        while(batches_.size())
        {
            auto batch = std::move(batches_.front());
            batches_.pop();
            lock.unlock();
            JLOG(j_.debug()) << "inserting batch size: " << batch.size();

            std::vector<std::pair<
                std::shared_ptr<NodeObject>, CassFuture*>> retry;
            // TODO This potentially never stops. If ETL is switched, then
            // this needs to check a flag that's set by ETL.
            while (batch.size())
            {
                for (auto& item : batch)
                {
                    auto const& no = item.first;
                    auto& fut = item.second;
                    NodeStore::EncodedBlob e;
                    e.prepare(no);
                    nudb::detail::buffer bf;
                    std::pair<void const*, std::size_t> compressed =
                        NodeStore::nodeobject_compress(e.getData(),
                            e.getSize(), bf);

                    CassStatement* statement = cass_prepared_bind(insert_);
                    CassError rc = cass_statement_bind_bytes(
                        statement, 0, static_cast<cass_byte_t const*>(
                                          e.getKey()), keyBytes_);
                    if (rc != CASS_OK)
                    {
                        cass_statement_free(statement);
                        std::stringstream ss;
                        ss << "Binding cassandra insert hash: " << rc
                           << ", " << cass_error_desc(rc);
                        Throw<std::runtime_error>(ss.str());
                    }
                    rc = cass_statement_bind_bytes(
                        statement, 1, static_cast<cass_byte_t const*>(
                            compressed.first), compressed.second);
                    if (rc != CASS_OK)
                    {
                        cass_statement_free(statement);
                        std::stringstream ss;
                        ss << "Binding cassandra insert object: " << rc
                           << ", " << cass_error_desc(rc);
                        Throw<std::runtime_error>(ss.str());
                    }
                    fut = cass_session_execute(session_.get(), statement);
                    cass_statement_free(statement);
                }

                for (auto& item : batch)
                {
                    auto const& no = item.first;
                    auto& fut = item.second;
                    CassError rc = cass_future_error_code(fut);
                    if (rc != CASS_OK)
                    {
                        JLOG(j_.error()) << "Cassandra insert error: " << rc
                            << ", " << cass_error_desc(rc)
                            << ". retrying " << no->getHash();
                        retry.push_back(item);
                    }
                    cass_future_free(fut);
                }
                if ((retry.size() + 0.0) / batch.size() > 0.1)
                {
                    JLOG(j_.debug()) << "sleeping 1s before retrying batch of "
                                     << retry.size() << " out of "
                                     << batch.size() << " inserts";
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
                batch = std::move(retry);
                retry = {};
            }

            lock.lock();
        }

//        pgQuery_->sync(keyBytes_);
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
        deletePath_ = true;
    }

    void
    verify() override
    {}

    int
    fdRequired() const override
    {
        return 0;
    }
};

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
        beast::Journal journal,
        std::shared_ptr<PgPool> pool) override
    {
        return std::make_unique <CassandraBackend> (
            keyBytes, keyValues, scheduler, journal, pool);
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
            keyBytes, keyValues, scheduler, context, journal);
    }
};

static CassandraFactory cassandraFactory;

}
}
