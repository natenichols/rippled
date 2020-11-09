#include<ripple/app/reporting/CassandraBackend.h>
#include<ripple/basics/strHex.h>

namespace ripple {
namespace NodeStore {

void
CassandraBackend::open(bool createIfMissing)
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
    bool setupSessionAndStateTable = false;
    while (!setupSessionAndStateTable)
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
            "CREATE TABLE IF NOT EXISTS stateTable ("
            "    hash   blob, "
            "    seq    bigint, "
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
            ss << "nodestore: Error creating Cassandra stateTable table: "
                << rc << ", " << cass_error_desc(rc);
            JLOG(j_.error()) << ss.str();
            continue;
        }

        statement = makeStatement("SELECT * FROM stateTable LIMIT 1", 0);
        fut = cass_session_execute(session_.get(), statement);
        rc = cass_future_error_code(fut);
        cass_future_free(fut);
        cass_statement_free(statement);
        if (rc != CASS_OK)
        {
            if (rc == CASS_ERROR_SERVER_INVALID_QUERY)
            {
                JLOG(j_.warn())
                    << "stateTable not here yet, sleeping 1s to "
                        "see if table creation propagates";
                continue;
            }
            else
            {
                std::stringstream ss;
                ss << "nodestore: Error checking for stateTable: " << rc
                    << ", " << cass_error_desc(rc);
                JLOG(j_.error()) << ss.str();
                continue;
            }
        }

        setupSessionAndStateTable = true;
    }

    bool setupTxTable = false;
    while (!setupTxTable)
    {
        statement = makeStatement(
            "CREATE TABLE IF NOT EXISTS txTable ("
            "    hash   blob, "
            "    seq    bigint, "
            "    transaction blob, "
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
            ss << "nodestore: Error creating Cassandra txTable: "
                << rc << ", " << cass_error_desc(rc);
            JLOG(j_.error()) << ss.str();
            continue;
        }

        statement = makeStatement("SELECT * FROM txTable LIMIT 1", 0);
        fut = cass_session_execute(session_.get(), statement);
        rc = cass_future_error_code(fut);
        cass_future_free(fut);
        cass_statement_free(statement);
        if (rc != CASS_OK)
        {
            if (rc == CASS_ERROR_SERVER_INVALID_QUERY)
            {
                JLOG(j_.warn())
                    << "txTable not here yet, sleeping 1s to "
                        "see if table creation propagates";
                continue;
            }
            else
            {
                std::stringstream ss;
                ss << "nodestore: Error checking for txTable: " << rc
                    << ", " << cass_error_desc(rc);
                JLOG(j_.error()) << ss.str();
                continue;
            }
        }

        setupTxTable = true;
    }

    cass_cluster_free(cluster);

    bool setupPreparedStatements = false;
    while (!setupPreparedStatements)
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        CassFuture* prepare_future = cass_session_prepare(
            session_.get(),
            "INSERT INTO txTable (hash, seq, transaction) VALUES (?, ?, ?)");

        /* Wait for the statement to prepare and get the result */
        rc = cass_future_error_code(prepare_future);

        if (rc != CASS_OK)
        {
            /* Handle error */
            cass_future_free(prepare_future);

            std::stringstream ss;
            ss << "nodestore: Error preparing insertTx : " << rc << ", "
                << cass_error_desc(rc);
            JLOG(j_.error()) << ss.str();
            continue;
        }

        /* Get the prepared object from the future */
        insertTx_ = cass_future_get_prepared(prepare_future);

        /* The future can be freed immediately after getting the prepared
            * object
            */
        cass_future_free(prepare_future);

        prepare_future = cass_session_prepare(
            session_.get(),
            "INSERT INTO stateTable (hash, seq, object) VALUES (?, ?, ?)");

        rc = cass_future_error_code(prepare_future);

        if (rc != CASS_OK)
        {
            cass_future_free(prepare_future);

            std::stringstream ss;
            ss << "nodestore: Error preparing insertEntry : " << rc << ", "
                << cass_error_desc(rc);
            JLOG(j_.error()) << ss.str();
            continue;
        }

        insertEntry_ = cass_future_get_prepared(prepare_future);

        cass_future_free(prepare_future);

        prepare_future = cass_session_prepare(
            session_.get(), "SELECT transaction FROM txTable WHERE hash = ? AND seq <= ? ORDER BY seq DESC LIMIT 1");

        rc = cass_future_error_code(prepare_future);

        if (rc != CASS_OK)
        {
            cass_future_free(prepare_future);

            std::stringstream ss;
            ss << "nodestore: Error preparing selectTx : " << rc << ", "
                << cass_error_desc(rc);
            JLOG(j_.error()) << ss.str();
            continue;
        }

        selectTx_ = cass_future_get_prepared(prepare_future);

        cass_future_free(prepare_future);

        prepare_future = cass_session_prepare(
            session_.get(), "SELECT object FROM stateTable WHERE hash = ? AND seq <= ? ORDER BY seq DESC LIMIT 1");

        rc = cass_future_error_code(prepare_future);

        if (rc != CASS_OK)
        {
            cass_future_free(prepare_future);

            std::stringstream ss;
            ss << "nodestore: Error preparing selectEntry : " << rc << ", "
                << cass_error_desc(rc);
            JLOG(j_.error()) << ss.str();
            continue;
        }

        selectEntry_ = cass_future_get_prepared(prepare_future);

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

void
CassandraBackend::close()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (insertTx_)
        {
            cass_prepared_free(insertTx_);
            insertTx_ = nullptr;
        }
        if(insertEntry_)
        {
            cass_prepared_free(insertEntry_);
            insertEntry_ = nullptr;
        }
        if (selectTx_)
        {
            cass_prepared_free(selectTx_);
            selectTx_ = nullptr;
        }
        if (selectEntry_)
        {
            cass_prepared_free(selectEntry_);
            selectEntry_ = nullptr;
        }
        work_.reset();
        if(ioThread_.joinable())
            ioThread_.join();
    }
    open_ = false;
}

Status
CassandraBackend::fetch(CassTable table, uint256 hash, uint32_t seq, std::shared_ptr<Blob>& obj)
{
    JLOG(j_.trace()) << "Fetching from cassandra";
    obj = nullptr;
    CassStatement* statement; 
    
    if(CassTable::TxTable == table)
        statement = cass_prepared_bind(selectTx_);
    else
        statement = cass_prepared_bind(selectEntry_);

    cass_statement_set_consistency(statement, CASS_CONSISTENCY_QUORUM);

    CassError rc = cass_statement_bind_bytes(
        statement, 0, static_cast<cass_byte_t const*>(hash.begin()), keyBytes_);
    if (rc != CASS_OK)
    {
        cass_statement_free(statement);
        JLOG(j_.error()) << "Binding Cassandra fetch key query: " << rc << ", "
                            << cass_error_desc(rc);
        return backendError;
    }

    rc = cass_statement_bind_int64(statement, 1, seq);
    if (rc != CASS_OK)
    {
        cass_statement_free(statement);
        JLOG(j_.error()) << "Binding Cassandra fetch seq query: " << rc << ", "
                            << cass_error_desc(rc);
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
        return notFound;
    }
    cass_byte_t const* buf;
    std::size_t bufSize;
    rc = cass_value_get_bytes(cass_row_get_column(row, 0), &buf, &bufSize);
    if (rc != CASS_OK)
    {
        cass_result_free(res);
        JLOG(j_.error()) << "Cassandra fetch result error: " << rc << ", "
                            << cass_error_desc(rc);
        ++counters_.readErrors;
        return backendError;
    }

    nudb::detail::buffer bf;
    auto [data, size] = lz4_decompress(buf, bufSize, bf);     
    auto slice = Slice(data, size);
    obj = std::make_shared<Blob>(slice.begin(), slice.end());

    cass_result_free(res);

    return ok;
}

std::vector<std::shared_ptr<Blob>>
CassandraBackend::fetchBatch(CassTable table, std::vector<uint256> hashes, uint32_t seq)
{
    std::vector<uint32_t> sequences(hashes.size());
    std::fill(sequences.begin(), sequences.end(), seq);
    return fetchBatch(table, hashes, sequences);
}

std::vector<std::shared_ptr<Blob>>
CassandraBackend::fetchBatch(CassTable table, std::vector<uint256> hashes, std::vector<uint32_t> sequences) 
{
    if(hashes.size() != sequences.size())
        Throw<std::runtime_error>("nodestoreHashes and sequenceNumbers must be of equal size");

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
            *this, table, hashes[i], sequences[i], results[i], cv, numFinished, n);
        read(*cbs[i]);
    }
    assert(results.size() == cbs.size());

    std::unique_lock<std::mutex> lck(mtx);
    cv.wait(lck, [&numFinished, &n]() { return numFinished == n; });

    JLOG(j_.trace()) << "Fetched " << n << " records from Cassandra";
    return results;
}

void
CassandraBackend::read(ReadCallbackData& data)
{
    CassStatement* statement;
    if(CassTable::TxTable == data.table)
        statement = cass_prepared_bind(selectTx_);
    else
        statement = cass_prepared_bind(selectEntry_);

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

    rc = cass_statement_bind_int64(statement, 1, data.seq);        
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

void
CassandraBackend::write(WriteCallbackData& data, bool isRetry)
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

    CassStatement* statement = nullptr;
    if(CassTable::TxTable == data.table)
        statement = cass_prepared_bind(insertTx_);
    else
        statement = cass_prepared_bind(insertEntry_);

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
    
    rc = cass_statement_bind_int64(statement, 1, data.seq);
    if (rc != CASS_OK)
    {
        cass_statement_free(statement);
        std::stringstream ss;
        ss << "Binding cassandra insert seq: " << rc << ", "
            << cass_error_desc(rc);
        JLOG(j_.error()) << __func__ << " : " << ss.str();
        Throw<std::runtime_error>(ss.str());
    }

    if (data.compressed.first)
    {
        rc = cass_statement_bind_bytes(
            statement,
            2,
            static_cast<cass_byte_t const*>(data.compressed.first),
            data.compressed.second);
    }
    else 
    {
        rc = cass_statement_bind_null(
            statement,
            2);
    }

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
CassandraBackend::store(CassTable table, uint256 hash, std::uint32_t seq, Blob object)
{
    // JLOG(j_.trace()) << "Writing to cassandra";
    WriteCallbackData* data =
        new WriteCallbackData(this, table, hash, seq, object, counters_.writeRetries);

    ++numRequestsOutstanding_;
    write(*data, false);
}

void
CassandraBackend::remove(CassTable table, uint256 hash, std::uint32_t seq)
{
    WriteCallbackData* data =
        new WriteCallbackData(this, table, hash, seq, std::nullopt, counters_.writeRetries);

    ++numRequestsOutstanding_;
    write(*data, false);
}

void
readCallback(CassFuture* fut, void* cbData)
{
    CassandraBackend::ReadCallbackData& requestParams =
        *static_cast<CassandraBackend::ReadCallbackData*>(cbData);

    CassError rc = cass_future_error_code(fut);

    if (rc != CASS_OK)
    {
        JLOG(requestParams.backend.j_.warn()) << "Cassandra fetch error : "
            << rc << " : " << cass_error_desc(rc) << " - retrying";
        // Retry right away. The only time the cluster should ever be overloaded
        // is when the very first ledger is being written in full (millions of
        // writes at once), during which no reads should be occurring. If reads
        // are timing out, the code/architecture should be modified to handle
        // greater read load, as opposed to just exponential backoff
        requestParams.backend.read(requestParams);
    }
    else
    {
        auto finish = [&requestParams]() {
            ++(requestParams.numFinished);
            if(requestParams.numFinished == requestParams.batchSize)
                requestParams.cv.notify_all();
        };
        CassResult const* res = cass_future_get_result(fut);

        CassRow const* row = cass_result_first_row(res);
        if (!row)
        {
            cass_result_free(res);
            JLOG(requestParams.backend.j_.error())
                << "Cassandra fetch get row error : " << rc << ", "
                << cass_error_desc(rc);
            finish();
            return;
        }
        cass_byte_t const* buf;
        std::size_t bufSize;
        rc = cass_value_get_bytes(cass_row_get_column(row, 0), &buf, &bufSize);
        if (rc != CASS_OK)
        {
            cass_result_free(res);
            JLOG(requestParams.backend.j_.error())
                << "Cassandra fetch get bytes error : " << rc << ", "
                << cass_error_desc(rc);
            ++requestParams.backend.counters_.readErrors;
            finish();
            return;
        }

        nudb::detail::buffer bf;
        auto [data, size] = lz4_decompress(buf, bufSize, bf);     

        cass_result_free(res);

        auto slice = Slice(data, size);
        requestParams.result = std::make_shared<Blob>(slice.begin(), slice.end());
        finish();
    }
}

void
writeCallback(CassFuture* fut, void* cbData)
{
    CassandraBackend::WriteCallbackData& requestParams =
        *static_cast<CassandraBackend::WriteCallbackData*>(cbData);
    CassandraBackend& backend = *requestParams.backend;
    auto rc = cass_future_error_code(fut);
    if (rc != CASS_OK)
    {
        JLOG(backend.j_.error())
            << "ERROR!!! Cassandra insert error: " << rc << ", "
            << cass_error_desc(rc) << ", retrying ";
        ++requestParams.totalWriteRetries;
        // exponential backoff with a max wait of 2^10 ms (about 1 second)
        auto wait = std::chrono::milliseconds(
            lround(std::pow(2, std::min(10u, requestParams.currentRetries))));
        ++requestParams.currentRetries;
        std::shared_ptr<boost::asio::steady_timer> timer =
            std::make_shared<boost::asio::steady_timer>(
                backend.ioContext_,
                    std::chrono::steady_clock::now() + wait);
        timer->async_wait([timer, &requestParams, &backend](
                              const boost::system::error_code& error) {
            backend.write(requestParams, true);
        });
    }
    else
    {
        backend.counters_.writeDurationUs +=
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - requestParams.begin)
                .count();
        --(backend.numRequestsOutstanding_);

        backend.throttleCv_.notify_all();
        if (backend.numRequestsOutstanding_ == 0)
            backend.syncCv_.notify_all();
        delete &requestParams;
    }
}

} //NodeStore
} //ripple