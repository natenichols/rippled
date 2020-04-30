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

#include <ripple/basics/Slice.h>
#include <ripple/protocol/digest.h>
#include <ripple/basics/contract.h>
#include <ripple/basics/strHex.h>
#include <ripple/basics/StringUtilities.h>
#include <ripple/beast/core/CurrentThreadName.h>
#include <ripple/core/Pg.h>
#include <ripple/nodestore/impl/codec.h>
#include <ripple/nodestore/impl/DecodedBlob.h>
#include <ripple/nodestore/impl/EncodedBlob.h>
#include <ripple/nodestore/NodeObject.h>
#include <nudb/nudb.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <functional>
#include <iterator>
#include <stdexcept>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace ripple {

static constexpr auto idle_sweep_timeout = std::chrono::seconds(5);

static void
noticeReceiver(void* arg, PGresult const* res)
{
    beast::Journal& j = *static_cast<beast::Journal*>(arg);
    JLOG(j.error()) << "server message: "
                         << PQresultErrorMessage(res);
}

/*
 Connecting described in:
 https://www.postgresql.org/docs/10/libpq-connect.html
 */
void
Pg::connect(yield_context yield, boost::asio::io_context::strand& strand)
{
    std::function <PostgresPollingStatusType (PGconn*)> poller;
    if (conn_)
    {
        if (PQstatus(conn_.get()) == CONNECTION_OK)
            return;
        /* Try resetting connection, or disconnect and retry if that fails.
           PQfinish() is synchronous so first try to asynchronously reset. */
        if (PQresetStart(conn_.get()))
            poller = PQresetPoll;
        else
            disconnect();
    }

    if (!conn_)
    {
        std::stringstream ss;
        ss << "conn ptrs: ";
        char** kidx = (char**)&config_.keywordsIdx[0];
        while (*kidx != nullptr)
        {
            ss << (char const*)kidx << ':';
            ++kidx;
        }
        char** vidx = (char**)&config_.valuesIdx[0];
        while (*vidx != nullptr)
        {
            ss << (char const*)vidx << ',';
            ++vidx;
        }
        std::cerr << ss.str() << '\n';
        ss.str(std::string());
        ss << "conn kvs (nelem " << config_.keywords.size() << "): ";
        for (std::size_t n = 0; n < config_.keywords.size(); ++n)
        {
            ss << config_.keywords[n] << ':' << config_.values[n] << ',';
        }
        std::cerr << ss.str() << '\n';
        conn_.reset(PQconnectStartParams(
            reinterpret_cast<char const* const*>(&config_.keywordsIdx[0]),
            reinterpret_cast<char const* const*>(&config_.valuesIdx[0]),
            0));
        poller = PQconnectPoll;
    }

    if (!conn_)
        Throw<std::runtime_error>("No db connection object");

    if (PQstatus(conn_.get()) == CONNECTION_BAD)
    {
        std::stringstream ss;
        ss << "DB connection status " << PQstatus(conn_.get()) << ": "
            << PQerrorMessage(conn_.get());
        Throw<std::runtime_error>(ss.str());
    }
//    noticeReceiver(const_cast<beast::Journal*>(&j_), nullptr);
    PQsetNoticeReceiver(conn_.get(), noticeReceiver,
        const_cast<beast::Journal*>(&j_));

    /* Asynchronously connecting entails several messages between
     * client and server. */
    PostgresPollingStatusType poll = PGRES_POLLING_WRITING;
    while (poll != PGRES_POLLING_OK)
    {
        socket_ = std::make_unique<boost::asio::ip::tcp::socket>(strand,
            config_.protocol, PQsocket(conn_.get()));

        switch (poll)
        {
            case PGRES_POLLING_FAILED:
            {
                std::stringstream ss;
                ss << "DB connection failed" ;
                char* err = PQerrorMessage(conn_.get());
                if (err)
                    ss << ":" <<  err;
                else
                    ss << '.';
                Throw<std::runtime_error>(ss.str());
            }

            case PGRES_POLLING_READING:
                socket_->async_wait(boost::asio::ip::tcp::socket::wait_read,
                    yield);
                break;

            case PGRES_POLLING_WRITING:
                socket_->async_wait(boost::asio::ip::tcp::socket::wait_write,
                    yield);
                break;

            default:
            {
                assert(false);
                std::stringstream ss;
                ss << "unknown DB polling status: " << poll;
                Throw<std::runtime_error>(ss.str());
            }
        }
        poll = poller(conn_.get());
        socket_->release();
    }

    /* Enable asynchronous writes. */
    if (PQsetnonblocking(conn_.get(), 1) == -1)
    {
        std::stringstream ss;
        char* err = PQerrorMessage(conn_.get());
        if (err)
            ss << "Error setting connection to non-blocking: " << err;
        else
            ss << "Unknown error setting connection to non-blocking";
        Throw<std::runtime_error>(ss.str());
    }

    if (PQstatus(conn_.get()) != CONNECTION_OK)
    {
        std::stringstream ss;
        ss << "bad connection" << std::to_string(PQstatus(conn_.get()));
        char* err = PQerrorMessage(conn_.get());
        if (err)
            ss << ": " << err;
        else
            ss << '.';
        Throw<std::runtime_error>(ss.str());
    }
}

pg_result_type
Pg::query(yield_context yield, boost::asio::io_context::strand& strand,
    char const* command, std::size_t nParams, char const* const* values)
{
    pg_result_type ret {nullptr, [](PGresult* result){ PQclear(result); }};
    // Connect then submit query.
    try
    {
        connect(yield, strand);
        if (!PQsendQueryParams(conn_.get(), command, nParams, nullptr,
            values, nullptr, nullptr, 0))
        {
            std::stringstream ss;
            ss << "Can't send query: " << PQerrorMessage(conn_.get());
            Throw<std::runtime_error>(ss.str());
        }

        socket_ = std::make_unique<boost::asio::ip::tcp::socket>(strand,
            config_.protocol, PQsocket(conn_.get()));

        // non-blocking connection requires manually flushing write.
        int flushed;
        do
        {
            flushed = PQflush(conn_.get());
            if (flushed == 1)
            {
                socket_->async_wait(
                    boost::asio::ip::tcp::socket::wait_write, yield);
            }
            else if (flushed == -1)
            {
                std::stringstream ss;
                ss << "error flushing query " << PQerrorMessage(conn_.get());
                Throw<std::runtime_error>(ss.str());
            }
        }
        while (flushed);

        /* Only read response if query was submitted successfully.
           Only a single response is expected, but the API requires
           responses to be read until nullptr is returned.

           It is possible for pending reads on the connection to interfere
           with the current query. For simplicity, this implementation
           only flushes pending writes and assumes there are no pending reads.
           To avoid this, all pending reads from each query must be consumed,
           and all connections with any type of error be severed. */
        while (true)
        {
            if (PQisBusy(conn_.get()))
                socket_->async_wait(boost::asio::ip::tcp::socket::wait_read,
                    yield);
            if (!PQconsumeInput(conn_.get()))
            {
                std::stringstream ss;
                ss << "query consume input error: "
                   << PQerrorMessage(conn_.get());
                Throw<std::runtime_error>(ss.str());
            }
            if (PQisBusy(conn_.get()))
                continue;
            pg_result_type res{PQgetResult(conn_.get()),
                               [](PGresult *result)
                               { PQclear(result); }};
            if (!res)
                break;

            JLOG(j_.debug()) << "Pg::query looping - "
                << "res = " << PQresultStatus(res.get())
                << " error_msg = " << PQerrorMessage(conn_.get());

            /*
            if (ret)
            {
                Throw<std::runtime_error>("multiple results returned");
            }
            */
            ret.reset(res.release());
            //Seems that ret is never null in this case, so need to break
            if(PQresultStatus(ret.get()) == PGRES_COPY_IN)
                break;
        }

        socket_->release();
    }
    catch (std::exception const& e)
    {
        // Sever connection upon any error.
        disconnect();
        socket_.release();
        std::stringstream ss;
        ss << "query error: " << e.what();
        Throw<std::runtime_error>(ss.str());
    }

    if (!ret)
        Throw<std::runtime_error>("no result structure returned");

    // Ensure proper query execution.
    if (! (PQresultStatus(ret.get()) == PGRES_TUPLES_OK
        || PQresultStatus(ret.get()) == PGRES_COMMAND_OK
        || PQresultStatus(ret.get()) == PGRES_COPY_IN))
    {
        std::stringstream ss;
        ss << "bad query result: "
           << PQresStatus(PQresultStatus(ret.get()))
           << ", number of tuples: "
           << PQntuples(ret.get())
           << ", number of fields: "
           << PQnfields(ret.get());
        Throw<std::runtime_error>(ss.str().c_str());
    }

    return ret;
}

pg_result_type
Pg::batchQuery(yield_context yield, boost::asio::io_context::strand& strand,
    char const* command)
{
    pg_result_type ret{nullptr, [](PGresult *result)
    { PQclear(result); }};
    // Connect then submit query.
    try
    {
        connect(yield, strand);
        ret.reset(PQexec(conn_.get(), command));
    }
    catch (std::exception const& e)
    {
        // Sever connection upon any error.
        disconnect();
        socket_.release();
        std::stringstream ss;
        ss << "batch query error: " << e.what();
        Throw<std::runtime_error>(ss.str());
    }
    return ret;
}

static pg_formatted_params
formatParams(pg_params const& dbParams, beast::Journal const j)
{
    std::vector<std::optional<std::string>> const& values = dbParams.second;
    /* Convert vector to C-style array of C-strings for postgres API.
       std::nullopt is a proxy for NULL since an empty std::string is
       0 length but not NULL. */
    std::vector<char const*> valuesIdx;
    valuesIdx.reserve(values.size());
    std::stringstream ss;
    bool first = true;
    for (auto const& value : values)
    {
        if (value)
        {
            valuesIdx.push_back(value->c_str());
            ss << value->c_str();
        }
        else
        {
            valuesIdx.push_back(nullptr);
            ss << "(null)";
        }
        if (first)
            first = false;
        else
            ss << ',';
    }

    JLOG(j.trace()) << "query: " << dbParams.first << ". params: " << ss.str();
    return valuesIdx;
}

pg_result_type
Pg::query(yield_context yield, boost::asio::io_context::strand& strand,
    pg_params const& dbParams)
{
    char const* const& command = dbParams.first;
    auto const formattedParams = formatParams(dbParams, j_);
    return query(yield, strand, command, formattedParams.size(),
        formattedParams.size() ?
            reinterpret_cast<char const* const*>(&formattedParams[0]) :
                nullptr);
}

void
Pg::timeout(boost::system::error_code const& ec)
{
    if (ec == boost::asio::error::operation_aborted)
        return;
    if (socket_)
        try { socket_->cancel(); } catch (...) {}
}

//-----------------------------------------------------------------------------

PgPool::PgPool(Section const& network_db_config, beast::Journal const j)
    : timer_ (io_)
    , j_ (j)
{
    /*
    Connect to postgres to create low level connection parameters
    with optional caching of network address info for subsequent connections.
    See https://www.postgresql.org/docs/10/libpq-connect.html

    For bounds checking of postgres connection data received from
    the network: the largest size for any connection field in
    PG source code is 64 bytes as of 5/2019. There are 29 fields.
    */
    constexpr std::size_t maxFieldSize = 1024;
    constexpr std::size_t maxFields = 1000;

    // PostgreSQL connection
    pg_connection_type conn(
        PQconnectdb(get<std::string>(network_db_config, "conninfo").c_str()),
        [](PGconn* conn){PQfinish(conn);});
    if (! conn)
        Throw<std::runtime_error>("Can't create DB connection.");
    if (PQstatus(conn.get()) != CONNECTION_OK)
        Throw<std::runtime_error>("Initial DB connection failed.");

    int const sockfd = PQsocket(conn.get());
    if (sockfd == -1)
        Throw<std::runtime_error>("No DB socket is open.");
    struct sockaddr_storage addr;
    socklen_t len = sizeof(addr);
    if (getpeername(
        sockfd, reinterpret_cast<struct sockaddr*>(&addr), &len) == -1)
    {
        Throw<std::system_error>(errno, std::generic_category(),
            "Can't get server address info.");
    }
    // Ensure connection is not over domain socket.
    if (addr.ss_family != AF_INET && addr.ss_family != AF_INET6)
        Throw<std::runtime_error>("DB connection must be either IPv4 or IPv6.");

    // Set "port" and "hostaddr" if we're caching it.
    bool const remember_ip = get(network_db_config, "remember_ip", true);

    if (remember_ip)
    {
        config_.keywords.push_back("port");
        config_.keywords.push_back("hostaddr");
        std::string port;
        std::string hostaddr;

        if (addr.ss_family == AF_INET)
        {
            hostaddr.assign(INET_ADDRSTRLEN, '\0');
            struct sockaddr_in const &ainfo =
                reinterpret_cast<struct sockaddr_in &>(addr);
            port = std::to_string(ntohs(ainfo.sin_port));
            if (!inet_ntop(AF_INET, &ainfo.sin_addr,
                &hostaddr[0],
                hostaddr.size()))
            {
                Throw<std::system_error>(errno, std::generic_category(),
                    "Can't get IPv4 address string.");
            }
        }
        else if (addr.ss_family == AF_INET6)
        {
            hostaddr.assign(INET6_ADDRSTRLEN, '\0');
            config_.protocol = boost::asio::ip::tcp::v6();
            struct sockaddr_in6 const &ainfo =
                reinterpret_cast<struct sockaddr_in6 &>(addr);
            port = std::to_string(ntohs(ainfo.sin6_port));
            if (! inet_ntop(AF_INET6, &ainfo.sin6_addr,
                &hostaddr[0],
                hostaddr.size()))
            {
                Throw<std::system_error>(errno, std::generic_category(),
                    "Can't get IPv6 address string.");
            }
        }

        config_.values.push_back(port.c_str());
        config_.values.push_back(hostaddr.c_str());
    }
    std::unique_ptr<PQconninfoOption, void(*)(PQconninfoOption*)> connOptions(
        PQconninfo(conn.get()),
        [](PQconninfoOption* opts){PQconninfoFree(opts);});
    if (! connOptions)
        Throw<std::runtime_error>("Can't get DB connection options.");

    std::size_t nfields = 0;
    for (PQconninfoOption* option = connOptions.get();
         option->keyword != nullptr; ++option)
    {
        if (++nfields > maxFields)
        {
            std::stringstream ss;
            ss << "DB returned connection options with > " << maxFields
               << " fields.";
            Throw<std::runtime_error>(ss.str());
        }

        if (! option->val
            || (remember_ip
                && (! strcmp(option->keyword, "hostaddr")
                    || ! strcmp(option->keyword, "port"))))
        {
            continue;
        }

        if (strlen(option->keyword) > maxFieldSize
            || strlen(option->val) > maxFieldSize)
        {
            std::stringstream ss;
            ss << "DB returned a connection option name or value with\n";
            ss << "excessive size (>" << maxFieldSize << " bytes).\n";
            ss << "option (possibly truncated): " <<
               std::string_view(option->keyword,
                   std::min(strlen(option->keyword), maxFieldSize))
               << '\n';
            ss << " value (possibly truncated): " <<
               std::string_view(option->val,
                   std::min(strlen(option->val), maxFieldSize));
            Throw<std::runtime_error>(ss.str());
        }
        config_.keywords.push_back(option->keyword);
        config_.values.push_back(option->val);
    }

    config_.keywordsIdx.reserve(config_.keywords.size() + 1);
    config_.valuesIdx.reserve(config_.values.size() + 1);
    for (std::size_t n = 0; n < config_.keywords.size(); ++n)
    {
        config_.keywordsIdx.push_back(config_.keywords[n].c_str());
        config_.valuesIdx.push_back(config_.values[n].c_str());
    }
    config_.keywordsIdx.push_back(nullptr);
    config_.valuesIdx.push_back(nullptr);

    get_if_exists(network_db_config, "max_connections",
        config_.max_connections);
    std::size_t timeout;
    if (get_if_exists(network_db_config, "timeout", timeout))
        config_.timeout = std::chrono::seconds(timeout);
}

void
PgPool::setup()
{
    {
        std::stringstream ss;
        ss << "max_connections: " << config_.max_connections << ", "
           << "timeout: " << config_.timeout.count() << ", "
           << "protocol: " <<
           (config_.protocol == boost::asio::ip::tcp::v4() ?
            "ipv4" : "ipv6")
           << ". connection params: ";
        bool first = true;
        for (std::size_t i = 0; i < config_.keywords.size(); ++i)
        {
            if (first)
                first = false;
            else
                ss << ", ";
            ss << config_.keywords[i] << ": "
               << (config_.keywords[i] == "password" ? "*" :
                   config_.values[i]);
        }
        JLOG(j_.debug()) << ss.str();
    }

    // Start async job before launching threads, or else io_ won't
    // have anything to run.
    timer_.expires_from_now(idle_sweep_timeout);
    timer_.async_wait(
        std::bind(&PgPool::sweepIdle, shared_from_this(),
            std::placeholders::_1));

    JLOG(j_.info()) << "Starting worker threads: " << nWorkers_;
    /** Re-spawn worker threads upon exception. For example, asio spawn()
     * can throw exceptions that aren't catchable by the caller of spawn(),
     * but propagate up to the thread.
     */
    for (std::size_t n = 0; n < nWorkers_; ++n)
    {
        workers_.push_back(std::thread([n, this]()
        {
            while (true)
            {
                try
                {
                    std::stringstream ss;
                    ss << "pgpool #" << n;
                    beast::setCurrentThreadName(ss.str());
                    io_.run();
                    break;
                }
                catch (std::system_error const& e)
                {
                    JLOG(j_.error()) << "system error in worker " << n
                        << ": " << e.what() << ", " << e.code();
                    assert(false);
                }
                catch (std::exception const &e)
                {
                    JLOG(j_.error()) << "exception in worker " << n << ": "
                                     << e.what();
                    assert(false);
                }
                catch (...)
                {
                    JLOG(j_.error())
                        << "unknown exception in worker " << n;
                    assert(false);
                }
            }
        }));
    }
}

void
PgPool::stop()
{
    stop_ = true;
    try { timer_.cancel(); } catch (...) {}
    std::lock_guard<std::mutex> lock(mutex_);
    idle_.clear();
}

void
PgPool::sweepIdle(boost::system::error_code const& ec)
{
    if (ec == boost::asio::error::operation_aborted)
        return;

    std::size_t before, after;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        before = idle_.size();
        if (config_.timeout != std::chrono::seconds(0))
        {
            auto const found = idle_.upper_bound(
                clock_type::now() - config_.timeout);
            for (auto it = idle_.begin(); it != found; ++it)
            {
                idle_.erase(it);
                --connections_;
            }
        }
        after = idle_.size();
    }

    JLOG(j_.info()) << "Idle sweeper. connections: " << connections_
                         << ". checked out: " << connections_ - after
                         << ". idle before, after sweep: "
                         << before << ", " << after;

    timer_.expires_from_now(idle_sweep_timeout);
    timer_.async_wait(
        std::bind(&PgPool::sweepIdle, shared_from_this(),
            std::placeholders::_1));
}

std::shared_ptr<Pg>
PgPool::checkout()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (idle_.size())
        {
            auto entry = idle_.rbegin();
            auto ret = entry->second;
            idle_.erase(std::next(entry).base());
            return ret;
        }
        else if (connections_ < config_.max_connections && ! stop_)
        {
            ++connections_;
            return std::make_shared<Pg>(config_, j_);
        }
    }
    return {};
}

void
PgPool::checkin(std::shared_ptr<Pg>& pg)
{
    if (!pg)
        return;
    pg->cancel();

    std::lock_guard<std::mutex> lock(mutex_);
    if (stop_ || ! *pg)
    {
        --connections_;
        pg.reset();
    }
    else
    {
        // place in connected pool
        idle_.emplace(clock_type::now(), pg);
    }
}

//-----------------------------------------------------------------------------

pg_result_type
PgQuery::querySync(pg_params const& dbParams, std::shared_ptr<Pg>& conn)
{
    auto self(shared_from_this());
    boost::asio::io_context::strand strand(pool_->io_);
    std::mutex mtx;
    std::condition_variable cv;
    bool finished = false;
    pg_result_type result {nullptr, [](PGresult* result){ PQclear(result); }};
    boost::asio::spawn(strand,
        [this, self, &conn, &strand, &dbParams, &mtx, &cv, &finished, &result]
        (boost::asio::yield_context yield)
        {
            while (true)
            {
                try
                {
                    // TODO make something with a condition var instead of spinning.
                    while (!conn)
                        conn = pool_->checkout();
                    result = conn->query(yield, strand, dbParams);
                    pool_->checkin(conn);
                    break;
                }
                catch (std::exception const &e)
                {
                    try { pool_->checkin(conn); } catch (...) {}
                    JLOG(pool_->j_.error()) << "lambda query exception: "
                                                  << e.what();
                }
                catch (...)
                {
                    JLOG(pool_->j_.debug()) << "unknown lambda query "
                                               "exception.";

                }
            }

            try
            {
                std::unique_lock<std::mutex> lambdaLock(mtx);
                finished = true;
                cv.notify_one();
            }
            catch (std::exception const& e)
            {
                assert(false);
            }
        }
    );

    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [&finished]() { return finished; });

    return result;
}

void
PgQuery::store(std::size_t const keyBytes)
{
    {
        std::lock_guard<std::mutex> submitLock(submitMutex_);
        if (submitting_)
            return;
        submitting_ = true;
    }

    static std::atomic<std::size_t> counter = 0;
    std::shared_ptr<PgQuery> self(shared_from_this());
    std::shared_ptr<boost::asio::io_context::strand> strand =
        std::make_shared<boost::asio::io_context::strand>(pool_->io_);
    boost::asio::spawn(*strand, [this, self, strand, keyBytes]
        (boost::asio::yield_context yield)
        {
            std::unique_lock<std::mutex> lock(batchMutex_);
            while (batch_.size())
            {
                std::vector<std::shared_ptr<NodeObject>> batch;
                batch.swap(batch_);
                lock.unlock();
                JLOG(pool_->j_.debug()) << "store batch size " << batch.size();

                std::shared_ptr<Pg> conn;
                try
                {
                    // TODO make something with a condition var instead of spinning.
                    while (!conn)
                        conn = pool_->checkout();

                    std::stringstream command;
                    for (auto const& no : batch)
                    {
                        NodeStore::EncodedBlob e;
                        e.prepare(no);
                        nudb::detail::buffer bf;
                        std::pair<void const*, std::size_t> compressed =
                            NodeStore::nodeobject_compress(e.getData(),
                                e.getSize(), bf);

                        command << "INSERT INTO objects VALUES ('\\x"
                            << strHex(static_cast<char const*>(e.getKey()),
                                static_cast<char const*>(e.getKey()) + keyBytes)
                            << "', '\\x"
                            << strHex(
                                static_cast<char const*>(compressed.first),
                                static_cast<char const*>(compressed.first) +
                                compressed.second)
                            << "') ON CONFLICT DO NOTHING; ";
                    }
                    JLOG(pool_->j_.debug()) << "store batch before";
                    conn->batchQuery(yield, *strand, command.str().c_str());
                    JLOG(pool_->j_.debug()) << "store batch after";
                    counter += batch.size();
                    JLOG(pool_->j_.debug()) << "store batch counter "
                        << counter;
                }
                catch (std::exception const& e)
                {
                    JLOG(pool_->j_.error()) << "store exception: "
                        << e.what() << '\n';
                    lock.lock();
                    batch_.insert(batch_.end(), batch.begin(),
                        batch.end());
                    lock.unlock();
                    try
                    {
                        if (conn)
                            conn->query(yield, *strand, "ROLLBACK");
                    }
                    catch (...)
                    {}
                }
                pool_->checkin(conn);
                lock.lock();
            }

            {
                std::lock_guard<std::mutex> submitLock(submitMutex_);
                submitting_ = false;
            }
        }
    );
}

void
PgQuery::store(std::shared_ptr<NodeObject> const& no, size_t const keyBytes)
{
    {
        std::lock_guard<std::mutex> lock_(batchMutex_);
        batch_.push_back(no);
    }
    store(keyBytes);
}

void
PgQuery::store(std::vector<std::shared_ptr<NodeObject>> const& nos,
    size_t const keyBytes)
{
    {
        std::lock_guard<std::mutex> lock(batchMutex_);
        batch_.insert(batch_.end(), nos.begin(), nos.end());
    }
    store(keyBytes);
}

std::pair<std::shared_ptr<Pg>, std::optional<LedgerIndex>>
PgQuery::lockLedger(std::optional<LedgerIndex> seq)
{
    auto self(shared_from_this());
    boost::asio::io_context::strand strand(pool_->io_);
    std::mutex mtx;
    std::condition_variable cv;
    bool finished = false;
    pg_result_type result {nullptr, [](PGresult* result){ PQclear(result); }};
    std::shared_ptr<Pg> conn;
    std::optional<LedgerIndex> locked;

    boost::asio::spawn(strand,
        [this, self, &seq, &strand, &mtx, &cv, &finished, &result, &conn,
         &locked]
        (boost::asio::yield_context yield)
        {
          try
          {
              // TODO make something with a condition var instead of spinning.
              while (!conn)
                  conn = pool_->checkout();

              std::stringstream cmd;
              cmd << "BEGIN;"
                     "SELECT ledger_seq"
                     "  FROM ledgers"
                     " WHERE ledger_seq = ";
              if (seq.has_value())
                  cmd << std::to_string(*seq);
              else
                  cmd << "min_ledger()";
              cmd << " FOR UPDATE";
              result = conn->batchQuery(yield, strand, cmd.str().c_str());

              if (result && PQntuples(result.get())
                  && ! PQgetisnull(result.get(), 0, 0))
              {
                  locked = std::atoll(PQgetvalue(result.get(), 0, 0));
              }
          }
          catch (std::exception const& e)
          {
              JLOG(pool_->j_.error()) << "lockLedger exception: "
                                      << e.what() << '\n';
          }

          if (! locked.has_value())
          {
              try
              {
                  if (conn)
                  {
                      conn->query(yield, strand, "ROLLBACK");
                      conn.reset();
                  }
              }
              catch (...) {}
          }

          std::unique_lock<std::mutex> lambdaLock(mtx);
          finished = true;
          cv.notify_one();
        });

    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [&finished]() { return finished; });

    return {conn, locked};
}


//-----------------------------------------------------------------------------

std::shared_ptr<PgPool>
make_PgPool(Section const& network_db_config, beast::Journal const j)
{
    if (network_db_config.empty())
    {
        return {};
    }
    else
    {
        auto ret = std::make_shared<PgPool>(network_db_config, j);
        ret->setup();
        return ret;
    }
}

std::optional<std::pair<std::string, std::string>>
no2pg(std::shared_ptr<NodeObject> const& no)
{
    if (!no)
        return {};
    std::pair<std::string, std::string> ret;
    NodeStore::EncodedBlob e;
    e.prepare(no);
    ret.first = "\\x" + std::string(static_cast<const char*>(e.getKey()),
        no->keyBytes);
    ret.second = "\\x" + std::string(static_cast<const char*>(e.getData()),
        e.getSize());
    return ret;
}

bool
testNo(std::shared_ptr<NodeObject> const& no, std::size_t const keyBytes)
{
    uint256 origHash = sha512Half(makeSlice(no->getData()));
    NodeStore::EncodedBlob e;
    e.prepare(no);
    std::string const pgKey("\\x" + strHex(
        static_cast<char const*>(e.getKey()),
        static_cast<char const*>(e.getKey()) + keyBytes));
    std::string const pgValue("\\x" + strHex(
        static_cast<char const*>(e.getData()),
        static_cast<char const*>(e.getData()) +
        e.getSize()));

    char const* pgValueCstr = pgValue.c_str() + 2;
    auto pgValueBlob = strUnHex(strlen(pgValueCstr), pgValueCstr, pgValueCstr +
        strlen(pgValueCstr));
    NodeStore::DecodedBlob decoded(e.getKey(), &pgValueBlob.get().at(0),
        pgValueBlob->size());
    if (!decoded.wasOk())
    {
        std::cerr << "testNo !decoded.wasOk\n";
        return false;
    }
    std::shared_ptr<NodeObject> resNo = decoded.createObject();
    uint256 const resHash = sha512Half(makeSlice(resNo->getData()));

    std::cerr << "testNo origkey orighash pgkey reshash "
        << no->getHash() << " "
        << origHash << " "
        << pgKey << " "
        << resHash << '\n';

    return no->getHash() == origHash && origHash == resHash;
}

} // ripple
