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

#ifndef RIPPLE_CORE_PG_H_INCLUDED
#define RIPPLE_CORE_PG_H_INCLUDED

#include <libpq-fe.h>
#include <ripple/basics/BasicConfig.h>
#include <ripple/basics/Log.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/io_context_strand.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/system/error_code.hpp>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace ripple {

// These postgres structs must be freed only by the postges API.
using pg_result_type = std::unique_ptr<PGresult, void(*)(PGresult*)>;
using pg_connection_type = std::unique_ptr<PGconn, void(*)(PGconn*)>;

/** first: command
 * second: parameter values
 *
 * The 2nd member takes an optional string is to
 * distinguish between NULL parameters and empty strings. An empty
 * item corresponds to a NULL parameter.
 *
 * Postgres reads each parameter as a c-string, regardless of actual type.
 * Binary types (bytea) need to be converted to hex and prepended with
 * \x ("\\x").
 */
using pg_params = std::pair<char const*,
    std::vector<std::optional<std::string>>>;

/* parameter values for pg API. */
using pg_formatted_params = std::vector<char const*>;

/** Parameters for managing postgres connections. */
struct PgConfig
{
    /** Maximum connections allowed to db. */
    std::size_t max_connections {std::numeric_limits<std::size_t>::max()};
    /** Close idle connections past this duration. */
    std::chrono::seconds timeout {600};

    /** Index of DB connection parameter names. */
    std::vector<char const*> keywordsIdx;
    /** DB connection parameter names. */
    std::vector<std::string> keywords;
    /** Index of DB connection parameter values. */
    std::vector<char const*> valuesIdx;
    /** DB connection parameter values. */
    std::vector<std::string> values;

    /** IP type. The class requires initialization. */
    boost::asio::ip::tcp protocol {boost::asio::ip::tcp::v4()};
};

//-----------------------------------------------------------------------------

/* Class that contains and operates upon a postgres connection. */
class Pg
    : public std::enable_shared_from_this<Pg>
{
    using yield_context = boost::asio::yield_context;
    PgConfig const& config_;
    beast::Journal const j_;

    pg_connection_type conn_ {
        nullptr, [](PGconn* conn){PQfinish(conn);}};
    std::unique_ptr<boost::asio::ip::tcp::socket> socket_;

public:
    /** Constructor for Pg class.
     *
     * @param dbConfig Config parameters.
     */
//     Pg() {}
    Pg(PgConfig const& config, beast::Journal const j)
        : config_ (config)
        , j_ (j)
    {}

    /** Whether the database connection has been established.
     *
     * @return Whether the database connection has been established.
     */
    operator bool() const
    {
        return conn_ != nullptr;
    }

    /** Asynchronously connect to postgres.
     *
     * Idempotently connects to postgres by first checking whether an
     * existing connection is already present. If connection is not present
     * or in an errored state, reconnects to the database.
     *
     * @param yield Asio coroutine state.
     * @param strand Strand upon which to perform asynchronous operations.
     */
    void connect(yield_context yield, boost::asio::io_context::strand& strand);

    /** Disconnect from postgres. */
    void
    disconnect()
    {
        conn_.reset();
    }

    /** Cancel asynchronous operations on underlying postgres socket. */
    void
    cancel()
    {
        if (socket_)
            try { socket_->cancel(); } catch (...) {}
    }

    /** Execute postgres query.
     *
     * See: https://www.postgresql.org/docs/10/libpq-async.html
     *
     * The API supports multiple response objects per command but this
     * implementation only returns 0 or 1 response objects.
     *
     * @param yield Asio coroutine state.
     * @param strand Asio strand upon which to execute asynchronous ops.
     * @param command postgres API command string.
     * @param nParams postgres API number of parameters.
     * @param values postgres API array of parameter.
     * @return Postgres API result struct if successful.
     */
    pg_result_type
    query(yield_context yield, boost::asio::io_context::strand& strand,
        char const* command, std::size_t nParams, char const* const* values);

    /** Execute postgres query with no parameters.
     *
     * @param yield Asio coroutine state.
     * @param strand Asio strand upon which to execute asynchronous ops.
     * @return Postgres API result struct.
     */
    pg_result_type
    query(yield_context yield, boost::asio::io_context::strand& strand,
        char const* command)
    {
        return query(yield, strand, command, 0, nullptr);
    }

    /** Execute postgres query with parameters.
     *
     * @param yield Asio coroutine state.
     * @param strand Asio strand upon which to execute asynchronous ops.
     * @param dbParams Database command and parameter values.
     * @return PostgreSQL API result struct.
     */
    pg_result_type
    query(yield_context yield, boost::asio::io_context::strand& strand,
        pg_params const& dbParams);

    pg_result_type
    batchQuery(yield_context yield, boost::asio::io_context::strand& strand,
        char const* command);

    /** Handler for timeout of database activities.
     *
     * @param ec Asio error.
     */
    void timeout(boost::system::error_code const& ec);
};

//-----------------------------------------------------------------------------

/** Database connection pool.
 *
 * Allow re-use of postgres connections. Postgres connections are created
 * as needed until configurable limit is reached. After use, each connection
 * is placed in a container ordered by time of use. Each request for
 * a connection grabs the most recently used connection from the container.
 * If none are available, a new connection is used (up to configured limit).
 * Idle connections are destroyed periodically after configurable
 * timeout duration.
 */
class PgPool
    : public std::enable_shared_from_this<PgPool>
{
    friend class PgQuery;
    using clock_type = std::chrono::steady_clock;

    // TODO the asio worker threads and io context need to be external for GA.
    boost::asio::io_context io_;
    boost::asio::steady_timer timer_ {io_};
    std::multimap<std::chrono::time_point<clock_type>,
        std::shared_ptr<Pg>> idle_;

public:
    beast::Journal const j_;
    std::mutex mutex_;
    std::size_t connections_ {};
    std::atomic<bool> stop_ {false};

    // TODO asio needs to be external for GA.
    std::vector<std::thread> workers_;
    // TODO make configurable.
    unsigned int nWorkers_ {std::thread::hardware_concurrency()};

    PgConfig config_;

    /** Disconnect idle postgres connections.
     *
     * @param ec Asio error code.
     */
    void sweepIdle(boost::system::error_code const& ec);

public:
    /** Connection pool constructor.
     *
     * @param io Asio io service.
     * @param dbConfig Config params.
     */
    PgPool(Section const& network_db_config,
        beast::Journal const j);

    /** Initiate idle connection timer.
     *
     * The PgPool object needs to be fully constructed to support asynchronous
     * operations.
     */
    void setup();

    /** Prepare for process shutdown. */
    void stop();

    /** Get a postgres connection object.
     *
     * @return Postgres object if any are available.
     */
    std::shared_ptr<Pg> checkout();

    /** Return a postgres object to be reused.
     *
     * Cancel any pending asynchronous operations on database connection.
     * Also set it up so that the connection can be severed if idle too long.
     * If shutting down, don't make object available for re-use nor set
     * for idle timeout.
     *
     * @param pg Pg object.
     */
    void checkin(std::shared_ptr<Pg>& pg);

};

//-----------------------------------------------------------------------------

class NodeObject;

/** Class to query postgres both synchronously and asynchronously. */
class PgQuery
    : public std::enable_shared_from_this<PgQuery>
{
private:
    std::shared_ptr<PgPool> pool_;
    std::vector<std::shared_ptr<NodeObject>> batch_;
    std::mutex batchMutex_;
    bool submitting_ {false};
    std::mutex submitMutex_;

    void store(std::size_t const keyBytes);

public:
    PgQuery(std::shared_ptr<PgPool>& pool)
        : pool_ (pool)
    {}

    /** Synchronously execute postgres query with parameters.
     *
     * Retries until a connection is available. Throws database and
     * connection errors.
     *
     * @param dbParams
     * @return PostgreSQL API result struct.
     */
    pg_result_type querySync(pg_params const& dbParams);
    pg_result_type querySync(char const* command);
    void store(std::shared_ptr<NodeObject> const& no, size_t const keyBytes);
    void store(std::vector<std::shared_ptr<NodeObject>> const& nos,
        size_t const keyBytes);

};

//-----------------------------------------------------------------------------

std::shared_ptr<PgPool> make_PgPool(Section const& network_db_config,
    beast::Journal const j);

std::optional<std::pair<std::string, std::string>>
no2pg(std::shared_ptr<NodeObject> const& no);

bool
testNo(std::shared_ptr<NodeObject> const& no, std::size_t const keyBytes = 32);

} // ripple

#endif //RIPPLE_CORE_PG_H_INCLUDED
