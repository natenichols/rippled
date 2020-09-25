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
Pg::connect()
{
    if (conn_)
    {
        // Nothing to do if we already have a good connection.
        if (PQstatus(conn_.get()) == CONNECTION_OK)
            return;
        /* Try resetting connection. */
        PQreset(conn_.get());
    }
    else  // Make new connection.
    {
        conn_.reset(PQconnectdbParams(
            reinterpret_cast<char const* const*>(&config_.keywordsIdx[0]),
            reinterpret_cast<char const* const*>(&config_.valuesIdx[0]),
            0));
        if (!conn_)
            Throw<std::runtime_error>("No db connection struct");
    }

    /** Results from a synchronous connection attempt can only be either
     * CONNECTION_OK or CONNECTION_BAD. */
    if (PQstatus(conn_.get()) == CONNECTION_BAD)
    {
        std::stringstream ss;
        ss << "DB connection status " << PQstatus(conn_.get()) << ": "
            << PQerrorMessage(conn_.get());
        Throw<std::runtime_error>(ss.str());
    }

    // Log server session console messages.
    PQsetNoticeReceiver(
        conn_.get(), noticeReceiver, const_cast<beast::Journal*>(&j_));
}

pg_variant_type
Pg::query(char const* command, std::size_t nParams, char const* const* values)
{
    pg_result_type ret {nullptr, [](PGresult* result){ PQclear(result); }};
    // Connect then submit query.
    while (true)
    {
        try
        {
            connect();
            ret.reset(PQexecParams(
                conn_.get(),
                command,
                nParams,
                nullptr,
                values,
                nullptr,
                nullptr,
                0));
            if (!ret)
                Throw<std::runtime_error>("no result structure returned");
            break;
        }
        catch (std::exception const& e)
        {
            // Sever connection and retry until successful.
            disconnect();
            JLOG(j_.error()) << "database error, retrying: " << e.what();
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    // Ensure proper query execution.
    switch (PQresultStatus(ret.get()))
    {
        case PGRES_TUPLES_OK:
        case PGRES_COMMAND_OK:
        case PGRES_COPY_IN:
        case PGRES_COPY_OUT:
        case PGRES_COPY_BOTH:
            break;
        default:
        {
            std::stringstream ss;
            ss << "bad query result: "
               << PQresStatus(PQresultStatus(ret.get()))
               << " error message: "
               << PQerrorMessage(conn_.get())
               << ", number of tuples: "
               << PQntuples(ret.get())
               << ", number of fields: "
               << PQnfields(ret.get());
            JLOG(j_.error()) << ss.str();
            disconnect();
            return pg_error_type{
                PQresultStatus(ret.get()), PQerrorMessage(conn_.get())};
        }
    }

    return std::move(ret);
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

pg_variant_type
Pg::query(pg_params const& dbParams)
{
    char const* const& command = dbParams.first;
    auto const formattedParams = formatParams(dbParams, j_);
    return query(
        command,
        formattedParams.size(),
        formattedParams.size()
            ? reinterpret_cast<char const* const*>(&formattedParams[0])
            : nullptr);
}

//-----------------------------------------------------------------------------

PgPool::PgPool(Section const& network_db_config, beast::Journal const j) : j_(j)
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
    {
        std::stringstream ss;
        ss << "Initial DB connection failed: "
           << PQerrorMessage(conn.get());
        Throw<std::runtime_error>(ss.str());
    }

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
           << "connection params: ";
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
}

void
PgPool::stop()
{
    stop_ = true;
    std::lock_guard<std::mutex> lock(mutex_);
    idle_.clear();
}

void
PgPool::idleSweeper()
{
    std::size_t before, after;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        before = idle_.size();
        if (config_.timeout != std::chrono::seconds(0))
        {
            auto const found = idle_.upper_bound( 
                clock_type::now() - config_.timeout);
            for (auto it = idle_.begin(); it != found;)
            {
                it = idle_.erase(it);
                --connections_;
            }
        }
        after = idle_.size();
    }

    JLOG(j_.info()) << "Idle sweeper. connections: " << connections_
                    << ". checked out: " << connections_ - after
                    << ". idle before, after sweep: " << before << ", "
                    << after;
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

    std::lock_guard<std::mutex> lock(mutex_);
    if (stop_ || ! *pg)
    {
        --connections_;
    }
    else
    {
        PGresult* res;
        while ((res = PQgetResult(pg->getConn())) != nullptr)
        {
            ExecStatusType const status = PQresultStatus(res);
            if (status == PGRES_COPY_IN)
            {
                if (PQputCopyEnd(pg->getConn(), nullptr) == -1)
                    pg.reset();
            }
            else if (status == PGRES_COPY_OUT || status == PGRES_COPY_BOTH)
            {
                pg.reset();
            }
        }

        if (pg)
            idle_.emplace(clock_type::now(), pg);
    }
    pg.reset();
}

//-----------------------------------------------------------------------------

pg_variant_type
PgQuery::queryVariant(pg_params const& dbParams, std::shared_ptr<Pg>& conn)
{
    while (!conn)
        conn = pool_->checkout();
    return conn->query(dbParams);
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

} // ripple
