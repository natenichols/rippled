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
#include <memory>

namespace ripple {
namespace NodeStore {

class PgBackend
    : public Backend
{
public:
    static constexpr std::size_t currentType = 1;

    beast::Journal const j_;
    size_t const keyBytes_;
    std::string const name_;
    nudb::store db_;
    std::atomic <bool> deletePath_;
    Scheduler& scheduler_;
    std::shared_ptr<PgPool> pool_;
    std::atomic<bool> open_ {false};
    std::shared_ptr<PgQuery> pgQuery_ {std::make_shared<PgQuery>(pool_)};

    PgBackend (
        size_t keyBytes,
        Section const& keyValues,
        Scheduler& scheduler,
        beast::Journal journal,
        std::shared_ptr<PgPool>& pool)
        : j_(journal)
        , keyBytes_ (keyBytes)
        , deletePath_(false)
        , scheduler_ (scheduler)
        , pool_ (pool)
    {}

    PgBackend (
        size_t keyBytes,
        Section const& keyValues,
        Scheduler& scheduler,
        nudb::context& context,
        beast::Journal journal)
        : j_(journal)
        , keyBytes_ (keyBytes)
        , db_ (context)
        , deletePath_(false)
        , scheduler_ (scheduler)
    {}

    ~PgBackend () override
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

        pgQuery_->querySync(
            "CREATE TABLE IF NOT EXISTS objects ("
            "    key   bytea NOT NULL,"
            "    value bytea NOT NULL)");
        pgQuery_->querySync(
            "CREATE INDEX IF NOT EXISTS objects_idx ON objects (key)");
        open_ = true;
    }

    void
    close() override
    {
        open_ = false;
    }

    Status
    fetch (void const* key, std::shared_ptr<NodeObject>* pno) override
    {
        pg_params params = {
            "SELECT value"
            "  FROM objects"
            " WHERE key = $1::bytea"
            " LIMIT 1",
            {}};
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
        pgQuery_->store(no, keyBytes_);
    }

    void
    storeBatch (Batch const& batch) override
    {
        pgQuery_->store(batch, keyBytes_);
    }

    void
    sync() override
    {
        pgQuery_->sync(keyBytes_);
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

    bool
    truncate() override
    {
        auto res = pgQuery_->querySync("truncate objects;");
        auto result = PQresultStatus(res.get());
        JLOG(j_.debug()) << __func__ << "result = " << result;
        return result == PGRES_COMMAND_OK;
    }
};

//------------------------------------------------------------------------------

class PgFactory : public Factory
{
public:
    PgFactory()
    {
        Manager::instance().insert(*this);
    }

    ~PgFactory() override
    {
        Manager::instance().erase(*this);
    }

    std::string
    getName() const override
    {
        return "postgres";
    }

    std::unique_ptr <Backend>
    createInstance (
        size_t keyBytes,
        Section const& keyValues,
        Scheduler& scheduler,
        beast::Journal journal,
        std::shared_ptr<PgPool> pool) override
    {
        return std::make_unique <PgBackend> (
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
        return std::make_unique <PgBackend> (
            keyBytes, keyValues, scheduler, context, journal);
    }
};

static PgFactory pgFactory;

}
}
