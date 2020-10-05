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

#include<ripple/app/reporting/FlatLedger.h>
#include<ripple/consensus/LedgerTiming.h>
#include<ripple/app/ledger/Ledger.h>

namespace ripple
{

static uint256
calculateLedgerHash(LedgerInfo const& info)
{
    // VFALCO This has to match addRaw in View.h.
    return sha512Half(
        HashPrefix::ledgerMaster,
        std::uint32_t(info.seq),
        std::uint64_t(info.drops.drops()),
        info.parentHash,
        info.txHash,
        info.accountHash,
        std::uint32_t(info.parentCloseTime.time_since_epoch().count()),
        std::uint32_t(info.closeTime.time_since_epoch().count()),
        std::uint8_t(info.closeTimeResolution.count()),
        std::uint8_t(info.closeFlags));
}

class FlatLedger::sles_iter_impl : public sles_type::iter_base
{
private:
    SHAMap::const_iterator iter_;

public:
    sles_iter_impl() = delete;
    sles_iter_impl&
    operator=(sles_iter_impl const&) = delete;

    sles_iter_impl(sles_iter_impl const&) = default;

    sles_iter_impl(SHAMap::const_iterator iter) : iter_(iter)
    {
    }

    std::unique_ptr<base_type>
    copy() const override
    {
        return std::make_unique<sles_iter_impl>(*this);
    }

    bool
    equal(base_type const& impl) const override
    {
        auto const& other = dynamic_cast<sles_iter_impl const&>(impl);
        return iter_ == other.iter_;
    }

    void
    increment() override
    {
        ++iter_;
    }

    sles_type::value_type
    dereference() const override
    {
        auto const item = *iter_;
        SerialIter sit(item.slice());
        return std::make_shared<SLE const>(sit, item.key());
    }
};

//------------------------------------------------------------------------------

class FlatLedger::txs_iter_impl : public txs_type::iter_base
{
private:
    bool metadata_;
    SHAMap::const_iterator iter_;

public:
    txs_iter_impl() = delete;
    txs_iter_impl&
    operator=(txs_iter_impl const&) = delete;

    txs_iter_impl(txs_iter_impl const&) = default;

    txs_iter_impl(bool metadata, SHAMap::const_iterator iter)
        : metadata_(metadata), iter_(iter)
    {
    }

    std::unique_ptr<base_type>
    copy() const override
    {
        return std::make_unique<txs_iter_impl>(*this);
    }

    bool
    equal(base_type const& impl) const override
    {
        auto const& other = dynamic_cast<txs_iter_impl const&>(impl);
        return iter_ == other.iter_;
    }

    void
    increment() override
    {
        ++iter_;
    }

    txs_type::value_type
    dereference() const override
    {
        auto const item = *iter_;
        if (metadata_)
            return deserializeTxPlusMeta(item);
        return {deserializeTx(item), nullptr};
    }
};

//------------------------------------------------------------------------------


FlatLedger::FlatLedger(LedgerInfo const& info, Config const& config, Family& family)
    : rules_(config.features)
    , info_(info)
{
    assert(config.reporting());

    info_.hash = calculateLedgerHash(info_);
}

FlatLedger::FlatLedger(
    FlatLedger const& previous, 
    NetClock::time_point closeTime)
    : fees_(previous.fees_)
    , rules_(previous.rules_)
{
    assert(previous.config().reporting());

    info_.seq = previous.info_.seq + 1;
    info_.parentCloseTime = previous.info_.closeTime;
    info_.hash = previous.info().hash + uint256(1);
    info_.drops = previous.info().drops;
    info_.closeTimeResolution = previous.info_.closeTimeResolution;
    info_.parentHash = previous.info().hash;
    info_.closeTimeResolution = getNextLedgerTimeResolution(
        previous.info_.closeTimeResolution,
        getCloseAgree(previous.info()),
        info_.seq);

    if (previous.info_.closeTime == NetClock::time_point{})
    {
        info_.closeTime = roundCloseTime(closeTime, info_.closeTimeResolution);
    }
    else
    {
        info_.closeTime =
            previous.info_.closeTime + info_.closeTimeResolution;
    }
}

FlatLedger::FlatLedger(
    LedgerInfo const& info,
    bool& loaded,
    bool acquire,
    Config const& config,
    Family& family,
    beast::Journal j)
    : rules_(config.features)
    , info_(info)
{
    assert(config.reporting());

    acquire = false;
    loaded = true;
    setup(config);

    info_.hash = calculateLedgerHash(info_);
}

bool
FlatLedger::setup(Config const& config)
{
    fees_.base = config.FEE_DEFAULT;
    fees_.units = config.TRANSACTION_FEE_BASE;
    fees_.reserve = config.FEE_ACCOUNT_RESERVE;
    fees_.increment = config.FEE_OWNER_RESERVE;

    return true;
}

boost::optional<uint256>
FlatLedger::succ(uint256 const& key, boost::optional<uint256> const& last) const 
{
    std::cout << "HI" << std::endl;
    return {};
}

std::shared_ptr<SLE const>
FlatLedger::read(Keylet const& k) const 
{
    std::cout << "HI" << std::endl;
    return nullptr;
}

auto 
FlatLedger::slesBegin() const -> std::unique_ptr<sles_type::iter_base>
{
    std::cout << "HI" << std::endl;
    return nullptr;
}

auto
FlatLedger::slesEnd() const -> std::unique_ptr<sles_type::iter_base>
{
    std::cout << "HI" << std::endl;
    return nullptr;
}

auto
FlatLedger::slesUpperBound(uint256 const& key) const 
    -> std::unique_ptr<sles_type::iter_base>
{
    std::cout << "HI" << std::endl;
    return nullptr;
}

auto
FlatLedger::txsBegin() const -> std::unique_ptr<txs_type::iter_base>
{
    std::cout << "HI" << std::endl;
    return nullptr;
}

auto 
FlatLedger::txsEnd() const -> std::unique_ptr<txs_type::iter_base>
{
    std::cout << "HI" << std::endl;
    return nullptr;
}

bool
FlatLedger::txExists(uint256 const& key) const 
{    
    std::cout << "HI" << std::endl;
    return false;
}

auto 
FlatLedger::txRead(key_type const& key) const -> tx_type
{
    std::cout << "HI" << std::endl;
    return {nullptr, nullptr};
}

void
FlatLedger::rawErase(uint256 const& key)
{

}

void
FlatLedger::rawInsert(
    std::shared_ptr<SLE> const& sle,
    NodeStore::CassandraBackend& cassandra)
{
    Serializer s;
    sle->add(s);
    auto item = std::make_shared<SHAMapItem const>(sle->key(), std::move(s));
    cassandra.store(sle->key(), info().seq, item->peekData());
}

void
FlatLedger::rawReplace(std::shared_ptr<SLE> const& sle)
{

}

auto
FlatLedger::digest(key_type const& key) const -> boost::optional<digest_type>
{
    std::cout << "HI" << std::endl;
    return {};
}

bool
FlatLedger::exists(Keylet const& k) const
{
    return true;
}

bool
FlatLedger::exists(uint256 const& entry) const
{
    return true;
}

uint256
FlatLedger::rawTxInsert(
    uint256 const& key,
    std::shared_ptr<Serializer const> const& txn,
    std::shared_ptr<Serializer const> const& metaData,
    NodeStore::CassandraBackend& cassandra)
{
    assert(metaData);

    // low-level - just add to table
    Serializer s(txn->getDataLength() + metaData->getDataLength() + 16);
    s.addVL(txn->peekData());
    s.addVL(metaData->peekData());
    auto item = std::make_shared<SHAMapItem const>(key, std::move(s));
    auto seq = info().seq;
    auto hash = sha512Half(
        HashPrefix::txNode, makeSlice(item->peekData()), item->key());

    // Write item, seq, and hash to Cassandra tx table
    cassandra.store(key, seq, item->peekData());

    return hash;
}

void
FlatLedger::updateSkipList()
{
    
}

std::tuple<std::shared_ptr<FlatLedger>, std::uint32_t, uint256>
loadLedgerHelperPostgres(
    std::variant<uint256, uint32_t, bool> const& whichLedger,
    Application& app,
    bool acquire)
{
    std::vector<LedgerInfo> infos;
    std::visit(
        [&infos, &app](auto&& arg) {
            infos = loadLedgerInfosPostgres(arg, app);
        },
        whichLedger);
    assert(infos.size() <= 1);
    if (!infos.size())
        return std::make_tuple(nullptr, 0, uint256{});
    LedgerInfo info = infos[0];
    bool loaded;
    auto ledger = std::make_shared<FlatLedger>(
        info,
        loaded,
        acquire,
        app.config(),
        app.getNodeFamily(),
        app.journal("Ledger"));

    if (!loaded)
        ledger.reset();

    return std::make_tuple(ledger, info.seq, info.hash);
}

std::shared_ptr<FlatLedger>
loadByIndexPostgres(std::uint32_t ledgerIndex, Application& app, bool acquire)
{
    auto [ledger, seq, hash] =
        loadLedgerHelperPostgres(uint32_t{ledgerIndex}, app, acquire);
    return ledger;
}

std::shared_ptr<FlatLedger>
loadByHashPostgres(uint256 const& ledgerHash, Application& app, bool acquire)
{
    auto [ledger, seq, hash] =
        loadLedgerHelperPostgres(uint256{ledgerHash}, app, acquire);

    assert(!ledger || ledger->info().hash == ledgerHash);

    return ledger;
}

std::shared_ptr<FlatLedger>
getValidatedLedgerPostgres(Application& app)
{
    auto seq = PgQuery(app.pgPool()).query("SELECT max_ledger()");
    if (!seq || PQgetisnull(seq.get(), 0, 0))
        return {};
    return loadByIndexPostgres(std::atoi(PQgetvalue(seq.get(), 0, 0)), app, false);
}

// TODO: make an abstract class that represents the ledgers db
// Implement two derived classes: one for SQLite, one for Postgres
// Maybe also do this for the transaction (or account_transactions) db

// if whichLedger is a bool, will simply load the latest ledger
// TODO create a struct for these args
std::vector<LedgerInfo>
loadLedgerInfosPostgres(
    std::variant<uint256, uint32_t, bool, std::pair<uint32_t, uint32_t>> const&
        whichLedger,
    Application& app)
{
    assert(app.config().usePostgresLedgerTx());
    std::string sql =
        "SELECT "
        "ledger_hash, prev_hash, account_set_hash, trans_set_hash, "
        "total_coins,"
        "closing_time, prev_closing_time, close_time_res, close_flags,"
        "ledger_seq from ledgers ";

    uint32_t expNumResults = 1;

    if (auto ledgerSeq = std::get_if<uint32_t>(&whichLedger))
    {
        sql += "WHERE ledger_seq = " + std::to_string(*ledgerSeq);
    }
    else if (auto ledgerHash = std::get_if<uint256>(&whichLedger))
    {
        sql += ("WHERE ledger_hash = \'\\x" + strHex(*ledgerHash) + "\'");
    }
    else if (
        auto minAndMax =
            std::get_if<std::pair<uint32_t, uint32_t>>(&whichLedger))
    {
        expNumResults = minAndMax->second - minAndMax->first;

        sql +=
            ("WHERE ledger_seq >= " + std::to_string(minAndMax->first) +
             " AND ledger_seq <= " + std::to_string(minAndMax->second));
    }
    else
    {
        sql += ("ORDER BY ledger_seq desc LIMIT 1");
    }
    sql += ";";

    JLOG(app.journal("Ledger").debug())
        << "loadLedgerHelperPostgres - sql : " << sql;

    assert(app.pgPool());
    std::shared_ptr<PgQuery> pg = std::make_shared<PgQuery>(app.pgPool());
    std::shared_ptr<Pg> conn;
    auto res = pg->query(sql.data(), conn);
    assert(res);
    auto result = PQresultStatus(res.get());
    app.pgPool()->checkin(conn);

    JLOG(app.journal("Ledger").debug())
        << "loadLedgerHelperPostgres - result: " << result;
    assert(result == PGRES_TUPLES_OK);

    // assert(PQntuples(res.get()) == expNumResults);
    if (PQntuples(res.get()) > 0)
        assert(PQnfields(res.get()) == 10);

    if (PQntuples(res.get()) == 0)
    {
        auto stream = app.journal("Ledger").debug();
        JLOG(stream) << "Ledger not found: " << sql;
        return {};
    }

    std::vector<LedgerInfo> infos;
    for (size_t i = 0; i < PQntuples(res.get()); ++i)
    {
        char const* hash = PQgetvalue(res.get(), i, 0);
        char const* prevHash = PQgetvalue(res.get(), i, 1);

        char const* accountHash = PQgetvalue(res.get(), i, 2);
        char const* txHash = PQgetvalue(res.get(), i, 3);
        char const* totalCoins = PQgetvalue(res.get(), i, 4);
        char const* closeTime = PQgetvalue(res.get(), i, 5);
        char const* parentCloseTime = PQgetvalue(res.get(), i, 6);
        char const* closeTimeRes = PQgetvalue(res.get(), i, 7);
        char const* closeFlags = PQgetvalue(res.get(), i, 8);
        char const* ledgerSeq = PQgetvalue(res.get(), i, 9);

        JLOG(app.journal("Ledger").debug())
            << "loadLedgerHelperPostgres - data = " << hash << " , " << prevHash
            << " , " << accountHash << " , " << txHash << " , " << totalCoins
            << ", " << closeTime << ", " << parentCloseTime << ", "
            << closeTimeRes << ", " << closeFlags << ", " << ledgerSeq;

        using time_point = NetClock::time_point;
        using duration = NetClock::duration;

        LedgerInfo info;
        info.parentHash.SetHexExact(prevHash + 2);
        info.txHash.SetHexExact(txHash + 2);
        info.accountHash.SetHexExact(accountHash + 2);
        info.drops = std::stoll(totalCoins);
        info.closeTime = time_point{duration{std::stoll(closeTime)}};
        info.parentCloseTime =
            time_point{duration{std::stoll(parentCloseTime)}};
        info.closeFlags = std::stoi(closeFlags);
        info.closeTimeResolution = duration{std::stoll(closeTimeRes)};
        info.seq = std::stoi(ledgerSeq);
        info.hash.SetHexExact(hash + 2);
        info.validated = true;
        infos.push_back(info);
    }

    return infos;
}

void 
FlatLedger::setImmutable(
    Config const& config,
    bool rehash)
{
    
}
} // namespace ripple
