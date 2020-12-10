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
#include<ripple/protocol/digest.h>
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
    using hash_map_iter =
        hash_map<uint256, sles_type::value_type>::const_iterator;

    hash_map_iter iter_;

public:
    sles_iter_impl() = delete;
    sles_iter_impl&
    operator=(sles_iter_impl const&) = delete;

    sles_iter_impl(sles_iter_impl const&) = default;

    sles_iter_impl(hash_map_iter iter) : iter_(iter)
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
        auto const [hash, sle] = *iter_;
        Serializer s;
        sle->add(s);
        SerialIter iter(s.slice());
        return std::make_shared<SLE const>(std::move(iter), hash);
    }
};

//------------------------------------------------------------------------------

class FlatLedger::txs_iter_impl : public txs_type::iter_base
{
private:
    bool metadata_;
    hash_map<uint256, tx_type>::const_iterator iter_;

public:
    txs_iter_impl() = delete;
    txs_iter_impl&
    operator=(txs_iter_impl const&) = delete;

    txs_iter_impl(txs_iter_impl const&) = default;

    txs_iter_impl(bool metadata, hash_map<uint256, tx_type>::const_iterator iter)
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
        auto [_, txs] = *iter_;
        return txs;
    }
};

//------------------------------------------------------------------------------


FlatLedger::FlatLedger(LedgerInfo const& info, Config const& config, NodeStore::ReportingBackend& backend)
    : rules_(config.features)
    , info_(info)
    , backend_(backend)
{
    assert(config.reporting());

    info_.hash = calculateLedgerHash(info_);
}

FlatLedger::FlatLedger(
    FlatLedger const& previous, 
    NetClock::time_point closeTime)
    : fees_(previous.fees_)
    , rules_(previous.rules_)
    , backend_(previous.backend_)
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
    beast::Journal j,
    NodeStore::ReportingBackend& backend)
    : rules_(config.features)
    , info_(info)
    , backend_(backend)
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
    return {};
}

std::shared_ptr<SLE const>
FlatLedger::read(Keylet const& k) const 
{
    std::shared_ptr<Blob> entry;
    backend_.fetchEntry(k.key, info().seq, entry);

    if(!entry)
        return nullptr;
    
    SerialIter sit(entry->data(), entry->size());
    auto sle = std::make_shared<SLE const>(sit, k.key);

    return sle;
}

auto 
FlatLedger::slesBegin() const -> std::unique_ptr<sles_type::iter_base>
{
    return std::make_unique<sles_iter_impl>(stateMap_.begin());
}

auto
FlatLedger::slesEnd() const -> std::unique_ptr<sles_type::iter_base>
{
    return std::make_unique<sles_iter_impl>(stateMap_.end());
}

auto
FlatLedger::slesUpperBound(uint256 const& key) const 
    -> std::unique_ptr<sles_type::iter_base>
{
    return nullptr;
}

auto
FlatLedger::txsBegin() const -> std::unique_ptr<txs_type::iter_base>
{
    return std::make_unique<txs_iter_impl>(true, txMap_.begin());
}

auto 
FlatLedger::txsEnd() const -> std::unique_ptr<txs_type::iter_base>
{
    return std::make_unique<txs_iter_impl>(true, txMap_.begin());
}

bool
FlatLedger::txExists(uint256 const& key) const 
{
    std::shared_ptr<Blob> tx;
    backend_.fetchTx(key, info().seq, tx);

    return tx != nullptr;
}


auto 
FlatLedger::txRead(key_type const& key) const -> tx_type
{
    std::shared_ptr<Blob> tx;
    backend_.fetchTx(key, info().seq, tx);

    if(!tx)
        return {nullptr, nullptr};

    auto item = std::make_shared<SHAMapItem>(key, *tx);
    return deserializeTxPlusMeta(*item);
}

std::shared_ptr<SLE>
FlatLedger::peek(Keylet const& k) const
{
    std::shared_ptr<Blob> entry;
    backend_.fetchEntry(k.key, info().seq, entry);

    if(!entry)
        return nullptr;
    
    SerialIter sit(entry->data(), entry->size());
    auto sle = std::make_shared<SLE>(sit, k.key);

    return sle;
}

void
FlatLedger::rawErase(uint256 const& key)
{
    // backend_.remove(key, info().seq);
}

void
FlatLedger::rawInsert(std::shared_ptr<SLE> const& sle)
{
    Serializer s;
    sle->add(s);
    auto item = std::make_shared<SHAMapItem const>(sle->key(), std::move(s));
    backend_.storeEntry(sle->key(), info().seq, std::make_shared<Blob>(item->peekData()));
}

void
FlatLedger::rawReplace(std::shared_ptr<SLE> const& sle)
{
    rawInsert(sle);
}

auto
FlatLedger::digest(key_type const& key) const -> boost::optional<digest_type>
{
    return {};
}

bool
FlatLedger::exists(Keylet const& k) const
{
   return exists(k.key);
}

bool
FlatLedger::exists(uint256 const& entry) const
{
    std::shared_ptr<Blob> tx;
    backend_.fetchEntry(entry, info().seq, tx);

    return tx != nullptr;
}

uint256
FlatLedger::rawTxInsertWithHash(
    uint256 const& key,
    std::shared_ptr<Serializer const> const& txn,
    std::shared_ptr<Serializer const> const& metaData)
{
    if(!metaData)
        Throw<std::runtime_error>("Metadata not provided");

    // low-level - just add to table
    Serializer s(txn->getDataLength() + metaData->getDataLength() + 16);
    s.addVL(txn->peekData());
    s.addVL(metaData->peekData());
    auto item = std::make_shared<SHAMapItem const>(key, std::move(s));
    auto seq = info().seq;
    auto hash = sha512Half(
        HashPrefix::txNode, makeSlice(item->peekData()), item->key());

    txMap_[hash] = deserializeTxPlusMeta(*item);
    // Write item, seq, and hash to Cassandra tx table
    backend_.storeTx(hash, seq, std::make_shared<Blob>(item->peekData()));

    return hash;
}

void
FlatLedger::rawTxInsert(
    uint256 const& key,
    std::shared_ptr<Serializer const> const& txn,
    std::shared_ptr<Serializer const> const& metaData)
{
    if(!metaData)
        Throw<std::runtime_error>("Metadata not provided");

    // low-level - just add to table
    Serializer s(txn->getDataLength() + metaData->getDataLength() + 16);
    s.addVL(txn->peekData());
    s.addVL(metaData->peekData());
    auto item = std::make_shared<SHAMapItem const>(key, std::move(s));
    auto seq = info().seq;
    auto hash = sha512Half(
        HashPrefix::txNode, makeSlice(item->peekData()), item->key());

    txMap_[hash] = deserializeTxPlusMeta(*item);
    // Write item, seq, and hash to Cassandra tx table
    backend_.storeTx(hash, seq, std::make_shared<Blob>(item->peekData()));
}

void
FlatLedger::updateSkipList()
{
    if (info_.seq == 0)  // genesis ledger has no previous ledger
        return;

    std::uint32_t prevIndex = info_.seq - 1;

    // update record of every 256th ledger
    if ((prevIndex & 0xff) == 0)
    {
        auto const k = keylet::skip(prevIndex);
        auto sle = peek(k);
        std::vector<uint256> hashes;

        bool created;
        if (!sle)
        {
            sle = std::make_shared<SLE>(k);
            created = true;
        }
        else
        {
            hashes = static_cast<decltype(hashes)>(sle->getFieldV256(sfHashes));
            created = false;
        }

        assert(hashes.size() <= 256);
        hashes.push_back(info_.parentHash);
        sle->setFieldV256(sfHashes, STVector256(hashes));
        sle->setFieldU32(sfLastLedgerSequence, prevIndex);
        if (created)
            rawInsert(sle);
        else
            rawReplace(sle);
    }

    // update record of past 256 ledger
    auto const k = keylet::skip();
    auto sle = peek(k);
    std::vector<uint256> hashes;
    bool created;
    if (!sle)
    {
        sle = std::make_shared<SLE>(k);
        created = true;
    }
    else
    {
        hashes = static_cast<decltype(hashes)>(sle->getFieldV256(sfHashes));
        created = false;
    }
    assert(hashes.size() <= 256);
    if (hashes.size() == 256)
        hashes.erase(hashes.begin());
    hashes.push_back(info_.parentHash);
    sle->setFieldV256(sfHashes, STVector256(hashes));
    sle->setFieldU32(sfLastLedgerSequence, prevIndex);
    if (created)
        rawInsert(sle);
    else
        rawReplace(sle);
}

// Load the ledger info for the specified ledger/s from the database
// @param whichLedger specifies the ledger to load via ledger sequence, ledger
// hash, a range of ledgers, or std::monostate (which loads the most recent)
// @param app Application
// @return vector of LedgerInfos
static std::vector<LedgerInfo>
loadLedgerInfosPostgres(
    std::variant<
        std::monostate,
        uint256,
        uint32_t,
        std::pair<uint32_t, uint32_t>> const& whichLedger,
    Application& app)
{
    if (!app.config().reporting())
        Throw<std::runtime_error>("Must be in reporting mode to use Postgres");

    std::vector<LedgerInfo> infos;
#ifdef RIPPLED_REPORTING
    auto log = app.journal("Ledger");
    assert(app.config().reporting());
    std::stringstream sql;
    sql << "SELECT ledger_hash, prev_hash, account_set_hash, trans_set_hash, "
           "total_coins, closing_time, prev_closing_time, close_time_res, "
           "close_flags, ledger_seq FROM ledgers ";

    uint32_t expNumResults = 1;

    if (auto ledgerSeq = std::get_if<uint32_t>(&whichLedger))
    {
        sql << "WHERE ledger_seq = " + std::to_string(*ledgerSeq);
    }
    else if (auto ledgerHash = std::get_if<uint256>(&whichLedger))
    {
        sql << ("WHERE ledger_hash = \'\\x" + strHex(*ledgerHash) + "\'");
    }
    else if (
        auto minAndMax =
            std::get_if<std::pair<uint32_t, uint32_t>>(&whichLedger))
    {
        expNumResults = minAndMax->second - minAndMax->first;

        sql
            << ("WHERE ledger_seq >= " + std::to_string(minAndMax->first) +
                " AND ledger_seq <= " + std::to_string(minAndMax->second));
    }
    else
    {
        sql << ("ORDER BY ledger_seq desc LIMIT 1");
    }
    sql << ";";

    JLOG(log.trace()) << __func__ << " : sql = " << sql.str();

    auto res = PgQuery(app.getPgPool())(sql.str().data());
    if (!res)
    {
        JLOG(log.error()) << __func__ << " : Postgres response is null - sql = "
                          << sql.str();
        assert(false);
        return {};
    }
    else if (res.status() != PGRES_TUPLES_OK)
    {
        JLOG(log.error()) << __func__
                          << " : Postgres response should have been "
                             "PGRES_TUPLES_OK but instead was "
                          << res.status() << " - msg  = " << res.msg()
                          << " - sql = " << sql.str();
        assert(false);
        return {};
    }

    JLOG(log.trace()) << __func__ << " Postgres result msg  : " << res.msg();

    if (res.isNull() || res.ntuples() == 0)
    {
        JLOG(log.debug()) << __func__
                          << " : Ledger not found. sql = " << sql.str();
        return {};
    }
    else if (res.ntuples() > 0)
    {
        if (res.nfields() != 10)
        {
            JLOG(log.error()) << __func__
                              << " : Wrong number of fields in Postgres "
                                 "response. Expected 10, but got "
                              << res.nfields() << " . sql = " << sql.str();
            assert(false);
            return {};
        }
    }

    for (size_t i = 0; i < res.ntuples(); ++i)
    {
        char const* hash = res.c_str(i, 0);
        char const* prevHash = res.c_str(i, 1);
        char const* accountHash = res.c_str(i, 2);
        char const* txHash = res.c_str(i, 3);
        std::int64_t totalCoins = res.asBigInt(i, 4);
        std::int64_t closeTime = res.asBigInt(i, 5);
        std::int64_t parentCloseTime = res.asBigInt(i, 6);
        std::int64_t closeTimeRes = res.asBigInt(i, 7);
        std::int64_t closeFlags = res.asBigInt(i, 8);
        std::int64_t ledgerSeq = res.asBigInt(i, 9);

        JLOG(log.trace()) << __func__ << " - Postgres response = " << hash
                          << " , " << prevHash << " , " << accountHash << " , "
                          << txHash << " , " << totalCoins << ", " << closeTime
                          << ", " << parentCloseTime << ", " << closeTimeRes
                          << ", " << closeFlags << ", " << ledgerSeq
                          << " - sql = " << sql.str();
        JLOG(log.debug()) << __func__
                          << " - Successfully fetched ledger with sequence = "
                          << ledgerSeq << " from Postgres";

        using time_point = NetClock::time_point;
        using duration = NetClock::duration;

        LedgerInfo info;
        info.parentHash.SetHexExact(prevHash + 2);
        info.txHash.SetHexExact(txHash + 2);
        info.accountHash.SetHexExact(accountHash + 2);
        info.drops = totalCoins;
        info.closeTime = time_point{duration{closeTime}};
        info.parentCloseTime = time_point{duration{parentCloseTime}};
        info.closeFlags = closeFlags;
        info.closeTimeResolution = duration{closeTimeRes};
        info.seq = ledgerSeq;
        info.hash.SetHexExact(hash + 2);
        info.validated = true;
        infos.push_back(info);
    }

#endif
    return infos;
}

// Load a ledger from Postgres
// @param whichLedger specifies sequence or hash of ledger. Passing
// @param app the Application
// @return tuple of (ledger, sequence, hash)
static std::tuple<std::shared_ptr<FlatLedger>, std::uint32_t, uint256>
loadLedgerHelperPostgres(
    std::variant<std::monostate, uint256, uint32_t> const& whichLedger,
    Application& app)
{
    if (!app.config().reporting())
        Throw<std::runtime_error>("Must be in reporting mode to use Postgres");

    std::vector<LedgerInfo> infos;
    std::visit(
        [&infos, &app](auto&& arg) {
            infos = loadLedgerInfosPostgres(arg, app);
        },
        whichLedger);
    assert(infos.size() <= 1);
    if (!infos.size())
        return std::make_tuple(std::shared_ptr<FlatLedger>(), 0, uint256{});
    LedgerInfo info = infos[0];
    auto ledger = std::make_shared<FlatLedger>(
        info,
        app.config(),
        app.getReportingETL().getReportingBackend());

    return std::make_tuple(ledger, info.seq, info.hash);
}

// Load a ledger by index (AKA sequence) from Postgres
// @param ledgerIndex the ledger index (or sequence) to load
// @param app reference to Application
// @return the loaded ledger
std::shared_ptr<FlatLedger>
loadByIndexPostgres(std::uint32_t ledgerIndex, Application& app)
{
    if (!app.config().reporting())
        Throw<std::runtime_error>("Must be in reporting mode to use Postgres");

    std::shared_ptr<FlatLedger> ledger;
    std::tie(ledger, std::ignore, std::ignore) =
        loadLedgerHelperPostgres(uint32_t{ledgerIndex}, app);

    return ledger;
}

// Load a ledger by hash from Postgres
// @param hash hash of the ledger to load
// @param app reference to Application
// @return the loaded ledger
std::shared_ptr<FlatLedger>
loadByHashPostgres(uint256 const& ledgerHash, Application& app)
{
    if (!app.config().reporting())
        Throw<std::runtime_error>("Must be in reporting mode to use Postgres");

    std::shared_ptr<FlatLedger> ledger;
    std::tie(ledger, std::ignore, std::ignore) =
        loadLedgerHelperPostgres(uint256{ledgerHash}, app);

    assert(!ledger || ledger->info().hash == ledgerHash);

    return ledger;
}

// Given a ledger sequence, return the ledger hash
// @param ledgerIndex ledger sequence
// @param app Application
// @return hash of ledger
uint256
getHashByIndexPostgres(std::uint32_t ledgerIndex, Application& app)
{
    if (!app.config().reporting())
        Throw<std::runtime_error>("Must be in reporting mode to use Postgres");

    uint256 ret;

    auto infos = loadLedgerInfosPostgres(ledgerIndex, app);
    assert(infos.size() <= 1);
    if (infos.size())
        return infos[0].hash;
    return {};
}

boost::optional<NetClock::time_point>
getCloseTimeByHashPostgres(
    LedgerHash const& ledgerHash,
    std::uint32_t index,
    Application& app)
{
    if (!app.config().reporting())
        Throw<std::runtime_error>("Must be in reporting mode to use Postgres");

    auto ledger = loadByHashPostgres(ledgerHash, app);

    if (ledger && ledger->info().seq == index)
        return ledger->info().closeTime;

    return {};
}

// Given a ledger sequence, return the ledger hash and the parent hash
// @param ledgerIndex ledger sequence
// @param[out] ledgerHash hash of ledger
// @param[out] parentHash hash of parent ledger
// @param app Application
// @return true if the data was found
bool
getHashesByIndexPostgres(
    std::uint32_t ledgerIndex,
    uint256& ledgerHash,
    uint256& parentHash,
    Application& app)
{
    if (!app.config().reporting())
        Throw<std::runtime_error>("Must be in reporting mode to use Postgres");

    auto infos = loadLedgerInfosPostgres(ledgerIndex, app);
    assert(infos.size() <= 1);
    if (infos.size())
    {
        ledgerHash = infos[0].hash;
        parentHash = infos[0].parentHash;
        return true;
    }
    return false;
}

// Given a contiguous range of sequences, return a map of
// sequence -> (hash, parent hash)
// @param minSeq lower bound of range
// @param maxSeq upper bound of range
// @param app Application
// @return mapping of all found ledger sequences to their hash and parent hash
std::map<std::uint32_t, std::pair<uint256, uint256>>
getHashesByIndexPostgres(
    std::uint32_t minSeq,
    std::uint32_t maxSeq,
    Application& app)
{
    if (!app.config().reporting())
        Throw<std::runtime_error>("Must be in reporting mode to use Postgres");

    std::map<uint32_t, std::pair<uint256, uint256>> ret;
    auto infos = loadLedgerInfosPostgres(std::make_pair(minSeq, maxSeq), app);
    for (auto& info : infos)
    {
        ret[info.seq] = std::make_pair(info.hash, info.parentHash);
    }
    return ret;
}

std::shared_ptr<FlatLedger>
getValidatedLedgerPostgres(Application& app)
{
    if (!app.config().reporting())
        Throw<std::runtime_error>("Must be in reporting mode to use Postgres");

    auto seq = PgQuery(app.getPgPool())("SELECT max_ledger()");
    if (!seq || seq.isNull())
        return {};

    return loadByIndexPostgres(seq.asInt(), app);
}

std::vector<
    std::pair<std::shared_ptr<STTx const>, std::shared_ptr<STObject const>>>
flatFetchTransactions(Application& app, std::vector<uint256>& nodestoreHashes, std::vector<std::uint32_t> const& sequences)
{
    if (!app.config().reporting())
    {
        assert(false);
        Throw<std::runtime_error>(
            "flatFetchTransactions: not running in reporting mode");
    }

    std::vector<
        std::pair<std::shared_ptr<STTx const>, std::shared_ptr<STObject const>>>
        txns;

    auto start = std::chrono::system_clock::now();
    auto [objs, status] =
        app.getReportingETL().getReportingBackend().fetchTxBatch(nodestoreHashes, sequences);

    if (status)
    {
        JLOG(app.journal("FlatLedger").debug())
            << "Failed to flatFetchTransaction with status: " 
            << status;

        return {};
    }

    auto end = std::chrono::system_clock::now();
    JLOG(app.journal("Ledger").debug())
        << " Flat fetch time : " << ((end - start).count() / 1000000000.0)
        << " number of transactions " << nodestoreHashes.size();
    assert(objs.size() == nodestoreHashes.size());
    for (size_t i = 0; i < objs.size(); ++i)
    {
        uint256& nodestoreHash = nodestoreHashes[i];
        auto& obj = objs[i];
        if (obj)
        {
            auto item = std::make_shared<SHAMapItem>(nodestoreHash, *obj);
            if (!item)
            {
                assert(false);
                Throw<std::runtime_error>(
                    "flatFetchTransactions : Error reading SHAMap node");
            }
            auto txnPlusMeta = deserializeTxPlusMeta(*item);
            if (!txnPlusMeta.first || !txnPlusMeta.second)
            {
                assert(false);
                Throw<std::runtime_error>(
                    "flatFetchTransactions : Error deserializing SHAMap node");
            }
            txns.push_back(std::move(txnPlusMeta));
        }
        else
        {
            assert(false);
            Throw<std::runtime_error>(
                "flatFetchTransactions : Containing SHAMap node not found");
        }
    }
    return txns;
}

std::vector<
    std::pair<std::shared_ptr<STTx const>, std::shared_ptr<STObject const>>>
flatFetchTransactions(ReadView const& ledger, Application& app)
{
    if (!app.config().reporting())
    {
        assert(false);
        return {};
    }
    std::vector<uint256> nodestoreHashes;
#ifdef RIPPLED_REPORTING

    auto log = app.journal("Ledger");

    std::string query =
        "SELECT nodestore_hash"
        "  FROM transactions "
        " WHERE ledger_seq = " +
        std::to_string(ledger.info().seq);
    auto res = PgQuery(app.getPgPool())(query.c_str());

    if (!res)
    {
        JLOG(log.error()) << __func__
                          << " : Postgres response is null - query = " << query;
        assert(false);
        return {};
    }
    else if (res.status() != PGRES_TUPLES_OK)
    {
        JLOG(log.error()) << __func__
                          << " : Postgres response should have been "
                             "PGRES_TUPLES_OK but instead was "
                          << res.status() << " - msg  = " << res.msg()
                          << " - query = " << query;
        assert(false);
        return {};
    }

    JLOG(log.trace()) << __func__ << " Postgres result msg  : " << res.msg();

    if (res.isNull() || res.ntuples() == 0)
    {
        JLOG(log.debug()) << __func__
                          << " : Ledger not found. query = " << query;
        return {};
    }
    else if (res.ntuples() > 0)
    {
        if (res.nfields() != 1)
        {
            JLOG(log.error()) << __func__
                              << " : Wrong number of fields in Postgres "
                                 "response. Expected 1, but got "
                              << res.nfields() << " . query = " << query;
            assert(false);
            return {};
        }
    }

    JLOG(log.trace()) << __func__ << " : result = " << res.c_str()
                      << " : query = " << query;
    for (size_t i = 0; i < res.ntuples(); ++i)
    {
        char const* nodestoreHash = res.c_str(i, 0);

        nodestoreHashes.push_back(from_hex_text<uint256>(nodestoreHash + 2));
    }
#endif

    auto sequences = std::vector<uint32_t>(nodestoreHashes.size(), ledger.info().seq);
    return flatFetchTransactions(app, nodestoreHashes, sequences);
}

} // namespace ripple
