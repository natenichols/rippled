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

#include <ripple/app/ledger/Ledger.h>
#include <ripple/beast/utility/Journal.h>
#include <ripple/core/TimeKeeper.h>
#include <ripple/protocol/STLedgerEntry.h>

#ifndef RIPPLE_CORE_FLATLEDGER_H_INCLUDED
#define RIPPLE_CORE_FLATLEDGER_H_INCLUDED

namespace ripple
{
class FlatLedger 
{
public:
    static char const*
    getCountedObjectName()
    {
        return "FlatLedger";
    }

    FlatLedger(FlatLedger const&) = delete;
    FlatLedger&
    operator=(FlatLedger const&) = delete;

    FlatLedger(LedgerInfo const& info, Config const& config, Family& family, NodeStore::CassandraBackend& cass);

    FlatLedger(FlatLedger const& previous, NetClock::time_point closeTime, NodeStore::CassandraBackend& cass);

    FlatLedger(
        LedgerInfo const& info,
        bool& loaded,
        bool acquire,
        Config const& config,
        Family& family,
        beast::Journal j,
        NodeStore::CassandraBackend& cass);

    bool
    setup(Config const& config);

    void
    rawErase(uint256 const& key);

    void
    rawInsert(std::shared_ptr<SLE> const& sle);

    void
    rawReplace(std::shared_ptr<SLE> const& sle);

    void
    updateSkipList();

    bool
    exists(uint256 const& entry) const;

    void
    setLedgerInfo(LedgerInfo const& info)
    {
        info_ = info;
    }

    LedgerInfo const&
    info() const
    {
        return info_;
    }

    uint256
    rawTxInsert(
        uint256 const& key,
        std::shared_ptr<Serializer const> const& txn,
        std::shared_ptr<Serializer const> const& metaData);

    void 
    setImmutable(
        Config const& config,
        bool rehash);
private:
    SHAMap::const_iterator
    upper_bound(uint256 const& id) const;

    std::mutex mutable mutex_;

    Fees fees_;
    Rules rules_;
    LedgerInfo info_;

    NodeStore::CassandraBackend& cassandra_;
};

extern std::shared_ptr<FlatLedger>
getValidatedLedgerPostgres(Application& app);

extern std::tuple<std::shared_ptr<FlatLedger>, std::uint32_t, uint256>
loadLedgerHelperPostgres(
    std::variant<uint256, uint32_t, bool> const& whichLedger,
    Application& app,
    bool acquire = true);

extern std::shared_ptr<FlatLedger>
loadByHashPostgres(
    uint256 const& ledgerHash,
    Application& app,
    bool acquire = true);

extern std::shared_ptr<FlatLedger>
loadByIndexPostgres(
    std::uint32_t ledgerIndex,
    Application& app,
    bool acquire = true);

extern std::vector<LedgerInfo>
loadLedgerInfosPostgres(
    std::variant<uint256, uint32_t, bool, std::pair<uint32_t, uint32_t>> const&
        whichLedger,
    Application& app);

} // namespace ripple


#endif