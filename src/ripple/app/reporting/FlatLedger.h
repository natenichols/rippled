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
#include <ripple/ledger/ReadView.h>
#include <ripple/beast/utility/Journal.h>
#include <ripple/core/TimeKeeper.h>
#include <ripple/protocol/STLedgerEntry.h>
#include <ripple/app/reporting/CassandraBackend.h>

#ifndef RIPPLE_CORE_FLATLEDGER_H_INCLUDED
#define RIPPLE_CORE_FLATLEDGER_H_INCLUDED

namespace ripple
{
class FlatLedger final : public DigestAwareReadView
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

    FlatLedger(LedgerInfo const& info, Config const& config, Family& family);

    FlatLedger(FlatLedger const& previous, NetClock::time_point closeTime);

    FlatLedger(
        LedgerInfo const& info,
        bool& loaded,
        bool acquire,
        Config const& config,
        Family& family,
        beast::Journal j);

    ~FlatLedger() = default;


    void
    rawErase(uint256 const& key);

    void
    rawInsert(
        std::shared_ptr<SLE> const& sle,
        NodeStore::CassandraBackend& cassandra);

    void
    rawReplace(std::shared_ptr<SLE> const& sle);

    void
    updateSkipList();

    bool
    exists(uint256 const& entry) const;

    bool
    exists(Keylet const& k) const override;

    bool
    open() const override
    {
        return false;
    }  

    void
    setLedgerInfo(LedgerInfo const& info)
    {
        info_ = info;
    }

    LedgerInfo const&
    info() const override
    {
        return info_;
    }

    Fees const&
    fees() const override
    {
        return fees_;
    }

    Rules const&
    rules() const override
    {
        return rules_;
    }

    boost::optional<uint256>
    succ(uint256 const& key, boost::optional<uint256> const& last = boost::none)
        const override;

    std::shared_ptr<SLE const>
    read(Keylet const& k) const override;

    std::unique_ptr<sles_type::iter_base>
    slesBegin() const final;

    std::unique_ptr<sles_type::iter_base>
    slesEnd() const override;

    std::unique_ptr<sles_type::iter_base>
    slesUpperBound(uint256 const& key) const override;

    std::unique_ptr<txs_type::iter_base>
    txsBegin() const override;

    std::unique_ptr<txs_type::iter_base>
    txsEnd() const override;

    bool
    txExists(uint256 const& key) const override;

    tx_type
    txRead(key_type const& key) const override;

    boost::optional<digest_type>
    digest(key_type const& key) const override;

    uint256
    rawTxInsert(
        uint256 const& key,
        std::shared_ptr<Serializer const> const& txn,
        std::shared_ptr<Serializer const> const& metaData,
        NodeStore::CassandraBackend& cassandra);

    void 
    setImmutable(
        Config const& config,
        bool rehash);
private:
    class sles_iter_impl;
    class txs_iter_impl;

    bool
    setup(Config const& config);
    
    SHAMap::const_iterator
    upper_bound(uint256 const& id) const;

    std::mutex mutable mutex_;

    Fees fees_;
    Rules rules_;
    LedgerInfo info_;

    std::map<uint256, Blob> txMap_;
    std::map<uint256, Blob> stateMap_;
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