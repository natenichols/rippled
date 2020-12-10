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


#include <ripple/ledger/ReadView.h>
#include <ripple/beast/utility/Journal.h>
#include <ripple/core/TimeKeeper.h>
#include <ripple/protocol/STLedgerEntry.h>
#include <ripple/app/reporting/nodestore/ReportingBackend.h>

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

    FlatLedger(LedgerInfo const& info, Config const& config, NodeStore::ReportingBackend& backend);

    FlatLedger(FlatLedger const& previous, NetClock::time_point closeTime);

    FlatLedger(
        LedgerInfo const& info,
        bool& loaded,
        bool acquire,
        Config const& config,
        beast::Journal j,
        NodeStore::ReportingBackend& Backend);

    ~FlatLedger() = default;

    void
    rawErase(uint256 const& key);

    void
    rawInsert(
        std::shared_ptr<SLE> const& sle);

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

    std::shared_ptr<SLE>
    peek(Keylet const& k) const;

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

    void
    rawTxInsert(
        uint256 const& key,
        std::shared_ptr<Serializer const> const& txn,
        std::shared_ptr<Serializer const> const& metaData);

    uint256
    rawTxInsertWithHash(
        uint256 const& key,
        std::shared_ptr<Serializer const> const& txn,
        std::shared_ptr<Serializer const> const& metaData);


private:
    class sles_iter_impl;
    class txs_iter_impl;

    bool
    setup(Config const& config);
    
    std::mutex mutable mutex_;

    Fees fees_;
    Rules rules_;
    LedgerInfo info_;

    hash_map<uint256, tx_type> txMap_;
    hash_map<uint256, sles_type::value_type> stateMap_;

    NodeStore::ReportingBackend& backend_;
};

extern std::shared_ptr<FlatLedger>
loadByIndexPostgres(std::uint32_t ledgerIndex, Application& app);

extern std::shared_ptr<FlatLedger>
loadByHashPostgres(uint256 const& ledgerHash, Application& app);

extern std::shared_ptr<FlatLedger>
getValidatedLedgerPostgres(Application& app);

extern uint256
getHashByIndexPostgres(std::uint32_t ledgerIndex, Application& app);

extern bool
getHashesByIndexPostgres(
    std::uint32_t ledgerIndex,
    uint256& ledgerHash,
    uint256& parentHash,
    Application& app);

extern std::map<std::uint32_t, std::pair<uint256, uint256>>
getHashesByIndexPostgres(
    std::uint32_t minSeq,
    std::uint32_t maxSeq,
    Application& app);

extern boost::optional<NetClock::time_point>
getCloseTimeByHashPostgres(
    uint256 const& ledgerHash,
    std::uint32_t index,
    Application& app);

// *** Reporting Mode Only ***
// Fetch all of the transactions contained in ledger from the nodestore.
// The transactions are fetched directly as a batch, instead of traversing the
// transaction SHAMap. Fetching directly is significantly faster than
// traversing, as there are less database reads, and all of the reads can
// executed concurrently. This function only works in reporting mode.
// @param ledger the ledger for which to fetch the contained transactions
// @param app reference to the Application
// @return vector of (transaction, metadata) pairs
extern std::vector<
    std::pair<std::shared_ptr<STTx const>, std::shared_ptr<STObject const>>>
flatFetchTransactions(ReadView const& ledger, Application& app);

// *** Reporting Mode Only ***
// For each nodestore hash, fetch the transaction.
// The transactions are fetched directly as a batch, instead of traversing the
// transaction SHAMap. Fetching directly is significantly faster than
// traversing, as there are less database reads, and all of the reads can
// executed concurrently. This function only works in reporting mode.
// @param nodestoreHashes hashes of the transactions to fetch
// @param app reference to the Application
// @return vector of (transaction, metadata) pairs
extern std::vector<
    std::pair<std::shared_ptr<STTx const>, std::shared_ptr<STObject const>>>
flatFetchTransactions(Application& app, std::vector<uint256>& nodestoreHashes, std::vector<std::uint32_t> const& seq);

} // namespace ripple
#endif