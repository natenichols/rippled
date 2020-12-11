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

#ifndef RIPPLE_NODESTORE_REPORTING_BACKEND_H_INCLUDED
#define RIPPLE_NODESTORE_REPORTING_BACKEND_H_INCLUDED

#include <ripple/nodestore/Types.h>
#include <atomic>
#include <cstdint>

namespace ripple {
namespace NodeStore {

/** A backend used for Reporting Mode.

    The ReportingBackend uses a swappable backend so that other database systems
    can be tried. ReportingBackend is called directly from a ReadView and does
    not rely on SHAMap. As such, it has different methods for storing transactions
    and storing ledger entries to mimic txMap_ and stateMap_.
*/
class ReportingBackend
{
public:
    struct Counters
    {
        std::atomic<std::uint64_t> writeDurationUs{0};
        std::atomic<std::uint64_t> writeRetries{0};
        std::atomic<std::uint64_t> writesDelayed{0};
        std::atomic<std::uint64_t> readRetries{0};
        std::atomic<std::uint64_t> readErrors{0};
    };

    /** Destroy the backend.

        All open files are closed and flushed. If there are batched writes
        or other tasks scheduled, they will be completed before this call
        returns.
    */
    virtual ~ReportingBackend() = default;

    /** Get the human-readable name of this backend.
        This is used for diagnostic output.
    */
    virtual std::string
    getName() = 0;

    /** Open the backend.
        @param createIfMissing Create the database files if necessary.
        This allows the caller to catch exceptions.
    */
    virtual void
    open(bool createIfMissing = true) = 0;

    /** Returns true is the database is open.
     */
    virtual bool
    isOpen() = 0;

    /** Close the backend.
        This allows the caller to catch exceptions.
    */
    virtual void
    close() = 0;

    /** Fetch a single transaction.
        If the object is not found or an error is encountered, the
        result will indicate the condition.
        @note This will be called concurrently.
        @param hash The nodestore hash of the data.
        @param seq The sequence number of the ledger that the transaction is in.
        @param txBytes [out] The bytes representing a transaction if successful.
        @return The result of the operation.
    */
    virtual Status
    fetchTx(uint256 const& hash, std::uint32_t seq, std::shared_ptr<Blob>& txBytes) = 0;

    /** Fetch a batch of transactions. */
    virtual std::pair<std::vector<std::shared_ptr<Blob>>, Status>
    fetchTxBatch(std::vector<uint256> const& hashes, std::vector<std::uint32_t> const& sequences) = 0;

    /** Store a single transaction.
        @note This will be called concurrently.
        @param hash The nodestore hash of the transaction to store.
        @param sequence The sequence of the ledger the transation is in.
        @param txBytes The bytes representing the transaction to store.
    */
    virtual void
    storeTx(uint256 const& hash, std::uint32_t const& seq, std::shared_ptr<Blob> const& txBytes) = 0;

    /** Store a group of transactions */
    virtual void
    storeTxBatch(std::vector<uint256> const& hashes, std::uint32_t seq, std::vector<std::shared_ptr<Blob>> const& txBytes) = 0;

    /** Fetch a single transaction.
        If the object is not found or an error is encountered, the
        result will indicate the condition.
        @note This will be called concurrently.
        @param hash The nodestore hash of the ledger entry data.
        @param seq The sequence number of the ledger that the ledger entry is in.
        @param entryBytes [out] The bytes representing a ledger entry if successful.
        @return The result of the operation.
    */
    virtual Status
    fetchEntry(uint256 const& hash, std::uint32_t seq, std::shared_ptr<Blob>& entryBytes) = 0;

    /** Fetch a batch of ledger entries. */
    virtual std::pair<std::vector<std::shared_ptr<Blob>>, Status>
    fetchEntryBatch(std::vector<uint256> const& hashes, std::vector<std::uint32_t> const& sequences) = 0;

    virtual std::pair<std::vector<std::pair<uint256, std::shared_ptr<Blob>>>, Status>
    doUpperBound(uint256 marker, std::uint32_t seq, std::uint32_t limit) = 0;

    /** Store a single ledger entry.
        @note This will be called concurrently.
        @param hash The nodestore hash of the ledger entry to store.
        @param sequence The sequence of the ledger the ledger entry is in.
        @param entryBytes The bytes representing the ledger entry to store.
    */
    virtual void
    storeEntry(uint256 const& hash, std::uint32_t const& seq, std::shared_ptr<Blob> const& entryBytes) = 0;

    /** Store a batch of ledger entries */
    virtual void
    storeEntryBatch(std::vector<uint256> const& hashes, std::uint32_t seq, std::vector<std::shared_ptr<Blob>> const& entryBytes) = 0;

    virtual void
    sync() = 0;

    virtual Counters const&
    counters() const = 0;
};

}  // namespace NodeStore
}  // namespace ripple

#endif