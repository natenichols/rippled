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

#include <ripple/app/reporting/ReportingETL.h>
#include <ripple/basics/Log.h>
#include <ripple/core/Pg.h>
#include <boost/container/flat_set.hpp>

#ifndef RIPPLE_CORE_DBHELPERS_H_INCLUDED
#define RIPPLE_CORE_DBHELPERS_H_INCLUDED

namespace ripple {
bool
writeToLedgersDB(
    LedgerInfo const& info,
    std::shared_ptr<PgQuery>& pgQuery,
    std::shared_ptr<Pg>& conn,
    beast::Journal& j);

template <class Func>
void
executeUntilSuccess(
    Func f,
    std::shared_ptr<Pg>& conn,
    ExecStatusType expectedResult,
    beast::Journal& j)
{
    JLOG(j.trace()) << __func__ << " : "
                    << " expectedResult = " << expectedResult;
    //    while (!etl.isStopping())
    while (true)
    {
        auto resCode = f();

        if (resCode == -1)
        {
            JLOG(j.error()) << __func__ << " : "
                            << "resCode is -1. error msg = "
                            << PQerrorMessage(conn->getConn()) << ". Retrying";
            continue;
        }
        else if (resCode == 0)
        {
            JLOG(j.error()) << __func__ << " : "
                            << "resCode is 0. Retrying";
            continue;
        }

        auto pqResult = PQgetResult(conn->getConn());
        auto pqResultStatus = PQresultStatus(pqResult);

        if (pqResultStatus == expectedResult)
        {
            JLOG(j.trace()) << __func__ << " : "
                            << "Successfully executed query";
            break;
        }
        else
        {
            JLOG(j.error())
                << __func__ << " : "
                << "pqResultStatus does not equal "
                << "expectedResult. pqResultStatus = " << pqResultStatus
                << " expectedResult = " << expectedResult;
            continue;
        }
    }
}

void
executeUntilSuccess(
    std::shared_ptr<PgQuery>& pg,
    std::shared_ptr<Pg>& conn,
    std::string const& query,
    ExecStatusType expectedResult,
    beast::Journal& j);

struct AccountTransactionsData
{
    boost::container::flat_set<AccountID> accounts;
    uint32_t ledgerSequence;
    uint32_t transactionIndex;
    uint256 txHash;

    AccountTransactionsData(TxMeta& meta, beast::Journal& j)
        : accounts(meta.getAffectedAccounts(j))
        , ledgerSequence(meta.getLgrSeq())
        , transactionIndex(meta.getIndex())
        , txHash(meta.getTxID())
    {
    }

    AccountTransactionsData(
        boost::container::flat_set<AccountID> const& accts,
        std::uint32_t seq,
        std::uint32_t idx,
        uint256 const& hash)
        : accounts(accts)
        , ledgerSequence(seq)
        , transactionIndex(idx)
        , txHash(hash)
    {
    }
};

void
bulkWriteToTable(
    std::shared_ptr<PgQuery>& pgQuery,
    std::shared_ptr<Pg>& conn,
    char const* copyQuery,
    std::string const bufString,
    beast::Journal& j);

bool
checkConsistency(std::shared_ptr<PgPool> const& pgPool, beast::Journal& j);

bool
writeToPostgres(
    LedgerInfo const& info,
    std::vector<AccountTransactionsData>& accountTxData,
    std::shared_ptr<PgPool> const& pgPool,
    bool useTxTables,
    beast::Journal& j);

}  // namespace ripple
#endif
