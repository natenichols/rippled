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
#include <ripple/core/Pg.h>
#ifndef RIPPLE_CORE_DBHELPERS_H_INCLUDED
#define RIPPLE_CORE_DBHELPERS_H_INCLUDED
namespace ripple {
bool
writeToLedgersDB(
    LedgerInfo const& info,
    std::shared_ptr<PgQuery>& pgQuery,
    std::shared_ptr<Pg>& conn,
    ReportingETL& etl)
{
    JLOG(etl.getJournal().debug()) << __func__;
    auto cmd = boost::format(
        R"(INSERT INTO ledgers 
                VALUES(%u,'\x%s', '\x%s',%u,%u,%u,%u,%u,'\x%s','\x%s')
                )");

    auto ledgerInsert = boost::str(
        cmd % info.seq % strHex(info.hash) % strHex(info.parentHash) %
        info.drops.drops() % info.closeTime.time_since_epoch().count() %
        info.parentCloseTime.time_since_epoch().count() %
        info.closeTimeResolution.count() % info.closeFlags %
        strHex(info.accountHash) % strHex(info.txHash));
    JLOG(etl.getJournal().trace()) << __func__ << " : "
                                   << " : "
                                   << "query string = " << ledgerInsert;

    while (!etl.isStopping())
    {
        auto res = pgQuery->querySyncVariant({ledgerInsert.data(), {}}, conn);

        // Uncomment to trigger "Ledger Ancestry error"
        //    res = pgQuery->querySyncVariant({ledgerInsert.data(), {}}, conn);

        // Uncomment to trigger "duplicate key"
        //    static int z = 0;
        //    if (++z < 10)
        //        return;
        //    res = pgQuery->querySyncVariant({ledgerInsert.data(), {}}, conn);

        if (std::holds_alternative<pg_error_type>(res))
        {
            pg_error_type const& err = std::get<pg_error_type>(res);
            auto resStatus = PQresStatus(err.first);
            if (err.first == PGRES_FATAL_ERROR &&
                ((err.second.find("ERROR:  duplicate key") !=
                  err.second.npos) ||
                 (err.second.find("ERROR:  Ledger Ancestry error") !=
                  err.second.npos)))
            {
                JLOG(etl.getJournal().error())
                    << __func__ << " : "
                    << "Insert into ledger DB error: " << resStatus << ", "
                    << err.second << ". Stopping ETL";
                return false;
            }
            else
            {
                JLOG(etl.getJournal().error())
                    << __func__ << " : "
                    << "Insert into ledger DB error: " << resStatus << ", "
                    << err.second << ". Retrying";
                continue;
            }
        }

        auto const& queryResult = std::get<pg_result_type>(res);
        if (!queryResult)
        {
            JLOG(etl.getJournal().error()) << __func__ << " : "
                                           << " queryResult is null. Retrying";
        }
        else if (PQresultStatus(queryResult.get()) != PGRES_COMMAND_OK)
        {
            JLOG(etl.getJournal().error())
                << __func__ << " : "
                << " resultStatus != PGRES_COMMAND_OK"
                << "result status = " << PQresultStatus(queryResult.get())
                << ". Retrying";
        }
        else
        {
            JLOG(etl.getJournal().debug()) << __func__ << " : "
                                           << "Succesfully wrote to ledgers db";
            break;
        }
    }
    return !etl.isStopping();
}

template <class Func>
void
executeUntilSuccess(
    Func f,
    std::shared_ptr<Pg>& conn,
    ExecStatusType expectedResult,
    ReportingETL& etl)
{
    JLOG(etl.getJournal().trace()) << __func__ << " : "
                                   << " expectedResult = " << expectedResult;
    while (!etl.isStopping())
    {
        auto resCode = f();

        if (resCode == -1)
        {
            JLOG(etl.getJournal().error())
                << __func__ << " : "
                << "resCode is -1. error msg = "
                << PQerrorMessage(conn->getConn()) << ". Retrying";
            continue;
        }
        else if (resCode == 0)
        {
            JLOG(etl.getJournal().error()) << __func__ << " : "
                                           << "resCode is 0. Retrying";
            continue;
        }

        auto pqResult = PQgetResult(conn->getConn());
        auto pqResultStatus = PQresultStatus(pqResult);

        if (pqResultStatus == expectedResult)
        {
            JLOG(etl.getJournal().trace()) << __func__ << " : "
                                           << "Successfully executed query";
            break;
        }
        else
        {
            JLOG(etl.getJournal().error())
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
    ReportingETL& etl)
{
    JLOG(etl.getJournal().trace())
        << __func__ << " : "
        << " query = " << query << " expectedResult = " << expectedResult;
    while (!etl.isStopping())
    {
        auto res = pg->querySyncVariant({query.data(), {}}, conn);
        if (auto result = std::get_if<pg_result_type>(&res))
        {
            auto resultStatus = PQresultStatus(result->get());
            if (resultStatus == expectedResult)
            {
                JLOG(etl.getJournal().trace())
                    << __func__ << " : "
                    << "Successfully executed query. "
                    << "query = " << query
                    << "result status = " << resultStatus;
                return;
            }
            else
            {
                JLOG(etl.getJournal().error())
                    << __func__ << " : "
                    << "result status does not match expected. "
                    << "result status = " << resultStatus
                    << " expected = " << expectedResult << "query = " << query;
            }
        }
        else if (auto result = std::get_if<pg_error_type>(&res))
        {
            auto errorStatus = PQresStatus(result->first);
            JLOG(etl.getJournal().error())
                << __func__ << " : "
                << "error executing query = " << query
                << ". errorStatus = " << errorStatus
                << ". error message = " << result->second << ". Retrying";
        }
        else
        {
            JLOG(etl.getJournal().error()) << __func__ << " : "
                                           << "empty variant. Retrying";
        }
    }
}

struct AccountTransactionsData
{
    std::vector<AccountID> accounts;
    uint32_t ledgerSequence;
    uint32_t transactionIndex;
    uint256 txHash;
    AccountTransactionsData(TxMeta& meta, beast::Journal& j)
    {
        ledgerSequence = meta.getLgrSeq();
        transactionIndex = meta.getIndex();
        txHash = meta.getTxID();
        for (auto& acct : meta.getAffectedAccounts(j))
        {
            accounts.push_back(acct);
        }
    }
};

void
writeToAccountTransactionsDB(
    std::vector<AccountTransactionsData>& accountTxData,
    std::shared_ptr<PgQuery>& pgQuery,
    std::shared_ptr<Pg>& conn,
    ReportingETL& etl)
{
    JLOG(etl.getJournal().debug()) << __func__;
    while (!etl.isStopping())
    {
        // Initiate COPY operation
        executeUntilSuccess(
            pgQuery,
            conn,
            "COPY account_transactions from STDIN",
            PGRES_COPY_IN,
            etl);

        // Write data to stream
        std::stringstream copyBuffer;
        for (auto& data : accountTxData)
        {
            std::string txHash = strHex(data.txHash);
            auto idx = data.transactionIndex;
            auto ledgerSeq = data.ledgerSequence;

            for (auto& a : data.accounts)
            {
                std::string acct = strHex(a);
                copyBuffer << "\\\\x" << acct << '\t'
                           << std::to_string(ledgerSeq) << '\t'
                           << std::to_string(idx) << '\t' << "\\\\x" << txHash
                           << '\n';
            }
        }

        PQsetnonblocking(conn->getConn(), 0);

        std::string bufString = copyBuffer.str();
        JLOG(etl.getJournal().trace()) << "copy buffer = " << bufString;
        executeUntilSuccess(
            [&conn, &bufString]() {
                return PQputCopyData(
                    conn->getConn(), bufString.c_str(), bufString.size());
            },
            conn,
            PGRES_COPY_IN,
            etl);

        executeUntilSuccess(
            [&conn]() { return PQputCopyEnd(conn->getConn(), nullptr); },
            conn,
            PGRES_COMMAND_OK,
            etl);
        auto pqResultStatus = PGRES_COMMAND_OK;
        while (!etl.isStopping())
        {
            auto pqResult = PQgetResult(conn->getConn());
            if (!pqResult)
                break;
            pqResultStatus = PQresultStatus(pqResult);
        }
        if (pqResultStatus != PGRES_COMMAND_OK)
        {
            JLOG(etl.getJournal().error())
                << __func__ << " : "
                << "Result of PQputCopyEnd is not PGRES_COMMAND_OK."
                << " Result = " << pqResultStatus << ". Retrying";
            continue;
        }
        JLOG(etl.getJournal().debug())
            << __func__ << " : "
            << "Successfully wrote to account_transactions db";
        break;
    }
}

bool
checkConsistency(ReportingETL& etl)
{
    JLOG(etl.getJournal().debug()) << __func__ << " : "
                                   << "checking consistency";
    bool isConsistent = true;
    assert(etl.getApplication().pgPool());
    std::shared_ptr<PgQuery> pgQuery =
        std::make_shared<PgQuery>(etl.getApplication().pgPool());

    // check that every ledger hash is present in nodestore
    std::string sql =
        "select ledger_seq, ledger_hash from ledgers left join objects on "
        "ledgers.ledger_hash = objects.key where objects.key is null;";

    auto res = pgQuery->querySync(sql.data());
    auto result = PQresultStatus(res.get());
    JLOG(etl.getJournal().debug()) << __func__ << " : "
                                   << " - ledger hash result : " << result;

    assert(result == PGRES_TUPLES_OK);

    if (PQntuples(res.get()) > 0)
    {
        isConsistent = false;
        for (size_t i = 0; i < PQntuples(res.get()); ++i)
        {
            char const* ledgerSeq = PQgetvalue(res.get(), i, 0);
            char const* ledgerHash = PQgetvalue(res.get(), i, 1);
            JLOG(etl.getJournal().error())
                << __func__ << " : "
                << "ledger hash not present in nodestore. sequence = "
                << ledgerSeq << " ledger hash = " << ledgerHash;
        }
    }

    // check that every state map root is present in nodestore
    sql =
        "select ledger_seq, account_set_hash from ledgers left join objects on "
        "ledgers.account_set_hash = objects.key where objects.key is null;";

    res = pgQuery->querySync(sql.data());
    result = PQresultStatus(res.get());
    JLOG(etl.getJournal().debug()) << __func__ << " : "
                                   << " - state map result : " << result;

    assert(result == PGRES_TUPLES_OK);

    if (PQntuples(res.get()) > 0)
    {
        isConsistent = false;
        for (size_t i = 0; i < PQntuples(res.get()); ++i)
        {
            char const* ledgerSeq = PQgetvalue(res.get(), i, 0);
            char const* stateRoot = PQgetvalue(res.get(), i, 1);
            JLOG(etl.getJournal().error())
                << __func__ << " : "
                << "state map root not present in nodestore. sequence = "
                << ledgerSeq << " state map root = " << stateRoot;
        }
    }

    // check that every tx map root is present in nodestore
    sql =
        "select ledger_seq, trans_set_hash from ledgers left join objects on "
        "ledgers.trans_set_hash = objects.key where objects.key is null;";

    res = pgQuery->querySync(sql.data());
    result = PQresultStatus(res.get());
    JLOG(etl.getJournal().debug()) << __func__ << " : "
                                   << " - tx map result : " << result;

    assert(result == PGRES_TUPLES_OK);

    if (PQntuples(res.get()) > 0)
    {
        for (size_t i = 0; i < PQntuples(res.get()); ++i)
        {
            char const* ledgerSeq = PQgetvalue(res.get(), i, 0);
            char const* txRoot = PQgetvalue(res.get(), i, 1);
            uint256 txHash;
            txHash.SetHexExact(txRoot + 2);
            if (txHash.isZero())
                isConsistent = false;
            JLOG(etl.getJournal().error())
                << __func__ << " : "
                << "tx map root not present in nodestore. sequence = "
                << ledgerSeq << " tx map root = " << txRoot;
        }
    }

    JLOG(etl.getJournal().info()) << __func__ << " : "
                                  << "isConsistent = " << isConsistent;
    if (!isConsistent)
    {
        JLOG(etl.getJournal().fatal()) << __func__ << " : "
                                       << "consistency check failed!";
    }
    else
    {
        JLOG(etl.getJournal().debug()) << __func__ << " : "
                                       << "consistency check succeeded";
    }

    return isConsistent;
}
}  // namespace ripple
#endif
