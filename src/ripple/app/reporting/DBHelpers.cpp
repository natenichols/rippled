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

#include <ripple/app/reporting/DBHelpers.h>
#include <memory>

namespace ripple {

bool
writeToLedgersDB(
    LedgerInfo const& info,
    std::shared_ptr<PgQuery>& pgQuery,
    std::shared_ptr<Pg>& conn,
    beast::Journal& j)
{
    JLOG(j.debug()) << __func__;
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
    JLOG(j.trace()) << __func__ << " : "
                    << " : "
                    << "query string = " << ledgerInsert;

    //    while (!etl.isStopping())
    while (true)
    {
        auto res = pgQuery->queryVariant({ledgerInsert.data(), {}}, conn);

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
                JLOG(j.error()) << __func__ << " : "
                                << "Insert into ledger DB error: " << resStatus
                                << ", " << err.second << ". Stopping ETL";
                return false;
            }
            else
            {
                JLOG(j.error()) << __func__ << " : "
                                << "Insert into ledger DB error: " << resStatus
                                << ", " << err.second << ". Retrying";
                continue;
            }
        }

        auto const& queryResult = std::get<pg_result_type>(res);
        if (!queryResult)
        {
            JLOG(j.error()) << __func__ << " : "
                            << " queryResult is null. Retrying";
        }
        else if (PQresultStatus(queryResult.get()) != PGRES_COMMAND_OK)
        {
            JLOG(j.error())
                << __func__ << " : "
                << " resultStatus != PGRES_COMMAND_OK"
                << "result status = " << PQresultStatus(queryResult.get())
                << ". Retrying";
        }
        else
        {
            JLOG(j.debug()) << __func__ << " : "
                            << "Succesfully wrote to ledgers db";
            break;
        }
    }

    return true;
    //    return !etl.isStopping();
}

void
executeUntilSuccess(
    std::shared_ptr<PgQuery>& pg,
    std::shared_ptr<Pg>& conn,
    std::string const& query,
    ExecStatusType expectedResult,
    beast::Journal& j)
{
    JLOG(j.trace()) << __func__ << " : "
                    << " query = " << query
                    << " expectedResult = " << expectedResult;
    //    while (!etl.isStopping())
    while (true)
    {
        auto res = pg->queryVariant({query.data(), {}}, conn);
        if (auto result = std::get_if<pg_result_type>(&res))
        {
            auto resultStatus = PQresultStatus(result->get());
            if (resultStatus == expectedResult)
            {
                JLOG(j.trace()) << __func__ << " : "
                                << "Successfully executed query. "
                                << "query = " << query
                                << "result status = " << resultStatus;
                return;
            }
            else
            {
                JLOG(j.error())
                    << __func__ << " : "
                    << "result status does not match expected. "
                    << "result status = " << resultStatus
                    << " expected = " << expectedResult << "query = " << query;
            }
        }
        else if (auto result = std::get_if<pg_error_type>(&res))
        {
            auto errorStatus = PQresStatus(result->first);
            JLOG(j.error())
                << __func__ << " : "
                << "error executing query = " << query
                << ". errorStatus = " << errorStatus
                << ". error message = " << result->second << ". Retrying";
        }
        else
        {
            JLOG(j.error()) << __func__ << " : "
                            << "empty variant. Retrying";
        }
    }
}

void
bulkWriteToTable(
    std::shared_ptr<PgQuery>& pgQuery,
    std::shared_ptr<Pg>& conn,
    char const* copyQuery,
    std::string const bufString,
    beast::Journal& j)
{
    JLOG(j.debug()) << __func__;
    //    while (!etl.isStopping())
    while (true)
    {
        // Initiate COPY operation
        executeUntilSuccess(pgQuery, conn, copyQuery, PGRES_COPY_IN, j);

        JLOG(j.trace()) << "copy buffer = " << bufString;
        executeUntilSuccess(
            [&conn, &bufString]() {
                return PQputCopyData(
                    conn->getConn(), bufString.c_str(), bufString.size());
            },
            conn,
            PGRES_COPY_IN,
            j);

        executeUntilSuccess(
            [&conn]() { return PQputCopyEnd(conn->getConn(), nullptr); },
            conn,
            PGRES_COMMAND_OK,
            j);
        auto pqResultStatus = PGRES_COMMAND_OK;
        //        while (!etl.isStopping())
        while (true)
        {
            auto pqResult = PQgetResult(conn->getConn());
            if (!pqResult)
                break;
            pqResultStatus = PQresultStatus(pqResult);
        }
        if (pqResultStatus != PGRES_COMMAND_OK)
        {
            JLOG(j.error()) << __func__ << " : "
                            << "Result of PQputCopyEnd is not PGRES_COMMAND_OK."
                            << " Result = " << pqResultStatus << ". Retrying";
            continue;
        }
        JLOG(j.debug()) << __func__ << " : "
                        << "Successfully wrote to account_transactions db";
        break;
    }
}

bool
checkConsistency(std::shared_ptr<PgPool> const& pgPool, beast::Journal& j)
{
    JLOG(j.debug()) << __func__ << " : "
                    << "checking consistency";
    bool isConsistent = true;
    assert(pgPool);
    std::shared_ptr<PgQuery> pgQuery = std::make_shared<PgQuery>(pgPool);

    // check that every ledger hash is present in nodestore
    std::string sql =
        "select ledger_seq, ledger_hash from ledgers left join objects on "
        "ledgers.ledger_hash = objects.key where objects.key is null;";

    auto res = pgQuery->query(sql.data());
    auto result = PQresultStatus(res.get());
    JLOG(j.debug()) << __func__ << " : "
                    << " - ledger hash result : " << result;

    assert(result == PGRES_TUPLES_OK);

    if (PQntuples(res.get()) > 0)
    {
        isConsistent = false;
        for (size_t i = 0; i < PQntuples(res.get()); ++i)
        {
            char const* ledgerSeq = PQgetvalue(res.get(), i, 0);
            char const* ledgerHash = PQgetvalue(res.get(), i, 1);
            JLOG(j.error())
                << __func__ << " : "
                << "ledger hash not present in nodestore. sequence = "
                << ledgerSeq << " ledger hash = " << ledgerHash;
        }
    }

    // check that every state map root is present in nodestore
    sql =
        "select ledger_seq, account_set_hash from ledgers left join objects on "
        "ledgers.account_set_hash = objects.key where objects.key is null;";

    res = pgQuery->query(sql.data());
    result = PQresultStatus(res.get());
    JLOG(j.debug()) << __func__ << " : "
                    << " - state map result : " << result;

    assert(result == PGRES_TUPLES_OK);

    if (PQntuples(res.get()) > 0)
    {
        isConsistent = false;
        for (size_t i = 0; i < PQntuples(res.get()); ++i)
        {
            char const* ledgerSeq = PQgetvalue(res.get(), i, 0);
            char const* stateRoot = PQgetvalue(res.get(), i, 1);
            JLOG(j.error())
                << __func__ << " : "
                << "state map root not present in nodestore. sequence = "
                << ledgerSeq << " state map root = " << stateRoot;
        }
    }

    // check that every tx map root is present in nodestore
    sql =
        "select ledger_seq, trans_set_hash from ledgers left join objects on "
        "ledgers.trans_set_hash = objects.key where objects.key is null;";

    res = pgQuery->query(sql.data());
    result = PQresultStatus(res.get());
    JLOG(j.debug()) << __func__ << " : "
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
            JLOG(j.error())
                << __func__ << " : "
                << "tx map root not present in nodestore. sequence = "
                << ledgerSeq << " tx map root = " << txRoot;
        }
    }

    JLOG(j.info()) << __func__ << " : "
                   << "isConsistent = " << isConsistent;
    if (!isConsistent)
    {
        JLOG(j.fatal()) << __func__ << " : "
                        << "consistency check failed!";
    }
    else
    {
        JLOG(j.debug()) << __func__ << " : "
                        << "consistency check succeeded";
    }

    return isConsistent;
}

bool
writeToPostgres(
    LedgerInfo const& info,
    std::vector<AccountTransactionsData>& accountTxData,
    std::shared_ptr<PgPool> const& pgPool,
    bool useTxTables,
    beast::Journal& j)
{
    // TODO: clean this up a bit. use less auto, better error handling, etc
    JLOG(j.debug()) << __func__ << " : "
                    << "Beginning write to Postgres";
    if (!pgPool)
    {
        JLOG(j.fatal()) << __func__ << " : "
                        << "app_.pgPool is null";
        assert(false);
    }
    std::shared_ptr<PgQuery> pg = std::make_shared<PgQuery>(pgPool);
    std::shared_ptr<Pg> conn;

    executeUntilSuccess(pg, conn, "BEGIN", PGRES_COMMAND_OK, j);

    // Writing to the ledgers db fails if the ledger already exists in the db.
    // In this situation, the ETL process has detected there is another writer,
    // and falls back to only publishing
    if (!writeToLedgersDB(info, pg, conn, j))
    {
        pgPool->checkin(conn);

        JLOG(j.warn()) << __func__ << " : "
                       << "Failed to write to ledgers database.";
        return false;
    }

    if (useTxTables)
    {
        std::stringstream transactionsCopyBuffer;
        std::stringstream accountTransactionsCopyBuffer;
        for (auto& data : accountTxData)
        {
            std::string txHash = strHex(data.txHash);
            auto idx = data.transactionIndex;
            auto ledgerSeq = data.ledgerSequence;

            transactionsCopyBuffer << std::to_string(ledgerSeq) << '\t'
                                   << std::to_string(idx) << '\t' << "\\\\x"
                                   << txHash << '\n';

            for (auto& a : data.accounts)
            {
                std::string acct = strHex(a);
                accountTransactionsCopyBuffer
                    << "\\\\x" << acct << '\t' << std::to_string(ledgerSeq)
                    << '\t' << std::to_string(idx) << '\n';
            }
        }
        JLOG(j.debug()) << "transactions: " << transactionsCopyBuffer.str();
        JLOG(j.debug()) << "account_transactions: "
                        << accountTransactionsCopyBuffer.str();

        bulkWriteToTable(
            pg,
            conn,
            "COPY transactions FROM stdin",
            transactionsCopyBuffer.str(),
            j);
        bulkWriteToTable(
            pg,
            conn,
            "COPY account_transactions FROM stdin",
            accountTransactionsCopyBuffer.str(),
            j);
    }

    executeUntilSuccess(pg, conn, "COMMIT", PGRES_COMMAND_OK, j);

    pgPool->checkin(conn);

    JLOG(j.info()) << __func__ << " : "
                   << "Successfully wrote to Postgres";
    return true;
}

}  // namespace ripple
