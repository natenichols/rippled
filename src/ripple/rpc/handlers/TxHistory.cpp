//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2012-2014 Ripple Labs Inc.

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

#include <ripple/app/ledger/LedgerMaster.h>
#include <ripple/app/main/Application.h>
#include <ripple/app/misc/Transaction.h>
#include <ripple/core/DatabaseCon.h>
#include <ripple/core/Pg.h>
#include <ripple/core/SociDB.h>
#include <ripple/net/RPCErr.h>
#include <ripple/protocol/ErrorCodes.h>
#include <ripple/protocol/jss.h>
#include <ripple/resource/Fees.h>
#include <ripple/rpc/Context.h>
#include <ripple/rpc/Role.h>
#include <boost/format.hpp>

namespace ripple {

Json::Value
doTxHistoryReporting(RPC::JsonContext& context)
{
    assert(context.app.config().usePostgresLedgerTx());
    context.loadType = Resource::feeMediumBurdenRPC;

    if (!context.params.isMember(jss::start))
        return rpcError(rpcINVALID_PARAMS);

    unsigned int startIndex = context.params[jss::start].asUInt();

    if ((startIndex > 10000) && (!isUnlimited(context.role)))
        return rpcError(rpcNO_PERMISSION);

    std::string sql = boost::str(
        boost::format("SELECT ledger_seq, trans_id "
                      "  FROM account_transactions"
                      " ORDER BY ledger_seq DESC LIMIT 20 "
                      "OFFSET %u;") %
        startIndex);

    assert(context.app.pgPool());
    std::shared_ptr<PgQuery> pg =
        std::make_shared<PgQuery>(context.app.pgPool());

    auto res = pg->query(sql.data());
    assert(res);
    auto result = PQresultStatus(res.get());

    JLOG(context.j.debug()) << "txHistory - result: " << result;
    assert(result == PGRES_TUPLES_OK);

    Json::Value txs;

    for (size_t i = 0; i < PQntuples(res.get()); ++i)
    {
        uint32_t ledgerSequence = std::stoi(PQgetvalue(res.get(), i, 0));

        uint256 txID;
        txID.SetHexExact(PQgetvalue(res.get(), i, 1) + 2);
        auto ledger = context.ledgerMaster.getLedgerBySeq(ledgerSequence);
        if (ledger)
        {
            auto [sttx, txnMeta] = ledger->txRead(txID);
            if (!sttx)
            {
                Json::Value err;
                err[jss::error] = "Transaction not found in ledger. ledger = " +
                    std::to_string(ledgerSequence) +
                    " . txnID = " + strHex(txID);
                txs.append(err);
                continue;
            }

            txs.append(sttx->getJson(JsonOptions::none));
        }
        else
        {
            Json::Value err;
            err[jss::error] =
                "Ledger not found : " + std::to_string(ledgerSequence);
            txs.append(err);
        }
    }

    Json::Value obj;
    obj[jss::index] = startIndex;
    obj[jss::txs] = txs;
    obj["used_postgres"] = true;

    return obj;
}

// {
//   start: <index>
// }
Json::Value
doTxHistory(RPC::JsonContext& context)
{
    if (!context.app.config().useTxTables())
        return rpcError(rpcNOT_ENABLED);

    if (context.app.config().usePostgresLedgerTx())
        return doTxHistoryReporting(context);
    context.loadType = Resource::feeMediumBurdenRPC;

    if (!context.params.isMember(jss::start))
        return rpcError(rpcINVALID_PARAMS);

    unsigned int startIndex = context.params[jss::start].asUInt();

    if ((startIndex > 10000) && (!isUnlimited(context.role)))
        return rpcError(rpcNO_PERMISSION);

    Json::Value obj;
    Json::Value txs;

    obj[jss::index] = startIndex;

    std::string sql = boost::str(
        boost::format(
            "SELECT LedgerSeq, Status, RawTxn "
            "FROM Transactions ORDER BY LedgerSeq desc LIMIT %u,20;") %
        startIndex);

    {
        auto db = context.app.getTxnDB().checkoutDb();

        boost::optional<std::uint64_t> ledgerSeq;
        boost::optional<std::string> status;
        soci::blob sociRawTxnBlob(*db);
        soci::indicator rti;
        Blob rawTxn;

        soci::statement st =
            (db->prepare << sql,
             soci::into(ledgerSeq),
             soci::into(status),
             soci::into(sociRawTxnBlob, rti));

        st.execute();
        while (st.fetch())
        {
            if (soci::i_ok == rti)
                convert(sociRawTxnBlob, rawTxn);
            else
                rawTxn.clear();

            if (auto trans = Transaction::transactionFromSQL(
                    ledgerSeq, status, rawTxn, context.app))
                txs.append(trans->getJson(JsonOptions::none));
        }
    }

    obj[jss::txs] = txs;

    return obj;
}

}  // namespace ripple
