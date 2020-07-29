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

#include <ripple/app/reporting/ReportingETL.h>
#include <ripple/app/reporting/TxProxy.h>
#include <ripple/json/json_reader.h>
#include <ripple/json/json_writer.h>

namespace ripple {

Json::Value
forwardToTx(RPC::JsonContext& context)
{
    return context.app.getReportingETL().getETLLoadBalancer().forwardToTx(
        context);
}

std::unique_ptr<org::xrpl::rpc::v1::XRPLedgerAPIService::Stub>
getForwardingStub(RPC::Context& context)
{
    return context.app.getReportingETL().getETLLoadBalancer().getForwardingStub(
        context);
}

// We only forward requests where ledger_index is "current" or "closed"
// otherwise, attempt to handle here
bool
shouldForwardToTx(RPC::JsonContext& context)
{
    if (!context.app.config().reporting())
        return false;

    Json::Value& params = context.params;
    std::string strCommand = params.isMember(jss::command)
        ? params[jss::command].asString()
        : params[jss::method].asString();

    JLOG(context.j.trace()) << "COMMAND:" << strCommand;
    JLOG(context.j.trace()) << "REQUEST:" << params;
    auto handler = RPC::getHandler(context.apiVersion, strCommand);
    if (!handler)
    {
        JLOG(context.j.error())
            << "Error getting handler. command = " << strCommand;
        return false;
    }

    if (handler->condition_ == RPC::NEEDS_CURRENT_LEDGER ||
        handler->condition_ == RPC::NEEDS_CLOSED_LEDGER)
    {
        return true;
    }
    // TODO consider forwarding sequence values greater than the
    // latest sequence we have
    if (params.isMember(jss::ledger_index))
    {
        auto indexValue = params[jss::ledger_index];
        if (!indexValue.isNumeric())
        {
            auto index = indexValue.asString();
            return index == "current" || index == "closed";
        }
    }
    return false;
}

}  // namespace ripple
