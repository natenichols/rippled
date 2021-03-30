#include <ripple/rpc/Context.h>
#include <ripple/rpc/GRPCHandlers.h>
#include <ripple/rpc/impl/RPCHelpers.h>

namespace ripple {
Json::Value
doLedgerDiff(RPC::JsonContext& context)
{
    Json::Value& request = context.params;
    Json::Value response;

    std::shared_ptr<ReadView const> baseLedgerRv;
    std::shared_ptr<ReadView const> desiredLedgerRv;

    if (RPC::lookupLedger(baseLedgerRv, context, request["base_ledger"]))
    {
        return rpcLGR_NOT_FOUND;
    }

    if (RPC::lookupLedger(desiredLedgerRv, context, request["desired_ledger"]))
    {
        return rpcLGR_NOT_FOUND;
    }

    std::shared_ptr<Ledger const> baseLedger =
        std::dynamic_pointer_cast<Ledger const>(baseLedgerRv);
    if (!baseLedger)
    {
        return rpcLGR_NOT_VALIDATED;
    }

    std::shared_ptr<Ledger const> desiredLedger =
        std::dynamic_pointer_cast<Ledger const>(desiredLedgerRv);
    if (!desiredLedger)
    {
        return rpcLGR_NOT_VALIDATED;
    }

    SHAMap::Delta differences;

    int maxDifferences = std::numeric_limits<int>::max();

    bool res = baseLedger->stateMap().compare(
        desiredLedger->stateMap(), differences, maxDifferences);
    if (!res)
    {
        return rpcEXCESSIVE_LGR_RANGE;
    }
    Json::Value arr = Json::arrayValue;

    for (auto& [k, v] : differences)
    {
        Json::Value diff = Json::objectValue;
        auto inBase = v.first;
        auto inDesired = v.second;

        // key does not exist in desired
        if (!inDesired)
        {
            diff["key"] = ripple::strHex(k);
        }
        else
        {
            assert(inDesired->size() > 0);
            diff["key"] = ripple::strHex(k);
            if (request["include_blobs"])
            {
                diff["data"] = ripple::strHex(
                    inDesired->slice().data(),
                    inDesired->slice().data() + inDesired->size());
            }
        }
        arr.append(std::move(diff));
    }
    response["objects"] = arr;
    return response;
}
std::pair<org::xrpl::rpc::v1::GetLedgerDiffResponse, grpc::Status>
doLedgerDiffGrpc(
    RPC::GRPCContext<org::xrpl::rpc::v1::GetLedgerDiffRequest>& context)
{
    org::xrpl::rpc::v1::GetLedgerDiffRequest& request = context.params;
    org::xrpl::rpc::v1::GetLedgerDiffResponse response;
    grpc::Status status = grpc::Status::OK;

    std::shared_ptr<ReadView const> baseLedgerRv;
    std::shared_ptr<ReadView const> desiredLedgerRv;

    if (RPC::ledgerFromSpecifier(baseLedgerRv, request.base_ledger(), context))
    {
        grpc::Status errorStatus{
            grpc::StatusCode::NOT_FOUND, "base ledger not found"};
        return {response, errorStatus};
    }

    if (RPC::ledgerFromSpecifier(
            desiredLedgerRv, request.desired_ledger(), context))
    {
        grpc::Status errorStatus{
            grpc::StatusCode::NOT_FOUND, "desired ledger not found"};
        return {response, errorStatus};
    }

    std::shared_ptr<Ledger const> baseLedger =
        std::dynamic_pointer_cast<Ledger const>(baseLedgerRv);
    if (!baseLedger)
    {
        grpc::Status errorStatus{
            grpc::StatusCode::NOT_FOUND, "base ledger not validated"};
        return {response, errorStatus};
    }

    std::shared_ptr<Ledger const> desiredLedger =
        std::dynamic_pointer_cast<Ledger const>(desiredLedgerRv);
    if (!desiredLedger)
    {
        grpc::Status errorStatus{
            grpc::StatusCode::NOT_FOUND, "base ledger not validated"};
        return {response, errorStatus};
    }

    SHAMap::Delta differences;

    int maxDifferences = std::numeric_limits<int>::max();

    bool res = baseLedger->stateMap().compare(
        desiredLedger->stateMap(), differences, maxDifferences);
    if (!res)
    {
        grpc::Status errorStatus{
            grpc::StatusCode::RESOURCE_EXHAUSTED,
            "too many differences between specified ledgers"};
        return {response, errorStatus};
    }

    for (auto& [k, v] : differences)
    {
        auto diff = response.mutable_ledger_objects()->add_objects();
        auto inBase = v.first;
        auto inDesired = v.second;

        // key does not exist in desired
        if (!inDesired)
        {
            diff->set_key(k.data(), k.size());
        }
        else
        {
            assert(inDesired->size() > 0);
            diff->set_key(k.data(), k.size());
            if (request.include_blobs())
            {
                diff->set_data(inDesired->data(), inDesired->size());
            }
        }
    }
    return {response, status};
}

}  // namespace ripple
