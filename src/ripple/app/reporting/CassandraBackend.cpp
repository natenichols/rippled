#include<ripple/app/reporting/CassandraBackend.h>
#include<ripple/basics/strHex.h>

namespace ripple {
namespace NodeStore {

void
readCallback(CassFuture* fut, void* cbData)
{

    CassandraBackend::ReadCallbackData& requestParams =
        *static_cast<CassandraBackend::ReadCallbackData*>(cbData);

    CassError rc = cass_future_error_code(fut);

    if (rc != CASS_OK)
    {
        JLOG(requestParams.backend.j_.warn()) << "Cassandra fetch error : "
            << rc << " : " << cass_error_desc(rc) << " - retrying";
        // Retry right away. The only time the cluster should ever be overloaded
        // is when the very first ledger is being written in full (millions of
        // writes at once), during which no reads should be occurring. If reads
        // are timing out, the code/architecture should be modified to handle
        // greater read load, as opposed to just exponential backoff
        requestParams.backend.read(requestParams);
    }
    else
    {
        auto finish = [&requestParams]() {
            ++(requestParams.numFinished);
            if(requestParams.numFinished == requestParams.batchSize)
                requestParams.cv.notify_all();
        };
        CassResult const* res = cass_future_get_result(fut);

        CassRow const* row = cass_result_first_row(res);
        if (!row)
        {
            cass_result_free(res);
            JLOG(requestParams.backend.j_.error())
                << "Cassandra fetch get row error : " << rc << ", "
                << cass_error_desc(rc);
            finish();
            return;
        }
        cass_byte_t const* buf;
        std::size_t bufSize;
        rc = cass_value_get_bytes(cass_row_get_column(row, 0), &buf, &bufSize);
        if (rc != CASS_OK)
        {
            cass_result_free(res);
            JLOG(requestParams.backend.j_.error())
                << "Cassandra fetch get bytes error : " << rc << ", "
                << cass_error_desc(rc);
            ++requestParams.backend.counters_.readErrors;
            finish();
            return;
        }
        // nudb::detail::buffer bf;
        // std::pair<void const*, std::size_t> uncompressed =
        //     nodeobject_decompress(buf, bufSize, bf);
        // DecodedBlob decoded(
        //     requestParams.hash.begin(), uncompressed.first, uncompressed.second);

        Blob decoded(buf, buf + bufSize);        
        cass_result_free(res);

        requestParams.result = std::make_shared<Blob>(std::move(decoded));
        finish();
    }
}

void
writeCallback(CassFuture* fut, void* cbData)
{
    CassandraBackend::WriteCallbackData& requestParams =
        *static_cast<CassandraBackend::WriteCallbackData*>(cbData);
    CassandraBackend& backend = *requestParams.backend;
    auto rc = cass_future_error_code(fut);
    if (rc != CASS_OK)
    {
        JLOG(backend.j_.error())
            << "ERROR!!! Cassandra insert error: " << rc << ", "
            << cass_error_desc(rc) << ", retrying ";
        ++requestParams.totalWriteRetries;
        // exponential backoff with a max wait of 2^10 ms (about 1 second)
        auto wait = std::chrono::milliseconds(
            lround(std::pow(2, std::min(10u, requestParams.currentRetries))));
        ++requestParams.currentRetries;
        std::shared_ptr<boost::asio::steady_timer> timer =
            std::make_shared<boost::asio::steady_timer>(
                backend.ioContext_,
                    std::chrono::steady_clock::now() + wait);
        timer->async_wait([timer, &requestParams, &backend](
                              const boost::system::error_code& error) {
            backend.write(requestParams, true);
        });
    }
    else
    {
        backend.counters_.writeDurationUs +=
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - requestParams.begin)
                .count();
        --(backend.numRequestsOutstanding_);

        backend.throttleCv_.notify_all();
        if (backend.numRequestsOutstanding_ == 0)
            backend.syncCv_.notify_all();
        delete &requestParams;
    }
}

}
}