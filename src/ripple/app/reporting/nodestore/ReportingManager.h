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

#ifndef RIPPLE_NODESTORE_REPORTING_MANAGER_H_INCLUDED
#define RIPPLE_NODESTORE_REPORTING_MANAGER_H_INCLUDED

#include <ripple/app/reporting/nodestore/ReportingFactory.h>

namespace ripple {

namespace NodeStore {

/** Singleton for managing NodeStore factories and back ends. */
class ReportingManager
{
public:
    virtual ~ReportingManager() = default;
    ReportingManager() = default;
    ReportingManager(ReportingManager const&) = delete;
    ReportingManager&
    operator=(ReportingManager const&) = delete;

    /** Returns the instance of the manager singleton. */
    static ReportingManager&
    instance();

    /** Add a factory. */
    virtual void
    insert(ReportingFactory& factory) = 0;

    /** Remove a factory. */
    virtual void
    erase(ReportingFactory& factory) = 0;

    /** Return a pointer to the matching factory if it exists.
        @param  name The name to match, performed case-insensitive.
        @return `nullptr` if a match was not found.
    */
    virtual ReportingFactory*
    find(std::string const& name) = 0;

    /** Create a backend. */
    virtual std::unique_ptr<ReportingBackend>
    make_ReportingBackend(
        Section const& parameters,
        beast::Journal journal) = 0;
};

//------------------------------------------------------------------------------

/** Create a ReportingBackend. */
std::unique_ptr<ReportingBackend>
make_ReportingBackend(
    Section const& config,
    beast::Journal journal);

}  // namespace NodeStore
}  // namespace ripple

#endif
