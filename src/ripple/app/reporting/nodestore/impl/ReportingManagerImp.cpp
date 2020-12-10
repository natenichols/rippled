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

#include <ripple/app/reporting/nodestore/impl/ReportingManagerImp.h>

#include <boost/algorithm/string/predicate.hpp>
#include <memory>

namespace ripple {

namespace NodeStore {

ReportingManagerImp&
ReportingManagerImp::instance()
{
    static ReportingManagerImp _;
    return _;
}

void
ReportingManagerImp::missing_backend()
{
    Throw<std::runtime_error>(
        "Your rippled.cfg is missing a [node_db] entry, "
        "please see the rippled-example.cfg file!");
}

std::unique_ptr<ReportingBackend>
ReportingManagerImp::make_ReportingBackend(
    Section const& parameters,
    beast::Journal journal)
{
    std::string const type{get<std::string>(parameters, "type")};
    if (type.empty())
        missing_backend();

    auto factory{find(type)};
    if (!factory)
    {
        missing_backend();
    }

    return factory->createInstance(
        NodeObject::keyBytes, parameters, journal);
}

void
ReportingManagerImp::insert(ReportingFactory& factory)
{
    std::lock_guard _(mutex_);
    list_.push_back(&factory);
}

void
ReportingManagerImp::erase(ReportingFactory& factory)
{
    std::lock_guard _(mutex_);
    auto const iter =
        std::find_if(list_.begin(), list_.end(), [&factory](ReportingFactory* other) {
            return other == &factory;
        });
    assert(iter != list_.end());
    list_.erase(iter);
}

ReportingFactory*
ReportingManagerImp::find(std::string const& name)
{
    std::lock_guard _(mutex_);
    auto const iter =
        std::find_if(list_.begin(), list_.end(), [&name](ReportingFactory* other) {
            return boost::iequals(name, other->getName());
        });
    if (iter == list_.end())
        return nullptr;
    return *iter;
}

//------------------------------------------------------------------------------

ReportingManager&
ReportingManager::instance()
{
    return ReportingManagerImp::instance();
}

//------------------------------------------------------------------------------

std::unique_ptr<ReportingBackend>
make_ReportingBackend(
    Section const& config,
    beast::Journal journal)
{
    return ReportingManager::instance().make_ReportingBackend(
        config, journal);
}

}  // namespace NodeStore
}  // namespace ripple
