//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_TRACE_REFS_RECURSIVE_HPP
#define LLFS_TRACE_REFS_RECURSIVE_HPP

#include <llfs/page_id.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/pinned_page.hpp>

#include <batteries/seq.hpp>
#include <batteries/status.hpp>
#include <batteries/utility.hpp>

#include <type_traits>
#include <unordered_set>
#include <vector>

namespace llfs {

template <typename IdTypePairSeq, typename Pred, typename Fn>
inline batt::Status trace_refs_recursive(PageLoader& page_loader, IdTypePairSeq&& roots,
                                         Pred&& should_recursively_trace, Fn&& fn)
{
  static_assert(std::is_convertible_v<std::decay_t<SeqItem<IdTypePairSeq>>, PageId>,
                "`roots` arg must be a Seq of PageViewId");

  std::unordered_set<PageId, PageId::Hash> pushed;
  std::vector<PageId> pending;

  BATT_FORWARD(roots) | seq::emplace_back(&pending);

  for (const PageId& page_id : pending) {
    pushed.insert(page_id);
  }

  while (!pending.empty()) {
    const PageId next = pending.back();
    pending.pop_back();

    batt::StatusOr<PinnedPage> status_or_page = page_loader.get_page(next, OkIfNotFound{false});
    BATT_REQUIRE_OK(status_or_page);

    PinnedPage& page = *status_or_page;
    BATT_CHECK_NOT_NULLPTR(page);
    page->trace_refs() | seq::for_each([&](const PageId& id) {
      fn(id);
      if (!pushed.count(id) && should_recursively_trace(id)) {
        pushed.insert(id);
        pending.push_back(id);
      }
    });
  }

  return batt::OkStatus();
}

template <typename IdTypePairSeq, typename Pred, typename Fn>
inline batt::StatusOr<std::unordered_map<PageId, u64, PageId::Hash>> trace_refs_depth_recursive(PageLoader& page_loader, IdTypePairSeq&& roots,
                                         Pred&& should_recursively_trace, Fn&& fn)
{
  static_assert(std::is_convertible_v<std::decay_t<SeqItem<IdTypePairSeq>>, PageId>,
                "`roots` arg must be a Seq of PageViewId");

  std::unordered_set<PageId, PageId::Hash> pushed;
  std::deque<PageId> pending;
  std::unordered_map<PageId, u64, PageId::Hash> ref_depths;

  BATT_FORWARD(roots) | seq::emplace_back(&pending);

  for (const PageId& page_id : pending) {
    pushed.insert(page_id);
    ref_depths[page_id] = 0;
  }

  while (!pending.empty()) {
    const PageId next = pending.front();
    pending.pop_front();

    batt::StatusOr<PinnedPage> status_or_page = page_loader.get_page(next, OkIfNotFound{false});
    BATT_REQUIRE_OK(status_or_page);

    PinnedPage& page = *status_or_page;
    BATT_CHECK_NOT_NULLPTR(page);
    page->trace_refs() | seq::for_each([&](const PageId& id) {
      fn(id);
      if (!pushed.count(id) && should_recursively_trace(id)) {
        pushed.insert(id);
        pending.push_back(id);
        ref_depths[id] = ref_depths[next] + 1;
      }
    });
  }

  return ref_depths;
}

}  // namespace llfs

#endif  // LLFS_TRACE_REFS_RECURSIVE_HPP
