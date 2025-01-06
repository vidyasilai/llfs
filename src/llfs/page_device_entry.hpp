//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_DEVICE_ENTRY_HPP
#define LLFS_PAGE_DEVICE_ENTRY_HPP

#include <llfs/bloom_filter_page.hpp>
#include <llfs/no_outgoing_refs_cache.hpp>
#include <llfs/page_arena.hpp>
#include <llfs/page_device_cache.hpp>

#include <memory>
#include <vector>

namespace llfs {

/** \brief All the per-PageDevice state for a single device in the storage pool.
 */
struct PageDeviceEntry {
  explicit PageDeviceEntry(PageArena&& arena,
                           boost::intrusive_ptr<PageCacheSlot::Pool>&& slot_pool) noexcept
      : arena{std::move(arena)}
      , cache{this->arena.device().page_ids(), std::move(slot_pool)}
      , no_outgoing_refs_cache{this->arena.device().page_ids()}
      , page_filters{this->arena.device().page_size(), this->arena.device().page_ids()}
  {
  }

  /** \brief The PageDevice and PageAllocator.
   */
  PageArena arena;

  /** \brief A per-device page cache; shares a PageCacheSlot::Pool with all other PageDeviceEntry
   * objects that have the same page size.
   */
  PageDeviceCache cache;

  /** \brief A per-device tracker of the outgoing reference status of each physical page in the
   * device.
   */
  NoOutgoingRefsCache no_outgoing_refs_cache;

  BloomFilterPages page_filters;
};
}  // namespace llfs

#endif  // LLFS_PAGE_DEVICE_ENTRY_HPP
