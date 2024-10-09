//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_STATE_NO_LOCK_HPP
#define LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_STATE_NO_LOCK_HPP

#include <llfs/page_allocator_ref_count.hpp>

#include <llfs/int_types.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/page_tracer.hpp>
#include <llfs/slot.hpp>
#include <llfs/status.hpp>

#include <memory>
#include <utility>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Base class of PageAllocatorState comprised of state that is safe to access without
 * holding a mutex lock.
 */
class PageAllocatorStateNoLock
{
 public:
  explicit PageAllocatorStateNoLock(const PageIdFactory& ids) noexcept;

  const PageIdFactory& page_ids() const;

  StatusOr<slot_offset_type> await_learned_slot(slot_offset_type min_learned_upper_bound);

  slot_offset_type learned_upper_bound() const;

  Status await_free_page();

  template <typename HandlerFn = void(Status)>
  void async_wait_free_page(HandlerFn&& handler);

  u64 page_device_capacity() const noexcept;

  u64 free_pool_size() noexcept;

  PageAllocatorRefCountStatus get_ref_count_status(PageId id) const noexcept;

  void halt() noexcept;

  NoOutgoingRefsCache* no_outgoing_refs_cache()
  {
    return this->no_outgoing_refs_cache_.get();
  }

 protected:
  // Returns the index of `ref_count` in the `page_ref_counts_` array (which is also the physical
  // page number for that page's device).  Panic if `ref_count` is not in our ref counts array.
  //
  isize index_of(const PageAllocatorRefCount* ref_count) const;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::Watch<slot_offset_type> learned_upper_bound_{0};

  // The size of the free pool; used to allow blocking on free pages becoming available.
  //
  batt::Watch<u64> free_pool_size_{0};

  // The number of pages addressable by the device.
  //
  const PageIdFactory page_ids_;

  // The array of page ref counts, indexed by the page id.
  //
  const std::unique_ptr<PageAllocatorRefCount[]> page_ref_counts_{
      new PageAllocatorRefCount[this->page_device_capacity()]};

  // The cache maintained by PageCache and PageAllocator to store outgoing refs information for
  // pages.
  //
  std::unique_ptr<NoOutgoingRefsCache> no_outgoing_refs_cache_;
};

}  // namespace llfs

#include <llfs/page_allocator_state_no_lock.ipp>

#endif  // LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_STATE_NO_LOCK_HPP
