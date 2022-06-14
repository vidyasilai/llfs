#pragma once
#ifndef LLFS_PAGE_LAYOUT_HPP
#define LLFS_PAGE_LAYOUT_HPP

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Must be first
#include <glog/logging.h>
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/buffer.hpp>
#include <llfs/data_layout.hpp>
#include <llfs/packed_array.hpp>
#include <llfs/packed_page_id.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_layout_id.hpp>
#include <llfs/page_ref_count.hpp>
#include <llfs/page_view.hpp>

#include <batteries/static_assert.hpp>
#include <batteries/type_traits.hpp>

#include <boost/uuid/uuid.hpp>

#include <cstddef>
#include <string_view>

namespace llfs {

// Offset is circular; thus the log can never be bigger than 2^63-1, but we
// will never need more bits.
//
using slot_offset_type = u64;

using PackedSlotOffset = little_u64;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Represents a user-specified logical timestamp for a page.  The `user` here isn't necessarily an
// end-user or human, it could be another part of the system (e.g., a Tablet).
//
struct PackedPageUserSlot {
  boost::uuids::uuid user_id;
  PackedSlotOffset slot_offset;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageUserSlot), 24);

std::ostream& operator<<(std::ostream& out, const PackedPageUserSlot& t);

struct PackedPageHeader;

std::ostream& operator<<(std::ostream& out, const PackedPageHeader& t);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct PackedPageHeader {
  static constexpr u64 kMagic = 0x35f2e78c6a06fc2bull;
  static constexpr u32 kCrc32NotSet = 0xdeadcc32ul;

  u32 unused_size() const noexcept
  {
    BATT_CHECK_LE(this->unused_begin, this->unused_end) << *this;
    return this->unused_end - this->unused_begin;
  }

  usize used_size() const noexcept
  {
    return this->size - this->unused_size();
  }

  big_u64 magic;
  PackedPageId page_id;
  PageLayoutId layout_id;
  little_u32 crc32;
  little_u32 unused_begin;
  little_u32 unused_end;
  PackedPageUserSlot user_slot;
  little_u32 size;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageHeader), 64);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//

inline const PackedPageHeader& get_page_header(const PageBuffer& page)
{
  return *reinterpret_cast<const PackedPageHeader*>(&page);
}

inline PackedPageHeader* mutable_page_header(PageBuffer* page)
{
  return reinterpret_cast<PackedPageHeader*>(page);
}

// Compute the crc64 for the given page.
//
u64 compute_page_crc64(const PageBuffer& page);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

// Finalize a page by computing its crc64 and user slot information.
//
Status finalize_page_header(PageBuffer* page,
                            const Interval<u64>& unused_region = Interval<u64>{0u, 0u});

template <typename Dst>
[[nodiscard]] PackedPageId* pack_object_to(const PageId& id, PackedPageId* packed_id, Dst*)
{
  packed_id->id_val = id.int_value();

  BATT_CHECK_EQ(packed_id->id_val, id.int_value());

  return packed_id;
}

template <typename Src>
inline StatusOr<PageId> unpack_object(const PackedPageId& packed_id, Src*)
{
  return PageId{packed_id.id_val};
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

struct PackedPageRefCount {
  little_page_id_int page_id;
  little_i32 ref_count;

  PageRefCount as_page_ref_count() const
  {
    return PageRefCount{
        .page_id = page_id,
        .ref_count = ref_count,
    };
  }
};

LLFS_DEFINE_PACKED_TYPE_FOR(PageRefCount, PackedPageRefCount);
LLFS_DEFINE_PACKED_TYPE_FOR(PackedPageRefCount, PackedPageRefCount);

std::ostream& operator<<(std::ostream& out, const PackedPageRefCount& t);

inline usize packed_sizeof(const PackedPageRefCount&)
{
  return packed_sizeof(batt::StaticType<PackedPageRefCount>{});
}

template <typename Dst>
[[nodiscard]] bool pack_object_to(const PageRefCount& prc, PackedPageRefCount* packed, Dst*)
{
  packed->page_id = prc.page_id;
  packed->ref_count = prc.ref_count;
  return true;
}

template <typename Dst>
[[nodiscard]] bool pack_object_to(const PackedPageRefCount& from, PackedPageRefCount* to, Dst*)
{
  *to = from;
  return true;
}

}  // namespace llfs

#endif  // LLFS_PAGE_LAYOUT_HPP
