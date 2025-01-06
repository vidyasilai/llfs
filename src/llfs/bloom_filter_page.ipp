//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_BLOOM_FILTER_PAGE_IPP
#define LLFS_BLOOM_FILTER_PAGE_IPP

namespace llfs {

template <typename GetKeyItems>
inline void BloomFilterPages::create_filter_page(PageId page_id, const GetKeyItems& items)
{
  BATT_CHECK_EQ(PageIdFactory::get_device_id(page_id), this->page_ids_.get_device_id());

  BloomFilterParams params = {.bits_per_item = batt::round_down_to<usize>(
                                  usize{1}, this->filter_page_size_ / items.size())};
  std::shared_ptr<PageBuffer> buffer = PageBuffer::allocate(this->filter_page_size_);

  PackedPageHeader* const header = mutable_page_header(buffer.get());
  header->layout_id = [] {
    llfs::PageLayoutId id;

    const char tag[sizeof(id.value) + 1] = "(filter)";

    std::memcpy(&id.value, tag, sizeof(id.value));

    return id;
  }();

  DataPacker dst{buffer->mutable_payload()};
  Optional<MutableBuffer> filter_buffer =
      dst.reserve_front(packed_sizeof_bloom_filter(params, items.size()));
  BATT_CHECK(filter_buffer);
  PackedBloomFilter* filter = reinterpret_cast<PackedBloomFilter*>(filter_buffer->data());

  filter->initialize(params, items.size(), page_id);
  parallel_build_bloom_filter(batt::WorkerPool::default_pool(), std::begin(items), std::end(items),
                              /*hash_fn=*/BATT_OVERLOADS_OF(get_key), filter);

  i64 physical_page_id = this->page_ids_.get_physical_page(page_id);
  BATT_CHECK_LT((usize)physical_page_id, this->pages_.size());
  this->pages_[physical_page_id].buffer = buffer;

  page_generation_int generation = this->page_ids_.get_generation(page_id);
  this->pages_[physical_page_id].generation.set_value(generation);
}
}  // namespace llfs

#endif  // LLFS_BLOOM_FILTER_PAGE_IPP