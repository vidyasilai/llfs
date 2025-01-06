//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_BLOOM_FILTER_PAGE_HPP
#define LLFS_BLOOM_FILTER_PAGE_HPP

#include <llfs/api_types.hpp>
#include <llfs/bloom_filter.hpp>
#include <llfs/data_packer.hpp>
#include <llfs/packed_page_header.hpp>
#include <llfs/page_buffer.hpp>

namespace llfs {

struct BloomFilterPageEntry {
  std::shared_ptr<PageBuffer> buffer;
  batt::Watch<u64> generation{0};
};

class BloomFilterPages
{
 public:
  explicit BloomFilterPages(PageSize data_page_size, const PageIdFactory& page_ids) noexcept;

  BloomFilterPages(const BloomFilterPages&) = delete;
  BloomFilterPages& operator=(const BloomFilterPages&) = delete;

  usize calculate_filter_size();

  PageSize calculate_filter_page_size();

  template <typename GetKeyItems>
  void create_filter_page(PageId page_id, const GetKeyItems& items);

  std::shared_ptr<PageBuffer> get_bloom_filter_page_buffer(PageId page_id);

 private:
  const PageSize data_page_size_;

  const PageSize filter_page_size_;

  const PageIdFactory page_ids_;

  std::vector<BloomFilterPageEntry> pages_;
};

}  // namespace llfs

#include <llfs/bloom_filter_page.ipp>

#endif  // LLFS_BLOOM_FILTER_PAGE_HPP