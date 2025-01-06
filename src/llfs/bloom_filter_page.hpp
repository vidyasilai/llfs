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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Data associated with a bloom filter page.
 */
struct BloomFilterPageEntry {
  /** \brief The page buffer that stores the bloom filter.
   */
  std::shared_ptr<PageBuffer> buffer;

  /** \brief The generation of the data page that this bloom filter page corresponds to. The
   * batt::Watch helps with synchronization between the batt::Task that creates the filter pages,
   * and other Tasks trying to query into the filter.
   */
  batt::Watch<u64> generation{0};
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A container of all the bloom filter pages for a PageDevice of data pages. This class
 * provides the interface to create and use the bloom filters.
 */
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
  /** \brief The size of the data page for the corresponding PageDevice.
   */
  const PageSize data_page_size_;

  /** \brief The size of the filter page for the corresponding data page.
   */
  const PageSize filter_page_size_;

  /** \brief A PageIdFactory object to help resolve physical page and generation values from a
   * PageId object.
   */
  const PageIdFactory page_ids_;

  /** \brief The collection of all bloom filter pages, indexed by the physical page id of the
   * corresponding data page.
   */
  std::vector<BloomFilterPageEntry> pages_;
};

}  // namespace llfs

#include <llfs/bloom_filter_page.ipp>

#endif  // LLFS_BLOOM_FILTER_PAGE_HPP