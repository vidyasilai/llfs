//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/bloom_filter_page.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BloomFilterPages::BloomFilterPages(PageSize data_page_size, const PageIdFactory& page_ids) noexcept
    : data_page_size_{data_page_size}
    , filter_page_size_{calculate_filter_page_size()}
    , page_ids_{page_ids}
    , pages_{this->page_ids_.get_physical_page_count().value()}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize BloomFilterPages::calculate_filter_size()
{
  static const double false_positive_rate = 0.1;
  const double filter_size_bits =
      -(this->data_page_size_ * std::log(false_positive_rate)) / (std::log(2) * std::log(2));
  usize rounded_filter_size = usize{1} << batt::log2_ceil(usize(filter_size_bits));
  return rounded_filter_size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageSize BloomFilterPages::calculate_filter_page_size()
{
  const usize rounded_filter_size = this->calculate_filter_size();
  usize total_page_size = (rounded_filter_size / 8) + sizeof(PackedPageHeader);
  usize total_page_size_log2 = batt::log2_ceil(total_page_size);
  if (total_page_size_log2 < batt::log2_ceil(512)) {
    return PageSize{512};
  } else if (total_page_size_log2 >= kMaxPageSizeLog2) {
    return PageSize{u32{1} << (kMaxPageSizeLog2 - 1)};
  } else {
    return PageSize{u32{1} << total_page_size_log2};
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::shared_ptr<PageBuffer> BloomFilterPages::get_bloom_filter_page_buffer(PageId page_id)
{
  BATT_CHECK_EQ(PageIdFactory::get_device_id(page_id), this->page_ids_.get_device_id());

  i64 physical_page_id = this->page_ids_.get_physical_page(page_id);
  BATT_CHECK_LT((usize)physical_page_id, this->pages_.size());

  page_generation_int generation = this->page_ids_.get_generation(page_id);
  batt::Status await_generation_status =
      this->pages_[physical_page_id].generation.await_equal(generation);
  BATT_CHECK_OK(await_generation_status);

  return this->pages_[physical_page_id].buffer;
}

}  //namespace llfs