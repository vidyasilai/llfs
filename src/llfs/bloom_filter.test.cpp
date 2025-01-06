//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/bloom_filter.hpp>
//
#include <llfs/bloom_filter.hpp>

#include <llfs/memory_log_device.hpp>
#include <llfs/memory_page_cache.hpp>
#include <llfs/metrics.hpp>
#include <llfs/page_cache_job.hpp>
#include <llfs/page_reader.hpp>
#include <llfs/page_view.hpp>
#include <llfs/slice.hpp>
#include <llfs/volume.hpp>

#include <llfs/testing/fake_log_device.hpp>

#include <random>
#include <sstream>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using llfs::as_slice;
using llfs::BloomFilterParams;
using llfs::LatencyMetric;
using llfs::LatencyTimer;
using llfs::packed_sizeof_bloom_filter;
using llfs::PackedBloomFilter;
using llfs::parallel_build_bloom_filter;

using namespace llfs::int_types;
using namespace llfs::constants;

using batt::WorkerPool;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <typename Rng>
std::string make_random_word(Rng& rng)
{
  static std::exponential_distribution<> pick_negative_len;
  static std::uniform_int_distribution<char> pick_letter('a', 'z');

  static constexpr double kMaxLen = 12;

  double nl = std::min(kMaxLen - 1, pick_negative_len(rng));
  usize len = kMaxLen - nl + 1;
  std::ostringstream oss;
  for (usize i = 0; i < len; ++i) {
    oss << pick_letter(rng);
  }
  return std::move(oss).str();
}

struct QueryStats {
  usize total = 0;
  usize false_positive = 0;
  double expected_rate = 0;

  double actual_rate() const
  {
    return double(this->false_positive) / double(this->total);
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

TEST(BloomFilterTest, RandomItems)
{
  std::default_random_engine rng{15324987};

  std::map<std::pair<u64, u16>, QueryStats> stats;

  LatencyMetric build_latency;
  LatencyMetric query_latency;

  double false_positive_rate_total = 0.0;
  double false_positive_rate_count = 0.0;

  for (usize r = 0; r < 10; ++r) {
    std::vector<std::string> items;
    const usize n_items = 10 * 1000;
    for (usize i = 0; i < n_items; ++i) {
      items.emplace_back(make_random_word(rng));
    }
    for (const auto& s : as_slice(items.data(), 40)) {
      LLFS_VLOG(1) << "sample random word: " << s;
    }
    std::sort(items.begin(), items.end());

    for (usize bits_per_item = 1; bits_per_item < 16; ++bits_per_item) {
      const BloomFilterParams params{
          .bits_per_item = bits_per_item,
      };

      std::unique_ptr<u8[]> memory{new u8[packed_sizeof_bloom_filter(params, items.size())]};
      PackedBloomFilter* filter = (PackedBloomFilter*)memory.get();
      *filter = PackedBloomFilter::from_params(params, items.size());

      const double actual_bit_rate = double(filter->word_count() * 64) / double(items.size());

      LLFS_VLOG(1) << BATT_INSPECT(n_items) << " (target)" << BATT_INSPECT(bits_per_item)
                   << BATT_INSPECT(filter->word_count_mask) << BATT_INSPECT(filter->hash_count)
                   << " bit_rate == " << actual_bit_rate;

      {
        LatencyTimer build_timer{build_latency, items.size()};

        parallel_build_bloom_filter(
            WorkerPool::default_pool(), items.begin(), items.end(),
            [](const auto& v) -> decltype(auto) {
              return v;
            },
            filter);
      }

      for (const std::string& s : items) {
        EXPECT_TRUE(filter->might_contain(s));
      }

      const auto items_contains = [&items](const std::string& s) {
        auto iter = std::lower_bound(items.begin(), items.end(), s);
        return iter != items.end() && *iter == s;
      };

      std::pair<u64, u16> config_key{filter->word_count(), filter->hash_count};
      QueryStats& c_stats = stats[config_key];
      {
        const double k = filter->hash_count;
        const double n = items.size();
        const double m = filter->word_count() * 64;

        c_stats.expected_rate = std::pow(1 - std::exp(-((k * (n + 0.5)) / (m - 1))), k);
      }

      for (usize j = 0; j < n_items * 10; ++j) {
        std::string query = make_random_word(rng);
        c_stats.total += 1;
        const bool ans = LLFS_COLLECT_LATENCY_N(query_latency, filter->might_contain(query),
                                                u64(std::ceil(actual_bit_rate)));
        if (ans && !items_contains(query)) {
          c_stats.false_positive += 1;
        }
      }

      false_positive_rate_count += 1.0;
      false_positive_rate_total += (c_stats.actual_rate() / c_stats.expected_rate);
    }
  }

  for (const auto& s : stats) {
    const u64 word_count = s.first.first;
    const u16 hash_count = s.first.second;

    // fpr == false positive rate
    //
    const double actual_fpr = s.second.actual_rate();
    const double expected_fpr = s.second.expected_rate;

    EXPECT_LT(actual_fpr / expected_fpr, 1.02)
        << BATT_INSPECT(actual_fpr) << BATT_INSPECT(expected_fpr);

    EXPECT_GT(expected_fpr / actual_fpr, 0.98)
        << BATT_INSPECT(actual_fpr) << BATT_INSPECT(expected_fpr);

    LLFS_LOG_INFO() << BATT_INSPECT(actual_fpr / expected_fpr) << BATT_INSPECT(word_count)
                    << BATT_INSPECT(hash_count) << BATT_INSPECT(actual_fpr)
                    << BATT_INSPECT(expected_fpr);
  }

  EXPECT_LT(false_positive_rate_total / false_positive_rate_count, 1.01);

  LLFS_LOG_INFO() << BATT_INSPECT(false_positive_rate_total / false_positive_rate_count);

  LLFS_LOG_INFO() << "build latency (per key) == " << build_latency
                  << " build rate (keys/sec) == " << build_latency.rate_per_second();

  LLFS_LOG_INFO() << "normalized query latency (per key*bit) == " << query_latency
                  << " query rate (key*bits/sec) == " << query_latency.rate_per_second();
}

class MockPageView : public llfs::PageView
{
 public:
  MockPageView(std::shared_ptr<const llfs::PageBuffer>&& buffer) : llfs::PageView{std::move(buffer)}
  {
    this->create_keys_for_page(4096);
  }

  llfs::PageLayoutId get_page_layout_id() const override
  {
    return MockPageView::page_layout_id();
  }

  batt::BoxedSeq<llfs::PageId> trace_refs() const override
  {
    return batt::seq::Empty<llfs::PageId>{} | batt::seq::boxed();
  }

  batt::Optional<llfs::KeyView> min_key() const override
  {
    return this->keys_in_this_page_.front();
  }

  batt::Optional<llfs::KeyView> max_key() const override
  {
    return this->keys_in_this_page_.back();
  }

  batt::StatusOr<usize> get_keys(llfs::ItemOffset lower_bound,
                                 const batt::Slice<llfs::KeyView>& key_buffer_out,
                                 [[maybe_unused]] llfs::StableStringStore& storage) const override
  {
    if (lower_bound >= this->keys_in_this_page_.size()) {
      return 0;
    }

    usize end_index =
        std::min(lower_bound + key_buffer_out.size(), this->keys_in_this_page_.size());
    BATT_CHECK_LE(lower_bound, end_index);
    auto key_buffer_iterator = key_buffer_out.begin();
    for (usize i = lower_bound; i < end_index; ++i) {
      *key_buffer_iterator = this->keys_in_this_page_[i];
      ++key_buffer_iterator;
    }
    return std::distance(key_buffer_out.begin(), key_buffer_iterator);
  }

  std::shared_ptr<llfs::PageFilter> build_filter() const override
  {
    return std::make_shared<llfs::NullPageFilter>(this->page_id());
  }

  void dump_to_ostream(std::ostream& out) const override
  {
    out << "(?)";
  }

  static const llfs::PageLayoutId& page_layout_id() noexcept
  {
    const static llfs::PageLayoutId id_ = [] {
      llfs::PageLayoutId id;

      const char tag[sizeof(id.value) + 1] = "(mock)";

      std::memcpy(&id.value, tag, sizeof(id.value));

      return id;
    }();

    return id_;
  }

  static llfs::PageReader page_reader() noexcept
  {
    return [](std::shared_ptr<const llfs::PageBuffer> page_buffer)
               -> batt::StatusOr<std::shared_ptr<const llfs::PageView>> {
      return {std::make_shared<MockPageView>(std::move(page_buffer))};
    };
  }

  void create_keys_for_page(usize num_keys)
  {
    for (usize i = 0; i < num_keys; ++i) {
      std::default_random_engine rng{i};
      this->keys_in_this_page_.emplace_back(make_random_word(rng));
    }
    std::sort(this->keys_in_this_page_.begin(), this->keys_in_this_page_.end());
  }

  std::vector<std::string>& keys_in_this_page()
  {
    return this->keys_in_this_page_;
  }

 private:
  std::vector<std::string> keys_in_this_page_;
};

class BuildBloomFilterTest : public ::testing::Test
{
 public:
  void SetUp() override
  {
    this->reset_cache();
  }

  void TearDown() override
  {
  }

  void reset_cache()
  {
    batt::StatusOr<batt::SharedPtr<llfs::PageCache>> page_cache_created =
        llfs::make_memory_page_cache(
            batt::Runtime::instance().default_scheduler(),
            /*arena_sizes=*/
            {
                {llfs::PageCount{this->page_device_capacity}, llfs::PageSize{this->page_size}},
            },
            llfs::MaxRefsPerPage{0});

    ASSERT_TRUE(page_cache_created.ok());

    this->page_cache = std::move(*page_cache_created);

    batt::Status register_reader_status = this->page_cache->register_page_reader(
        MockPageView::page_layout_id(), __FILE__, __LINE__, MockPageView::page_reader());
    ASSERT_TRUE(register_reader_status.ok());
  }

  batt::StatusOr<llfs::PinnedPage> make_mock_page(llfs::PageCacheJob& job)
  {
    llfs::StatusOr<std::shared_ptr<llfs::PageBuffer>> page_allocated = job.new_page(
        llfs::PageSize{this->page_size}, batt::WaitForResource::kFalse,
        MockPageView::page_layout_id(), llfs::Caller::Unknown, /*cancel_token=*/llfs::None);
    BATT_REQUIRE_OK(page_allocated);

    std::shared_ptr<MockPageView> mock_page_view =
        std::make_shared<MockPageView>(std::move(*page_allocated));

    this->page_to_keys[mock_page_view->page_id()] = mock_page_view->keys_in_this_page();

    return job.pin_new(std::move(mock_page_view), llfs::Caller::Unknown);
  }

  batt::Status create_mock_pages_and_commit(u64 num_pages, llfs::PageCacheJob& job)
  {
    for (u64 i = 0; i < num_pages; ++i) {
      batt::StatusOr<llfs::PinnedPage> pinned_page = this->make_mock_page(job);
      BATT_REQUIRE_OK(pinned_page);
      this->page_cache->push_to_page_filter_queue(pinned_page->page_id());
    }

    return batt::OkStatus();
  }

  const u32 page_size{2 * kMiB};

  const u64 page_device_capacity{16};

  std::unordered_map<llfs::PageId, std::vector<std::string>, llfs::PageId::Hash> page_to_keys;

  batt::SharedPtr<llfs::PageCache> page_cache;
};

TEST_F(BuildBloomFilterTest, BuildFilters)
{
  std::unique_ptr<llfs::PageCacheJob> job = this->page_cache->new_job();
  batt::Status create_pages_status =
      this->create_mock_pages_and_commit(this->page_device_capacity, *job);
  ASSERT_TRUE(create_pages_status.ok());

  for (auto& [page_id, key_set] : this->page_to_keys) {
    for (auto& key : key_set) {
      EXPECT_TRUE(this->page_cache->page_might_contain_key(page_id, key));
    }
  }
}

}  // namespace
