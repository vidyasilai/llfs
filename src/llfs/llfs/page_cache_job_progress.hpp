#pragma once
#ifndef LLFS_PAGE_CACHE_JOB_PROGRESS_HPP
#define LLFS_PAGE_CACHE_JOB_PROGRESS_HPP

#include <batteries/assert.hpp>

#include <ostream>

namespace llfs {

enum struct PageCacheJobProgress {
  kPending,
  kDurable,
  kAborted,
};

inline bool is_terminal_state(PageCacheJobProgress t)
{
  switch (t) {
    case PageCacheJobProgress::kPending:
      return false;
    case PageCacheJobProgress::kDurable:
      return true;
    case PageCacheJobProgress::kAborted:
      return true;
  }
  BATT_PANIC() << "bad value for PageCacheJobProgress: " << (int)t;
  BATT_UNREACHABLE();
}

inline std::ostream& operator<<(std::ostream& out, PageCacheJobProgress t)
{
  switch (t) {
    case PageCacheJobProgress::kPending:
      return out << "Pending";
    case PageCacheJobProgress::kDurable:
      return out << "Durable";
    case PageCacheJobProgress::kAborted:
      return out << "Aborted";
  }
  return out << "Unknown";
}

}  // namespace llfs

#endif  // LLFS_PAGE_CACHE_JOB_PROGRESS_HPP
