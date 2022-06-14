#pragma once
#ifndef LLFS_PACKED_INTERVAL_HPP
#define LLFS_PACKED_INTERVAL_HPP

#include <ostream>

namespace llfs {

template <typename T>
struct PackedInterval {
  T lower_bound;
  T upper_bound;
};

inline std::ostream& operator<<(std::ostream& out, const PackedInterval<T>& t)
{
  return out << "[" << t.lower_bound << ", " << t.upper_bound << ")";
}

template <typename T>
struct PackedCInterval {
  T lower_bound;
  T upper_bound;
};

inline std::ostream& operator<<(std::ostream& out, const PackedCInterval<T>& t)
{
  return out << "[" << t.lower_bound << ", " << t.upper_bound << "]";
}

}  // namespace llfs

#endif  // LLFS_PACKED_INTERVAL_HPP
