
/**
 * Multiple threads can invode const method on a Slice without
 * external synchronization, buf if any of the threds may call a 
 * non-const method, all threads accessing the same Slice must use
 * external synchronization
*/

#ifndef STORAGE_MY_LEVELDB_INCLUDE_SLICE_H_
#define STORAGE_MY_LEVELDB_INCLUDE_SLICE_H_

#include <cassert>
#include <cstddef>
#include <cstring>
#include <string>

#include "leveldb/export.h"

namespace my_leveldb {

class Slice {
 public:
  Slice() : data_(""), size_(0) {}

  // Create a slice that refers to d[0, n-1].
  Slice(const char *d, size_t n) : data_(d), size_(n) {}

  // Create a slice that refers to s[0, strlen(s)-1]
  Slice(const char *s) : data_(s), size_(strlen(s)) {}

  Slice(const std::string &s) : data_(s.data()), size_(s.size()) {}

  // Intentionally copyable.
  Slice(const Slice&) = default;
  Slice& operator=(const Slice&) = default;

  // Return a pointer to the beginning of the referenced data
  auto data() const -> const char*{ return data_; }

  auto size() const -> size_t { return size_; }

  auto empty() const -> bool { return size_ == 0; }

  auto operator[](size_t n) const -> char {
    assert(n < size());
    return data_[n];
  }

  void clear() {
    data_ = "";
    size_ = 0;
  }

  void remove_prefix(size_t n) {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  auto ToString() const -> std::string { return std::string(data_, size_); }

  // Three-way comparison. Returns value:
  // < 0 iff "*this" < "b",
  // == 0 iff "*this" == "b",
  // > 0 iff "*this" > "b".

  auto compare(const Slice &b)const -> int;

  // Return true iff "x" is a prefix of "*this"

  auto starts_with(const Slice &x) const -> bool {
    return ((size_ >= x.size_) && memcmp(data_, x.data_, x.size_) == 0);
  }

 private:
  const char *data_;
  size_t size_;
};

inline auto operator==(const Slice &x, const Slice &y) -> bool {
  return ((x.size() == y.size()) && 
      (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline auto operator!=(const Slice &x, const Slice &y) -> bool {
  return !(x == y);
}

inline auto Slice::compare(const Slice &b) const -> int  {
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = memcmp(data_, b.data_, min_len);
  if (r == 0) {
    if (size_ < b.size_) {
      r = -1;
    } else if (size_ > b.size_) {
      r = 1;
    }
  }

  return r;
}
}
#endif