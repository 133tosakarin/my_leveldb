#ifndef STORAGE_MY_LEVELDB_UTIL_ARENA_H_
#define STORAGE_MY_LEVELDB_UTIL_ARENA_H_

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <cstdint>
#include <vector>

namespace my_leveldb {

class Arena {
 public:
  Arena();

  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  ~Arena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  auto Allocate(size_t bytes) -> char*;

  // Allocate memory with the normal alignment guarantees provided by malloc.

  auto AllocateAligned(size_t bytes) -> char*;

  // Returns an esimate of the total memory usage of data allocated
  // by the arena.

  auto MemoryUsage() const -> size_t {
    return memory_usage_.load(std::memory_order_relaxed);
  }

  private:
    auto AllocateFallback(size_t bytes) -> char*;
    auto AllocateNewBlock(size_t block_types) -> char*;

    // Allocation state

    char *alloc_ptr_;
    size_t alloc_bytes_remaining_;

    // Array of new[] allocated memory blocks
    std::vector<char *> blocks_;

    // Total memory usage of the arena.
    std::atomic<size_t> memory_usage_;

};

inline auto Arena::Allocate(size_t bytes) -> char* {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocation, so we disallow them here (we don't need
  // them for out internal use)

  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    char *result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}
} // namespace my_leveldb
#endif