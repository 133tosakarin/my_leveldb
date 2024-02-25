#ifndef STORAGE_MY_LEVELDB_UTIL_HASH_H_
#define STORAGE_MY_LEVELDB_UTIL_HASH_H_

#include <cstddef>
#include <cstdint>

namespace my_leveldb {
// simple hash function used for internal data structures.
uint32_t Hash(const char *data, size_t n, uint32_t seed);
}
#endif