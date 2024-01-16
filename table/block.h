
#pragma once

#include <cstddef>
#include <cstdint>

#include "leveldb/iterator.h"
#include "leveldb/slice.h"

namespace my_leveldb {

class Comparator;
class BlockContents;

class Block {
 public:

  explicit Block(const BlockContents &contents);

  REMOVE_COPY_CONSTRUCTOR(Block);

  ~Block();

  size_t size() const { return size_; }
  Iterator *NewIterator(const Comparator *comparator);
  
 private:
  class Iter;

  auto NumRestarts() const -> uint32_t;
  
  const char *data_;  // data_ is point to the first restart_point offset
  size_t size_;       // the final must be restart_num * sizeof(uint32_t) + sizeof(restars_num)
  uint32_t restart_offset_; // Offset int data_ of restart array
  bool onwed_; // Block owns data_[]
};
}