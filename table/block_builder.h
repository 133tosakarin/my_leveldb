
#pragma once

#include <cstdint>
#include <vector>

#include "leveldb/status.h"
#include "leveldb/slice.h"


namespace my_leveldb {

struct Options;

class BlockBuilder {
 public:

  explicit BlockBuilder(const Options *options);

  REMOVE_COPY_CONSTRUCTOR(BlockBuilder);

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRESE: Finish() has not been called since the last call to Reset().
  // REQUIRESE: key is larger than any previously added key
  void Add(const Slice &key, const Slice &value);

  // Returns the all restart point and its length
  // Finish building the block and return a slice that refers to the
  // block contents. The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  auto Finish() -> Slice;

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  auto empty() const -> bool { return buffer_.empty(); }


 private:

  const Options *options_;
  std::string buffer_;              // Destination buffer
  std::vector<uint32_t> restarts_;  // Restart points 
  int counter_;                     // Number of entries emitted since restart
  bool finished_;                   // Has Finish() been called?
  std::string last_key_;
};
} // namespace my_leveldb