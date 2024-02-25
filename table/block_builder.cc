


/**
 * 
 * BlockBuilder generate blocks where keys are prefix-compressed:
 * 
 * When we store a key, we drop the prefix shared with the previous
 * string.  This helps reduce the space requirement significantly.
 * Furthermore, once every K keys, we do not apply the prefix
 * compression and store the entrie key. We call this a "restart point".
 * The tail end of the block stores the offsets of all of the restart points, 
 * Values are stored as-is  (without compression)
 * immediately folloing the corresponding key.
 * 
 * An entry for a particular key-value pair has the form:
 *      shared_bytes:         varint32
 *      unshared_bytes:       varint32
 *      value_length:         varint32
 *      key_delta:            char[unshared_bytes]
 *      value:                char[value_length];
 * shared_bytes == 0 for restart points.
 * 
 * The trailer of the block has the form:
 *      restarts:             uint32[num_restarts]
 *      num_restarts:         uint32
 * restarts[i] contains the offset within the block of the ith restart point.
 * 
*/

#include <algorithm>
#include <cassert>

#include "table/block_builder.h"
#include "leveldb/options.h"
#include "leveldb/comparator.h"
#include "util/coding.h"

namespace my_leveldb {

BlockBuilder::BlockBuilder(const Options *options)
    : options_(options), restarts_ {}, counter_{0}, finished_{0} {
  restarts_.push_back(0);   // First restat point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

auto BlockBuilder::CurrentSizeEstimate() const -> size_t {
  return (buffer_.size() +                       // Row data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

auto BlockBuilder::Finish() -> Slice {
  // Append resart array
  for (size_t i = 0; i < restarts_.size(); ++i) {
    PutFixed32(&buffer_, restarts_[i]);
  }

  // Put "restarts.size()" to buffer_
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}


void BlockBuilder::Add(const Slice &key, const Slice &value) {
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty()    // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);

  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) {
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }

  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // Update state
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(last_key_ == key);
  counter_++;
}
}