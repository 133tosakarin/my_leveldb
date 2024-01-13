
#pragma once

namespace my_leveldb {
namespace log {

enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,

  // kFullType will be set if log can be stored in block completely 
  kFullType = 1,

  // For fragments
  // kFirstType will be set if the content is the first frament 
  kFirstType = 2,
  // kMiddleType will be set if the content is the middle frament 
  kMiddleType = 3,
  // kLastType will be set if the content is the last frament 
  kLastType = 4
};

static constexpr int kMaxReCordType = kLastType;

static constexpr int kBlockSize = 32 * 1024;

// Header is checksum (4 bytes), length (2 bytes), type (1 bytes).
static constexpr int kHeaderSize = 4 + 2 + 1;
} // namespace log
} // namespace leveldb