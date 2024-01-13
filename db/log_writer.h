
#pragma once

#include <cstdint>
#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"


namespace my_leveldb {

class WritableFile;

namespace log {

class Writer {
 public:

  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(WritableFile *dest);

  // Create a writer that will append data to "*dest".
  // "*dest" must have initial length "dest_length".
  // "*dest" must remain live while this Writer is in use.
  Writer(WritableFile *dest, uint64_t dest_length);

  REMOVE_COPY_CONSTRUCTOR(Writer);

  ~Writer();

  auto AddRecord(const Slice &slice) -> Status;



 private:
  auto EmitPhysicalRecord(RecordType type, const char *ptr, size_t length) -> Status;

  WritableFile *dest_;
  int block_offset_;  // Current offset in block

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the 
  // record type stored in the header.
  uint32_t type_crc_[kMaxReCordType + 1];

};
} // namespace log
} // namespace my_leveldb