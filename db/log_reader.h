
#pragma once

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"


namespace my_leveldb {

class SequentialFile;

namespace log {

class Reader {
 public:
  // Interface for reporting errors.
  class Reporter {
   public:
    virtual ~Reporter();
    
    // Some corruption was detected. "bytes" is the approximate number
    // of bytes dropped due to the corruption.
    virtual void Corruption(size_t bytes, const Status &status) = 0;
  };

  Reader(SequentialFile *file, Reporter *reporter, bool checksum,
                uint64_t initial_offset);

  REMOVE_COPY_CONSTRUCTOR(Reader);

  ~Reader();

  // Read the next record into *record. Return true if read
  // successfully, false if we hit end of the input. May use
  // "*scratch" as temporary storage. The contents filled in *record
  // will noly be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  auto ReadRecord(Slice *record, std::string *scratch) -> bool;

  // Returns the physical offset of the last record returned by ReadRecord.
  //
  // Undefined before the first call to ReadRecord.
  auto LastRecordOffset() -> uint64_t;
 private:
  // Extend record types with the following special values  
  enum {

    kEof = kMaxReCordType + 1,
    // Returned whenever we find an invalid physical record.
    // Currently there are three situation in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported) 
    // * The record is below constructor's initial_offset (No drop is reported)
    kBadRecord = kMaxReCordType + 2
  };

  // Skip all blocks that are completely before "initial_offset_".
  //
  // Returns true on success. Handles reporting.
  auto SkipToInitialBlock() -> bool;

  // Return type, or one of the preceing special values
  auto ReadPhysicalRecord(Slice *result) -> unsigned int;

  // Reports dropped bytes to the reporter.
  // buffer_ must be updated to remove the dropped bytes prior to invocation.
  void ReportCorruption(uint64_t bytes, const char *reason);
  void ReportDrop(uint64_t bytes, const Status &reason);

  SequentialFile *const file_;
  Reporter *const reporter_;
  bool const checksum_;
  char * const backing_store_;
  Slice buffer_;
  bool eof_;  // Last Read() indicated EOF by returning < kBlockSize

  // Offset of the last record returned by ReadRecord.
  uint64_t last_record_offset_;

  // Offset of the first location past the end of buffer_.
  uint64_t end_of_buffer_offset_;

  // Offst at which to start looking for the first record to return.
  uint64_t const initial_offset_;

  // True if we resynchronizing after a seek (initial_offset_ > 0). In
  // parrticular, a run of kMiddleType and kLastType records can be silently
  // skipped in this mode
  bool resyncing_;

};
} // namespace log
} // namespace leveldb