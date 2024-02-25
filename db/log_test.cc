

#include "gtest/gtest.h"
#include "util/random.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "leveldb/env.h"
#include "leveldb/slice.h"

namespace my_leveldb {

namespace log {

// Construct a string of the specified length make out of the suppplied
// partial string.
static std::string BigString(const std::string &partial_string, size_t n) {
  std::string result;
  while (result.size() < n) {
    result.append(partial_string);
  }
  result.resize(n);
  return result;
}

// Construct a string from a number
static std::string NumberString(int n) {
  char buf[50];
  std::snprintf(buf, sizeof(buf), "%d.", n);
  return std::string(buf);
}

// Return a skewed potential long string
static std::string RandomSkewedString(int i, Random *rnd) {
  return BigString(NumberString(i), rnd->Skewd(17));
}

class LogTest : public testing::Test {
 public:
  LogTest()
      : reading_(false),
        writer_{new Writer(&dest_)},
        reader_(new Reader(&source_, &reporter_, true, 0)) {}
  
  ~LogTest() {
    delete writer_;
    delete reader_;
  }

  void ReopenForAppend() {
    delete writer_;
    writer_ = new Writer(&dest_, dest_.contents_.size());
  }

  void Write(const std::string &msg) {
    ASSERT_TRUE(!reading_) << "Write() after starting to read";
    writer_->AddRecord(Slice(msg));
  }

  size_t WriteBytes() const { return dest_.contents_.size(); }

  std::string Read() {
    if (!reading_) {
      reading_ = true;
      source_.contents_ = Slice(dest_.contents_);
    }
    std::string scratch;
    Slice record;
    if (reader_->ReadRecord(&record, &scratch)) {
      return record.ToString();
    } else {
      return "EOF";
    }
  }

  void IncrementByte(int offset, int delta) {
    dest_.contents_[offset] += delta;
  }

  void SetByte(int offset, char new_byte) {
    dest_.contents_[offset] = new_byte;
  }

  void ShrinkSize(int bytes) {
    dest_.contents_.resize(dest_.contents_.size() - bytes);
  }

  void FixChecksum(int header_offset, int len) {
    // Compute crc of type/len/data
    uint32_t crc = crc32c::Value(&dest_.contents_[header_offset + 6], 1 + len);
    crc = crc32c::Mask(crc);
    EncodeFixed32(&dest_.contents_[header_offset], crc);
  }

  void ForceError() { source_.force_error_ = true; }

  size_t DroppedBytes() const { return reporter_.dropped_bytes_; }

  std::string ReportMessage() const { return reporter_.message_; }

  // Returns OK iff record error message contains "msg"
  std::string MatchError(const std::string &msg) const {
    if (reporter_.message_.find(msg) == std::string::npos) {
      return reporter_.message_;
    } else {
      return "OK";
    }
  }

  void WriteInitialOffsetLog() {
    for (int i = 0; i < num_initial_offset_records_; i++) {
      std::string record(initial_offset_record_size_[i], static_cast<char>('a' + i));
      Write(record);
    }
  }

  void StartReadingAt(uint64_t initial_offset) {
    delete reader_;
    reader_ = new Reader(&source_, &reporter_, true, initial_offset);
  }

  void CheckOffsetPastEndReturnsNoRecords(uint64_t offset_past_end) {
    WriteInitialOffsetLog();
    reading_ = true;
    source_.contents_ = Slice(dest_.contents_);
    Reader *offset_reader = new Reader(&source_, &reporter_, true,
                                WriteBytes() + offset_past_end);
    Slice record;
    std::string scratch;
    ASSERT_TRUE(!offset_reader->ReadRecord(&record, &scratch));
    delete offset_reader;
  }

  void CheckInitialOffsetRecord(uint64_t initial_offset,
                                int expected_record_offset) {
    WriteInitialOffsetLog();
    reading_ = true;
    source_.contents_ = Slice(dest_.contents_);
    Reader *offset_reader = 
      new Reader(&source_, &reporter_, true, initial_offset);
    
    // Read all records from expected_record_offset through the last one.
    ASSERT_LT(expected_record_offset, num_initial_offset_records_);
    for (; expected_record_offset < num_initial_offset_records_; ++expected_record_offset) {
      Slice record;
      std::string scratch;
      ASSERT_TRUE(offset_reader->ReadRecord(&record, &scratch));
      ASSERT_EQ(initial_offset_last_record_offsets_[expected_record_offset],
                offset_reader->LastRecordOffset());
      ASSERT_EQ((char)('a' + expected_record_offset), record.data()[0]);
    }
    delete offset_reader;
  }
  
 private: 
  class StringDest : public WritableFile {
   public:
    Status Close() override { return Status::OK(); }
    Status Flush() override { return Status::OK(); }
    Status Sync() override { return Status::OK(); }
    Status Append(const Slice &slice) override {
      contents_.append(slice.data(), slice.size());
      // std::fprintf(stdout, "size: %llu\n", contents_.size());
      return Status::OK(); 
    }
    std::string contents_;
  };

  class StringSource : public SequentialFile {
   public:
    StringSource() : force_error_(false), returned_partial_(false) {}

    Status Read(size_t n, Slice *result, char *scratch) override {
      EXPECT_TRUE(!returned_partial_) << "mst not Read() after eof/error";

      if (force_error_) {
        force_error_ = false;
        returned_partial_ = true;
        return Status::Corruption("read error");
      }
      
      if (contents_.size() < n) {
        n = contents_.size();
        returned_partial_ = true;
      }
      *result = Slice(contents_.data(), n);
      contents_.remove_prefix(n);
      return Status::OK();
    }

    Status Skip(uint64_t n) override {
      if (n > contents_.size()) {
        contents_.clear();
        return Status::NotFound("in-memory file skipped past end.");
      }

      contents_.remove_prefix(n);
      return Status::OK();
    }

    

    Slice contents_;
    bool force_error_;
    bool returned_partial_;
  };
  class ReporterCollector : public Reader::Reporter {
   public:
    ReporterCollector() : dropped_bytes_(0) {}
    void Corruption(size_t bytes, const Status &status) override {
      dropped_bytes_ += bytes;
      message_.append(status.ToString());
    }
    size_t dropped_bytes_;
    std::string message_;
  };

  // Record metadata for testing initial offset functionality
  static size_t initial_offset_record_size_[];
  static uint64_t initial_offset_last_record_offsets_[];
  static int num_initial_offset_records_;

  StringDest dest_;
  StringSource source_;
  ReporterCollector reporter_;
  bool reading_;
  Writer *writer_;
  Reader *reader_;

};
size_t LogTest::initial_offset_record_size_[] = {
  10000,  // Two sizable records in first block
  10000,  
  2 * log::kBlockSize - 1000,  // Span three blocks
  1,  
  13716,                      // Consume all but two bytes of block 3
  log::kBlockSize - kHeaderSize,  // Consume the entirety of block 4
};
uint64_t LogTest::initial_offset_last_record_offsets_[] = {
  0,
  kHeaderSize + 10000,
  2 * (kHeaderSize + 10000),
  2 * (kHeaderSize + 10000) + (2 * log::kBlockSize - 1000) + 3 * kHeaderSize,
  2 * (kHeaderSize + 10000) + (2 * log::kBlockSize - 1000) + 3 * kHeaderSize + 
      kHeaderSize + 1,
  3 * log::kBlockSize,

};
int LogTest::num_initial_offset_records_ = {
 sizeof(LogTest::initial_offset_last_record_offsets_) / sizeof(uint64_t)
};

TEST_F(LogTest, Empty) { ASSERT_EQ("EOF", Read()); }
 
TEST_F(LogTest, ReadWrite) {
  Write("foo");
  Write("bar");
  Write("");
  Write("xxxx");
  ASSERT_EQ("foo", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("xxxx", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("EOF", Read()); // Make sure reads at eof work
}

TEST_F(LogTest, ManyBlocks) {
  for (int i = 0; i < 100000; ++i) {
    Write(NumberString(i));
  }
  for (int i = 0; i < 100000; ++i) {
    ASSERT_EQ(NumberString(i), Read());
  }
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, Fragmentation) {
  Write("small");
  Write(BigString("medium", 50000));
  Write(BigString("large", 100000));
  ASSERT_EQ("small", Read());
  ASSERT_EQ(BigString("medium", 50000), Read());
  ASSERT_EQ(BigString("large", 100000), Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, MarginalTrailer2) {
  // Make a trailer that is exactly the same length as an empty record
  const int n = kBlockSize - 2 * kHeaderSize;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize, WriteBytes());
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_F(LogTest, ShortTrailer) {
  const int n = kBlockSize - 2 * kHeaderSize + 4;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize + 4, WriteBytes());
  Write("");
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("",  Read());
  ASSERT_EQ("bar",  Read());
  ASSERT_EQ("EOF",  Read());
}

TEST_F(LogTest, AlignedEof) {
  const int n = kBlockSize - 2 * kHeaderSize + 4;
  Write(BigString("foo", n));
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, OpenForAppend) {
  // each write operation will be flushed to disk
  Write("hello");
  ReopenForAppend();
  Write("world");
  ASSERT_EQ("hello", Read());
  ASSERT_EQ("world", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, RandomRead) {
  const int N = 500;
  Random write_rnd(301);

  for (int i = 0; i < N; ++i) {
    Write(RandomSkewedString(i, &write_rnd));
  }
  Random read_rnd(301);
  for (int i = 0; i < N; ++i) {
    ASSERT_EQ(RandomSkewedString(i, &read_rnd), Read());
  }
  ASSERT_EQ("EOF", Read());
}

// Tests of all the error paths in log_reader.cc follow:

TEST_F(LogTest, ReadError) {
  Write("foo");
  ForceError();
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(kBlockSize, DroppedBytes());
  ASSERT_EQ("OK", MatchError("read error"));
}

TEST_F(LogTest, BadRecordType) {
  Write("foo");
  // type is stored in header[6]
  IncrementByte(6, 100);
  FixChecksum(0, 3);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("unknown record type"));
}

TEST_F(LogTest, TruncatedTrailingRecordIsIgnored) {
  Write("foo");
  ShrinkSize(4);  // Drop all payload as well as a header byte
  ASSERT_EQ("EOF", Read());
  // Truncated last recrod is ignored, not treated as an error
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_F(LogTest, BadLength) {
  const int kPayloadSize = kBlockSize - kHeaderSize;
  Write(BigString("bar", kPayloadSize));
  Write("foo");
  // Least significant size byte is stored in header[4]
  IncrementByte(4, 1);
  ASSERT_EQ("foo", Read());
  ASSERT_EQ(kBlockSize, DroppedBytes());
  ASSERT_EQ("OK", MatchError("bad record length"));
}

TEST_F(LogTest, BadLengthAtEndIsIgnored) {
  Write("foo");
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_F(LogTest, ChecksumMismatch) {
  Write("foo");
  IncrementByte(0, 10);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(10, DroppedBytes());
  ASSERT_EQ("OK", MatchError("checksum mismatch"));
}

TEST_F(LogTest, UnexpectedMiddleType) {
  Write("foo");
  SetByte(6, kMiddleType);
  FixChecksum(0, 3);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("missing start"));
}

TEST_F(LogTest, UnexpectedLastType) {
  Write("foo");
  // Write("bar");
  SetByte(6, kLastType);
  FixChecksum(0, 3);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("missing start"));
}

TEST_F(LogTest, UnexpectedFunnType) {
  Write("foo");
  Write("bar");
  SetByte(6, kFirstType);
  FixChecksum(0, 3);
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("partial record without end"));
}

TEST_F(LogTest, UnexpectedFirstType) {
  Write("foo");
  Write(BigString("bar", 100000));
  SetByte(6, kFirstType);
  FixChecksum(0, 3);
  ASSERT_EQ(BigString("bar", 100000), Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("partial record without end"));
}

TEST_F(LogTest, MissingLastIsIgnored) {
  Write(BigString("bar", kBlockSize));
  // Remove the LAST block, including header.
  ShrinkSize(14);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0, DroppedBytes());
}

TEST_F(LogTest, PartialLastIsIgnored) {
  Write(BigString("bar", kBlockSize));
  // Cause a bad record length in the LAST block
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0, DroppedBytes());
}

TEST_F(LogTest, SkipIntoMultiRecord) {
  // Consider a fragmented record:
  //    first(R1), middle(R1), last(R1), first(R2)
  // If initial_offset points to a record after first(R1) but before first(R2)
  // incompelte fragment errors are not actual errors, and must be suppressed
  // untial a new first or full record is encountered.
  Write(BigString("foo", 3 * kBlockSize));
  Write("correct");
  StartReadingAt(kBlockSize);

  ASSERT_EQ("correct", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("EOF", Read()); 
}

TEST_F(LogTest, ErrorJoinsRecord) {
  // Consider two fragment records:
  //    first(R1) last(R1) First(R2) Last(R2)
  // where the middle two frgaments disappear.  We do not want
  // first(R1), last(R2) to get joinedd and returned as valid record.
  
  // Write records that span two blocks
  Write(BigString("foo", kBlockSize));
  Write(BigString("bar", kBlockSize));
  Write("correct");

  // Wipe the middle block
  for (int offset = kBlockSize; offset < 2 * kBlockSize; offset++) {
    SetByte(offset, 'x');
  }
  ASSERT_EQ("correct", Read());
  ASSERT_EQ("EOF", Read());
  const size_t dropped = DroppedBytes();
  ASSERT_LE(dropped, 2 * kBlockSize + 100);
  ASSERT_GE(dropped, 2 * kBlockSize);
}

TEST_F(LogTest, ReadStart) {
  CheckInitialOffsetRecord(0, 0);
}

TEST_F(LogTest, ReadSsecondOneOff) {
  CheckInitialOffsetRecord(1, 1);
}

TEST_F(LogTest, ReadSecondTenThousandOff) {
  CheckInitialOffsetRecord(10000, 1);
}

TEST_F(LogTest, ReadSecondStart) {
  CheckInitialOffsetRecord(10007, 1);
}

TEST_F(LogTest, ReadThirdOneOff) {
  CheckInitialOffsetRecord(10008, 2);
}

TEST_F(LogTest, ReadThirdOneStart) {
  CheckInitialOffsetRecord(20014, 2);
}

TEST_F(LogTest, ReadFourthOneOff) {
  CheckInitialOffsetRecord(20015, 3);
}

TEST_F(LogTest, ReadFourThFirstBlockTrailer) {
  CheckInitialOffsetRecord(log::kBlockSize - 4, 3);
}

TEST_F(LogTest, ReadFourThMiddleBlock) {
  CheckInitialOffsetRecord(log::kBlockSize + 1, 3);
}

TEST_F(LogTest, ReadFourthLastBlock) {
  CheckInitialOffsetRecord(2 * log::kBlockSize + 1, 3);
}

TEST_F(LogTest, ReadFourthStart) {
  CheckInitialOffsetRecord(
    2 * (kHeaderSize + 1000) + (2 * log::kBlockSize - 1000) + 3 * 
    kHeaderSize, 3); 
}

TEST_F(LogTest, ReadInitialOffsetIntoBlockPadding) {
  CheckInitialOffsetRecord(3 * log::kBlockSize - 3, 5);
}

TEST_F(LogTest, ReadEnd) {
  CheckOffsetPastEndReturnsNoRecords(0);
}

TEST_F(LogTest, ReadPastEnd) {
  CheckOffsetPastEndReturnsNoRecords(5);
}

} // namespace 

} // namespace my_leveldb