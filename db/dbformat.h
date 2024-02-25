
#pragma once

#include <cstdint>
#include <cstddef>
#include <string>

#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "leveldb/table_builder.h"
#include  "util/logging.h"
#include "util/coding.h"
namespace my_leveldb {

// Grouping of constants. We may want to make some of these
// parameters set via options.
namespace config {

static constexpr int kNumLevels = 7;

// Level-0 compaction is started when we hit this many files.
static constexpr int kL0_CompactionTrigger = 4;

// Soft limit on number of level-0 files. We slow down writes at this point.
static constexpr int kL0_SlowdownWritesTrigger = 8;

// Maximun number of level-0 files. We stop writes at this point.
static constexpr int kL0_StopWritesTrigger = 12;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap. We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations. We do not push all the way to
// the largest level since that can gernae a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
static constexpr int kMaxMemCompactionLevel = 2;

// Approximate gap in bytes between samples of data read during iteration.
static constexpr int kReadBytesPeriod = 1048576;

} // namespace config

class InternalKey;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk 
// data structures.
enum ValueType { kTypeDeletion = 0x0, kTypeValue = 0x1 };

// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence number in decreasing order 
// and the value type is embedded as the low 8 bits in the sequence 
// number in internal keys, we need to use the highest-numbered ValueType, not the lowest)
static constexpr ValueType kValueTypeForSeek = kTypeValue;

using SequenceNumber = uint64_t;

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
static constexpr uint64_t kMaxSequenceNumber = ((0x1ull << 56) - 1);

struct ParsedInternalKey {
  Slice user_key;
  SequenceNumber sequence;
  ValueType type;

  ParsedInternalKey() {} // Intentionally left uninitiallized (for speed)
  ParsedInternalKey(const Slice &u, const SequenceNumber &seq, ValueType t)
      : user_key(u), sequence(seq), type(t) {}
  
  auto DebugString() const -> std::string;
};


// Return the length of the encoding of "key".
inline auto InternalKeyEncodingLength(const ParsedInternalKey &key) -> size_t{
  return key.user_key.size() + 8;
}

// Append the serialization of "key" to *result.
void AppendInteralKey(std::string *result, const ParsedInternalKey &key);

// 
inline auto ParseInternalKey(const Slice &internal_key, ParsedInternalKey *result) -> bool;

inline auto ExtractUserKey(const Slice &internal_key) -> Slice {
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() -8);
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number
class InternalKeyComparator : public Comparator {
 private:
  const Comparator *user_comparator_;
 public:
  explicit InternalKeyComparator(const Comparator *c) : user_comparator_(c) {
    // std::fprintf(stderr, "InternalKeyComparator name: %s\n",user_comparator_->Name());
    // std::fprintf(stderr, "Comparator name: %s\n", c->Name());
  }

  const char *Name() const override;
  auto Compare(const Slice &a, const Slice &b) const -> int override;
  void FindShortestSeparator(std::string *start, const Slice &limit) const override;
  void FindShortSuccessor(std::string *key) const override;
  auto user_comparator() const -> const Comparator * { return user_comparator_; }
  int Compare(const InternalKey &a, const InternalKey &b) const;

};

// Filter policy wrapper that converts from internal keys to user keys
class InternalFilterPolicy : public FilterPolicy {
 private:
  const FilterPolicy *const user_policy_;

 public:
  explicit InternalFilterPolicy(const FilterPolicy *p) : user_policy_(p) {}
  auto Name() const -> const char * override;
  void CreateFilter(const Slice *keys, int n, std::string *dst) const override;
  auto KeyMayMatch(const Slice &key, const Slice &filter) const -> bool override;
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparsions instead of an InternalKeyComparator.
class InternalKey {
 private:
  std::string rep_;
 public:
  InternalKey() {} // Leave rep_ as empty to indicate it is invalid
  InternalKey(const Slice &user_key, SequenceNumber s, ValueType t) {
    AppendInteralKey(&rep_, ParsedInternalKey(user_key, s, t));
  }

  auto DecodeFrom(const Slice &s) -> bool {
    rep_.assign(s.data(), s.size());
    return !rep_.empty();
  }

  auto Encode() const -> Slice {
    assert(!rep_.empty());
    return rep_;
  }

  auto user_key() const -> Slice { return ExtractUserKey(rep_); }

  void SetFrom(const ParsedInternalKey &p) {
    rep_.clear();
    AppendInteralKey(&rep_, p);
  }

  void Clear() { rep_.clear(); }

  auto DebugString() const -> std::string;
};

inline auto InternalKeyComparator::Compare(const InternalKey &a,
                                           const InternalKey &b) const -> int {
  return Compare(a.Encode(), b.Encode());
}

inline auto ParseInternalKey(const Slice &internal_key,
                              ParsedInternalKey *result) -> bool {
  const size_t n = internal_key.size();
  if (n < 8) { return false; }

  // Sequence# is the last 8 bytes
  uint64_t num = DecodeFixed32(internal_key.data() + n - 8);
  // Sequence# last 8 bits is ValueType.
  uint8_t c = num & 0xff;
  result->sequence = num >> 8;
  result->type = static_cast<ValueType>(c);
  result->user_key = Slice(internal_key.data(), internal_key.size() - 8);
  // legal value is 0x1, 0x0 in currently
  return (c <= static_cast<uint8_t>(kTypeValue));
}

// A helper class useful for DBImpl::Get()
class LookupKey {
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  LookupKey(const Slice &user_key, SequenceNumber sequence);

  REMOVE_COPY_CONSTRUCTOR(LookupKey);

  ~LookupKey();

  auto memtable_key() const -> Slice {
    return Slice(start_, end_ - start_);
  }

  auto internal_key() const -> Slice {
    return Slice(kstart_, end_ - kstart_);
  }

  auto user_key() const -> Slice {
    // skip 8 bytes' tag
    return Slice(kstart_, end_ - kstart_ - 8);
  }
 private:
  // We construct a char array of the form:
  //       klength varint32        <-- start_
  //       userkey char[klength]   <-- kstart_ 
  //       tag     uint64         
  //                               <-- end_
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be userd as an InternalKey.
  const char *start_;
  const char *kstart_;
  const char *end_;
  char space_[200]; // Avoid allocation for short keys
};

inline LookupKey::~LookupKey() {
  if (start_ != space_) { delete [] start_; }
}
} // namespace my_leveldb