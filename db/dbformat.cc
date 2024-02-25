
#include "db/dbformat.h"
#include "util/coding.h"
#include <cstdio>
#include <sstream>

namespace my_leveldb {

static auto PackSequenceAndType(uint64_t seq, ValueType t) -> uint64_t {
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | t;
}

void AppendInteralKey(std::string *result, const ParsedInternalKey &key) {
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

auto ParsedInternalKey::DebugString() const -> std::string {
  std::ostringstream ss;
  ss << '\'' << EscapeString(user_key.ToString()) << "' @ " << sequence << " : "
     << static_cast<int>(type);
  return ss.str();
}

auto InternalKey::DebugString() const -> std::string {
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {
    return parsed.DebugString();
  }
  std::ostringstream ss;
  ss << "(bad)" << EscapeString(rep_);
  return ss.str();
  
}

auto InternalKeyComparator::Name() const -> const char * {
  return "leveldb.InternalKeyComparator";
}

auto InternalKeyComparator::Compare(const Slice &a, const Slice &b) const -> int{
  // Order by:
  // increasing user key (according to user-supplied comparator)
  // decreasing sequence number
  // decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_->Compare(ExtractUserKey(a), ExtractUserKey(b));
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(a.data() + a.size() - 8);
    const uint64_t bnum = DecodeFixed64(b.data() + b.size() - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = 1;
    }
  }
  return r;
}

void InternalKeyComparator::FindShortestSeparator(std::string *start,
                                                  const Slice &limit) const {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() < user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string *key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() < user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

auto InternalFilterPolicy::Name() const -> const char *{ return user_policy_->Name(); }

void InternalFilterPolicy::CreateFilter(const Slice *keys, int n,
                                        std::string *dst) const {
  // We rely on the fact that the code in table.cc does not mind us
  // adjusting keys[].
  Slice *mkey = const_cast<Slice *>(keys);
  for (int i = 0; i < n; ++i) {
    mkey[i] = ExtractUserKey(keys[i]);
  }
  user_policy_->CreateFilter(mkey, n, dst);
}

auto InternalFilterPolicy::KeyMayMatch(const Slice &key, const Slice &f) const -> bool {
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

LookupKey::LookupKey(const Slice &user_key, SequenceNumber s) {
  size_t usize = user_key.size();
  // varint32 may be 5 bytes
  size_t needed = usize + 13; // A conservative estimate
  char *dst;
  // encoded: varint32 usize
  //          user_key.data()
  //          sequenceNumber
  //          type
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  dst = EncodeVarint32(dst, usize + 8);
  kstart_ = dst;
  std::memcpy(dst, user_key.data(), usize);
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}


}