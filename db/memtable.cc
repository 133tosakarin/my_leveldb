

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace my_leveldb {

static auto GetLengthPrefixedSlice(const char *data) -> Slice {
  uint32_t len;
  const char *p = data;
  p = GetVarint32Ptr(p, p + 5, &len); // +5: we assume "p" is not corrupted
  return Slice(p, len);
}
MemTable::MemTable(const InternalKeyComparator &comparator)
  : comparator_(comparator), refs_(0), table_(comparator_, &arena_) {}

MemTable::~MemTable() { assert(refs_ ==0); }

auto MemTable::ApproximateMemoryUsage() -> size_t { return arena_.MemoryUsage(); }

auto MemTable::KeyComparator::operator()(const char *aptr, const char *bptr) const -> int {
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static auto EncodeKey(std::string *scratch, const Slice &target) -> const char * {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table *table) : iter_(table) {}
  
  REMOVE_COPY_CONSTRUCTOR(MemTableIterator);

  ~MemTableIterator() = default;

  auto Valid() const -> bool override {
    return iter_.Valid();
  }

  void Seek(const Slice &k) override {
    iter_.Seek(EncodeKey(&tmp_, k));
  }

  void SeekToLast() override {
    iter_.SeekToLast();
  }
  void SeekToFirst() override {
    iter_.SeekToFirst();
  }

  void Next() override { iter_.Next(); }

  void Prev() override { iter_.Prev(); }

  auto key() const -> Slice override { return GetLengthPrefixedSlice(iter_.key());}
  auto value() const -> Slice override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  auto status() const -> Status override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_; // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

void MemTable::Add(SequenceNumber s, ValueType type, const Slice &key,
                   const Slice &value) {
  // Format of an entry is concatentaion of:
  // key_size         : varint32 of internal_key.size()
  // key bytes        : char[internal_key.size()]
  // tag              : uint64((sequence << 8 | type))
  // value_size       : varint32 of value.size()
  // value bytes      : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len = VarintLength(internal_key_size) + 
                             internal_key_size + VarintLength(val_size) + 
                             val_size;
  char *buf = arena_.Allocate(encoded_len);
  char *p = EncodeVarint32(buf, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  table_.Insert(buf);
}

auto MemTable::Get(const LookupKey &key, std::string *value, Status *s) -> bool {
  // LookupKey has memkey and user_key, the before is encode 
  //  8 bytes seq# at last
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    // klength    varint32
    // userkey    char[klength]  
    // tag        uint64
    // vlength    varint32
    //  value     char[vlength]
    const char *entry = iter.key();
    uint32_t key_length;
    const char *key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
      key.user_key(), Slice(key_ptr, key_length - 8)) == 0) {
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);;
      ValueType type = static_cast<ValueType>(tag & 0xff);
      switch (type) {
        case kTypeValue: {
          Slice val = GetLengthPrefixedSlice(key_ptr + key_length);
          value->append(val.data(), val.size());
          return true;
        }
        case kTypeDeletion: {
          *s = Status::NotFound(Slice());
          return true;
        }
      }
    }
  }
  return false;
}


}