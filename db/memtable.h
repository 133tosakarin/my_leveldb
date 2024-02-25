

#pragma once

#include "db/skiplist.h"
#include "util/arena.h"
#include "leveldb/slice.h"
#include "leveldb/iterator.h"
#include "db/dbformat.h"

namespace my_leveldb {

class InternalKeyComparator;
class MemTableIterator;
class MemTable {
 public:

  explicit MemTable(const InternalKeyComparator &comparator);

  REMOVE_COPY_CONSTRUCTOR(MemTable);

  void ref() { ++refs_; }

  // Drop reference count. Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when Memtable is being modified.
  auto ApproximateMemoryUsage() -> size_t;

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live. The keys returned by this
  // iterator are internal keys encoded by AppendInteralKey in the
  // db/format.{h,cc} module.
  auto NewIterator() -> Iterator *;

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type == kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type, const Slice &key,
           const Slice &value);
  
  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  auto Get(const LookupKey &key, std::string *value, Status *s) -> bool;
 private:
  friend class MemTableIterator;
  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator &cmp) : comparator(cmp) {}
    int operator()(const char *a, const char *b) const;
  };

  using Table = SkipList<const char *, KeyComparator>;
  
 ~MemTable(); // Private since only Unref() should be used to delete it.
  KeyComparator comparator_;
  int refs_;
  Arena arena_ {};
  Table table_;
};


}