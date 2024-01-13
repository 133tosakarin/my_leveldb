
#pragma once

#include <cstdint>
#include <cstdio>

#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/iterator.h"

namespace my_leveldb {

static const int kMajorVersion = 1;
static const int kMinorVersion = 23;

struct Options;
struct ReadOptions;
struct WriteOptions;
class WriteBatch;

// Abstract handle to particular state of a DB.
// A Snapshot is an immutable object and can therefore be safely
// accessed from multiple threads without any external synchronization.
class Snapshot {
 protected:
  virtual ~Snapshot();
};

// A range of keys
struct Range {
  Range() = default;
  Range(const Slice &s, const Slice &l) : start(s), limit(l) {}

  Slice start; // Included in the range
  Slice limit; // Not included in the range
};

// A DB is a persistent ordered map from keys to values.
// A DB is safe for concurrent access from multiple threads without
// any external synchronization.
class DB {
 public:
  // Open the database with the specified "name".
  // Stores a pointer to a heap-allocated database to *dbptr and returns
  // OK on Success
  // Stores nulptr in *dptr and return a non-OK status on error.
  // Caller should delete *dbptr when it is no longer needed.
  static auto Open(const Options &options, const std::string &name,
                   DB **dbptr) -> Status;

  DB() = default;

  REMOVE_COPY_CONSTRUCTOR(DB);

  virtual ~DB();

  // Set the database entry for "key" to “value". Returns OK on success.
  // and a non-OK status on error.
  // Note: consider setting options.sync = true
  virtual auto Put(const WriteOptions &options, const Slice &key,
                   const Slice &value) -> Status = 0;

  // Remove the database entry (if any) for "key". Returns Ok on
  // success, and a non-OK status on error. It is not an error if "key"
  // did not exist in the database.
  // Note: consider setting options.sync = true.
  virtual auto Delete(const WriteOptions &options, const Slice &key) -> Status = 0;

  // Apply the specified updates to the database
  // Returns OK on success, non-OK on failure.
  // Note: consider setting options.sync = true.
  virtual auto Write(const WriteOptions &options, WriteBatch *updatese) -> Status = 0;

  // If the database contains an entry for "key"  store the 
  // corresponding value in *value and return OK.
  //
  // If there is no entry for "key" leave *value unchanged and return
  // a status for which Status::IsNotFound() return true.
  virtual auto Get(const ReadOptions &options, const Slice &key, std::string *value) -> Status = 0;

  // Return a heap-allocated iterator over the contents of the database.
  // The result of NewIterator() is initially invalid (caller must call one of
  // the seek methods on the iterator before using it).
  //
  // Caller should delete the iterator when it is no longer needed.
  // The returned iterator should be deleted before this db is deleted.
  virtual auto NewItertor(const ReadOptions &options) -> Iterator * = 0;

  // Return a handle to the current DB state. Iterator created with 
  // this handle will all observe a stable snaphot of the current DB
  // state. The caller must call ReleaseSnapshot(result) when the
  // snapshot is no longer needed. 
  virtual auto GetSnapshot() -> const Snapshot * = 0;

  // Release a previously acquired snapshot. The caller must not
  // use "snapshot" after this call.
  virtual void ReleaseSnapshot(const Snapshot *snapshot) = 0;

  // DB implementations can export properties about their state 
  // via this method. If "property" is a valid property understood by
  // this DB implementations, files "*value" with its current value and returns
  // true. Otherwise returns false.
  //
  //
  // Valid property names include:
  //
  // "leveldb.num-files-at-level<N>" - return the number of files at level<N>.,
  //   where <N> is an ASCII representation of a level number (e.g. "0").
  // "leveldb.status" - returns a multi-line string that describes statistics
  //    about the internal operation of the DB.
  // "leveldb.sstables" - return a multi-line string that describes all
  //    of the sstables that make up the db contents.
  // "leveldb.approximate-memory-usage" - returns the approximate  number of 
  //    bytes of memory in use by the DB.
  virtual auto GetProperty(const Slice &property, std::string *value) -> bool = 0;

  // For each i in [0,n-1], store in "sizes[i]", the approximate
  // file system space used by keys in "[range[i]].start .. range[i].limit]".
  //
  // Not that the returned sizes measure file system space usage, so
  // if the user data compresses by a factor of ten, the returned sizes
  // will be one-tenth the size of the corresponding user data size.
  //
  // The retults may not include the sizes of recently written data.
  virtual void GetApproximateSizes(const Range *range, int n, uint64_t *sizes) = 0;

  // Compact the underlying storage for the key range [*begin, *end]
  // In particular, deleted and overwritten versions are discarded,
  // and the data is rearranged to reduce the cost of operations
  // needed to access the data. This operation should typically only
  // be invoked by users who understand the underlying implementation
  //
  // begin == nullptr is treated as a key before all keys in the databases.
  // end == nullptr as a key after all keys in the database.
  // Therefore the following call will compact the entire database:
  //  db->CompactRange(nullptr, nullptr);
  virtual void CompactRange(const Slice *begin, const Slice *end) = 0;

  
 private:
};
// Destroy the contents of the specified database.
// Be very careful using this method.
//
// Note: For backwards compatibility, if DestroyDb is unable to list the
// database files, Status::OK() will still be returned masking this failure.
auto DestroyDB(const std::string &name, const Options &options) -> Status;

// If a DB connot be opened, you may attempt to call this method to
// resurrect as much of the contents of the database as possible.
// Some data may be lost, so be careful when calling this function
// on a database that contains important information.
auto RepairDb(const std::string &dbname, const Options &options) -> Status;
} // namespace leveldb