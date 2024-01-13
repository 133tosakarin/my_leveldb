
#pragma once

#include "leveldb/slice.h"
#include "leveldb/status.h"


namespace my_leveldb {

class Cache;

// Create a new cache with a fixed size capacity. This implementation
// of Cache uses a leas-recently-used eviction policy.
Cache *NewLRUCache(size_t capacity);

class Cache {
 public:
  Cache() = default;

  REMOVE_COPY_CONSTRUCTOR(Cache);

  virtual ~Cache();

  // Opaque handle to an enry stored in the cache.
  struct Handle {};

  // Insert a mapping from key-value into the cache and assign it 
  // the specified charge against the total cache capacity.
  //
  // Returns a handle that corresponds to the mapping. The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  //
  // When the inserted entry is no longer needed, the key and 
  // value will be passed to "deleter".
  virtual Handle* Insert(const Slice &key, void *value, size_t charge,
                          void (*deletr)(const Slice &key, void *value)) = 0;
  
  // If the cache has no mapping for "key", returns nullptr.
  //
  // Else return a handle that corresponds to the mapping. The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  virtual auto Lookup(const Slice &key) -> Handle * = 0;

  // Release a mapping returned by a provious Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by
  virtual void Release(Handle *handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual auto Value(Handle *handle) -> void * = 0;

  // If the cache contains entry for key, erase it. Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice &key) = 0;

  // Return a new numeric id. May be used by multiple clients who are
  // sharing the same cache to partition the key space. Typically the 
  // client will allocate a new id at startup and prepend the id to 
  // its cache keys.
  virtual auto NewId() -> uint64_t = 0;

  // Remove all cache entries that are not actively in use. Memory-constrained
  // application may wish to call this method to reduce memory usage.
  // Default implementation of Prune() does nothing. Subclasses are stringly
  // encouraged to override the default implementation. A future release of 
  // leveldb may change Prune() to a pure abstract method.
  virtual void Prune() {}

  // Return an estimate of the combined charges of all elements stored in the
  // cache.
  virtual auto TotalCharge() const -> size_t  = 0;
 private:
};


} // namespace leveldb