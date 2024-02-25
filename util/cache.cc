


#include <cassert>

#include <cstdio>
#include <cstdlib>
#include <mutex>
#include "spdlog/spdlog.h"
#include "util/hash.h"

#include "leveldb/cache.h"

namespace my_leveldb {

Cache::~Cache() {}

namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry. The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache. All items in the
// cache are in one list or the other, and never both. Item still referenced 
// by clients but erased from the cache are in neither list. The lists are:
// - in-use:  contains the itens currently referenced by clients, in no
//   particular order.  (This list is used for invarient checking.  If we 
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU: contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methos,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure. Entries
// are kept in a cicular doubly linked list ordered by access time.
struct LRUHandle {
  void *value;
  void (*deleter)(const Slice&, void *value);
  LRUHandle *next_hash;
  LRUHandle *next;
  LRUHandle *prev;
  size_t charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  bool in_cache;  // Whether entry is in the cache.
  uint32_t refs;  // References, including cache reference, if present.
  uint32_t hash;  // Hash of key(); used for fase sharding and comparsions
  char key_data[1]; // Begining of key

  Slice key() const {
    // next is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);
    return Slice(key_data, key_length);
  }
};

// We provide our own simple hash table since it removes a whole bunch
//  of porting hacks and is also faster thatn some of the built-in hash 
// table implementations in some of the compiler/runtime combinations
// we have tested. E.g., read random speeds up by ~5% overthe g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  HandleTable() : length_{0}, elems_{0}, list_{nullptr} { Resize(); }
  ~HandleTable() { delete [] list_; }

  auto Lookup(const Slice &key, uint32_t hash) -> LRUHandle* {
    return *FindPointer(key, hash);
  }

  auto Insert(LRUHandle *h) -> LRUHandle * {
    // spdlog::info("insert handle: {}, h->key(): {}, h->hash {}",
    //              static_cast<void*>(h), h->key().ToString(), h->hash);
    LRUHandle **ptr = FindPointer(h->key(), h->hash);
    LRUHandle *old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;
    if (old == nullptr) {
      ++elems_;
      if (elems_ > length_) {
        // Slice each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  auto Remove(const Slice &key, uint32_t hash) -> LRUHandle *{
    LRUHandle **ptr = FindPointer(key, hash);
    LRUHandle *result = *ptr;
    if (result != nullptr) {
      // std::fprintf(stdout, "remove Handle[%d] ptr: %p, result: %p", hash & length_ - 1, static_cast<void*>(ptr), static_cast<void*>(result));
      // spdlog::info("remove Handle[{}] ptr: %p, result: %p", hash & length_ - 1, static_cast<void*>(ptr), static_cast<void*>(result));
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }
 private:

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash. If there is no suce cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  auto FindPointer(const Slice &key, uint32_t hash) -> LRUHandle ** {
    LRUHandle **ptr = &list_[hash & (length_ - 1)];
    
    // spdlog::info("[{}]key: {}, key.size(): {}, hash: {}, (*ptr)->key(): {}", 
    //               hash & (length_ - 1), key.ToString(), key.size(), hash, (*ptr) != nullptr ? (*ptr)->key().ToString() : "null");
    assert(ptr != nullptr);
    if (ptr != nullptr) {
      // spdlog::info("Find ptr: {}, *ptr: {}", static_cast<void*>(ptr), static_cast<void*>(*ptr));
      // std::fprintf(stderr, "Find ptr: %p, *ptr: %p\n", static_cast<void*>(ptr), static_cast<void*>(*ptr));
      // assert(*ptr != nullptr);
    }

    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    // spdlog::info("first into here");
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle **new_list = new LRUHandle *[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; ++i) {
      LRUHandle *h = list_[i];
      while (h != nullptr) {
        LRUHandle *next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle **ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }

    assert(elems_ == count);
    delete [] list_;
    list_ = new_list;
    length_ = new_length;
  }

  // The table consists of an array of buckets where each bucket is
  // a linked of cache entries that hash into the bucket
  uint32_t length_;
  uint32_t elems_;
  LRUHandle **list_;
};

// a single shard of sharded cache.
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constrcutor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  Cache::Handle* Insert(const Slice &key, uint32_t hash, void *value,
                        size_t charge, void (*deleter)(const Slice &key, void *value));
  Cache::Handle* Lookup(const Slice &key, uint32_t hash);
  void Erase(const Slice &key, uint32_t hash);
  void Release(Cache::Handle *handle);
  void Prune();
  size_t TotalCharge() const {
    std::lock_guard lock(mutex_);
    return usage_;
  }
 private:

  void LRU_Remove(LRUHandle *e);
  void LRU_Append(LRUHandle *list, LRUHandle *e);
  void Ref(LRUHandle *e);
  void Unref(LRUHandle *e);
  bool FinishErase(LRUHandle *e);
  // Initialized before use.
  size_t capacity_;

  // mutex_ protects the following state.
  mutable std::mutex mutex_;
  size_t usage_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is old entry.
  // Entries have refs = 1 and in_cache == true
  LRUHandle lru_;

  // Dummy head of in-use first.
  // Entries are in use by clients, and refs >= 2 and in_cache==true
  LRUHandle in_use_;

  HandleTable table_;
};

LRUCache::LRUCache() : capacity_(0), usage_(0) {
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);
  for (LRUHandle *e = lru_.next; e != &lru_;) {
    LRUHandle *next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);
    Unref(e);
    e = next;
  }
}

auto LRUCache::Lookup(const Slice &key,  uint32_t hash) -> Cache::Handle * {
  std::lock_guard lock(mutex_);
  // spdlog::info("find key: {}, hash: {}",
  //             key.ToString(), hash);
  auto e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle *>(e);
}

void LRUCache::Ref(LRUHandle *e) {
  if (e->refs == 1 && e->in_cache) { // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

void LRUCache::Unref(LRUHandle *e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {
    assert(!e->in_cache);
    // spdlog::info("({})->deleter({}, e->value)", static_cast<void *>(e), e->key().ToString(), e->value);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

void LRUCache::LRU_Remove(LRUHandle *e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle *list, LRUHandle *e) {
  e->next = list;
  e->prev = list->prev;
  e->next->prev = e;
  e->prev->next = e;
}

auto LRUCache::Insert(const Slice &key, uint32_t hash, void *value,
                      size_t charge, void (*deleter)(const Slice &key, void *value)) -> Cache::Handle * {
  std::lock_guard lock(mutex_);

  LRUHandle *e = reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;
  e->key_length = key.size();
  memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    usage_ += charge;
    FinishErase(table_.Insert(e));
  } else { // dont't cache. (capacity_ == 0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }

  while(usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle *old = lru_.next;
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) { // to avoid unused variable when compiled NDBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table. Return whether e != nullptr.
auto LRUCache::FinishErase(LRUHandle *e) -> bool {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != nullptr;
}

void LRUCache::Erase(const Slice &key, uint32_t hash) {
  std::lock_guard lock(mutex_);
  FinishErase(table_.Remove(key, hash));
}

void LRUCache::Prune() {
  std::lock_guard lock(mutex_);
  while (lru_.next != &lru_) {
    LRUHandle *e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    assert(erased);
  }
}

void LRUCache::Release(Cache::Handle *handle) {
  std::lock_guard lock(mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

static constexpr int kNumShardBits = 4;
static constexpr int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];
  std::mutex id_mutex_;
  uint64_t last_id_;

  static inline auto HashSlice(const Slice &s) -> uint32_t {
    return Hash(s.data(), s.size(), 0);
  }
  static auto Shard(uint32_t hash) -> uint32_t { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity) : last_id_ {0} {
    
    // each shard_lru_cache's capacity
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;

    for (int s = 0; s < kNumShards; ++s) {
      shard_[s].SetCapacity(per_shard);
    }

  }

  auto Insert(const Slice &key, void *value, size_t charge,
              void (*deleter)(const Slice &key, void *value)) -> Handle* override {
    // spdlog::info("ShardCache::Insert----key: {}, value: {}", key.ToString(), value);
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }

  auto Lookup(const Slice &key) -> Handle * override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }

  void Release(Handle *handle) override {
    LRUHandle *h = reinterpret_cast<LRUHandle *>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }

  void Erase(const Slice &key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }

  auto Value(Handle *handle) -> void * override  {
    return reinterpret_cast<LRUHandle *>(handle)->value;
  }

  auto NewId() -> uint64_t override {
    std::lock_guard lock(id_mutex_);
    return ++last_id_;
  }

  void Prune() override {
    for (int s = 0; s < kNumShards; ++s) {
      shard_[s].Prune();
    }
  }

  size_t TotalCharge() const override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; ++s) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }



  ~ShardedLRUCache() override {}
};
}

Cache *NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }
}