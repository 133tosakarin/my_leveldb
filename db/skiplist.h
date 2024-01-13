#ifndef STORAGE_MY_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_MY_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// ------------
// Writes require external synchronization, most likely a mutex
// Reads require a guarantee that the SkipList wiil not be destroyed
// while the read is in progess. Apart from that, reads progress
// without any internal locking or synchronization.
// Invariants:
// (1) Allocated nodes are never deleted util the SkipList is
// destroyed. This is trivially guaranteed by the code since we
// never delete any skip list nodes
// 
// (2) The contents of a Node except for next/prev pointers are
// immutable after the Node has been linked into the skipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or more lists.

#include <atomic>
#include <cassert>
#include <cstdlib>


#include "util/arena.h"
#include "util/random.h"
namespace my_leveldb {

template<typename Key, class Comparator>
class SkipList  {
 private:
  struct Node;
 
 public:
  // create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena". Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena *arena);

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // Insert key into the list.
  // REQUIRES: nothing that compare equal to key is currently in the list
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  auto Contains(const Key &key) const -> bool;

  // Iterator over the contents of a skip list
  class Iterator {
   public:

   // Initialsize an iterator over the specified list.
   // The returned iterator is not valid.
    explicit Iterator(const SkipList *list);

    // Returns true iff the iterator is positioned at a valid node.

    auto Valid() const -> bool;

    // Returns the key at the current position
    // Requires: Valid()
    auto key() const -> const Key& ;

    // Advances to the next position.
    // Requires: Valid()

    void Next();
    // Advances to the previous position.
    // Requires: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key &target);

    // Position at the first entry in list.
    // Finial state of iterator is Valid() if list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Finial state of iterator is Valid() if list is not empty.
    void SeekToLast();

   private:
    const SkipList *list_;
    Node *node_;

  };

 private:
  enum { kMaxHeight = 12 };

  inline auto GetMaxHeight() const -> int {
    return max_height_.load(std::memory_order_relaxed);
  }

  auto NewNode(const Key &key, int height) -> Node*;
  auto RandomHeight() -> int;
  auto Equal(const Key &a, const Key &b) -> bool { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "nd"
  auto KeyIsAfterNode(const Key &key, Node *nd) const -> bool;

  // Return the earlist node that comes at or after key.
  // Return nullptr if there is no such node.
  // 
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0, max_height_ - 1]
  auto FindGreaterOrEqual(const Key &key, Node **prev) const -> Node*;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  auto FindLessThan(const Key &key) const -> Node*;

  // Return the last node in the list.
  // Return head_ if list is empty.
  auto FindLast() const -> Node*;

  // Immutable after construction
  Arena *const arena_; // Arena used for allocations of nodes
  Node *const head_;
  Comparator const compare_;
  // Modified only be Insert(). Read racily by readers, but
  // stale values are ok.

  std::atomic<int> max_height_;

  // Read/Written only be Insert().
  Random rand_;
};

// Implements details follow
template<typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
 explicit Node(const Key& k) : key(k) {}
  Key const key;

  // Assessors/mutators for links. Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  auto Next(int n) -> Node*{
    assert(n >= 0);
    // Use an 'acquire load' so that we oberve a fully initialized
    // version of the returned Node.
    return next_[n].load(std::memory_order_acquire);
  }

  void SetNext(int n, Node *nd) {
    assert(n >= 0);

    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    return next_[n].store(nd, std::memory_order_release);
  }

  // No-barrier variants that can be safely used in a few location
  auto NoBarrier_Next(int n) -> Node* {
    assert(n >= 0);

    return next_[n].load(std::memory_order_relaxed);
  }

  void NoBarrier_SetNext(int n, Node *nd) {
    assert(n >= 0);
    return next_[n].store(nd, std::memory_order_relaxed);
  }

 private:
  std::atomic<Node*> next_[1];
};

template<typename Key, class Comparator>
auto SkipList<Key, Comparator>::NewNode(const Key &key, int height) 
  -> SkipList<Key, Comparator>::Node* {
  char *const node_memory = arena_->AllocateAligned(
    sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
    return new (node_memory) Node(key);
}

template<typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList *list) {
  list_ = list;
  node_ = nullptr;
}

template<typename Key, class Comparator>
inline auto SkipList<Key, Comparator>::Iterator::Valid() const -> bool {
  return node_ != nullptr;
}

template<typename Key, class Comparator>
inline auto SkipList<Key, Comparator>::Iterator::key() const -> const Key& {
  assert(Valid());

  return node_->key;
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key &target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
  // if (list_->compare_(node_->key, target) != 0) {
  //   node_ = nullptr;
  // }
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  // the skiplist is empty
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template<typename Key, class Comparator>
auto SkipList<Key, Comparator>::RandomHeight() -> int {
  // Increase height with prohability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && rand_.OneIn(kBranching)) {
    height++;
  }

  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

template<typename Key, class Comparator>
auto SkipList<Key, Comparator>::KeyIsAfterNode(const Key &key, Node *nd) const -> bool {
  return (nd != nullptr) && (compare_(nd->key, key) < 0);
}

template<typename Key, class Comparator>
auto SkipList<Key, Comparator>::Contains(const Key &key) const -> bool {
  auto node = FindGreaterOrEqual(key, nullptr);
  if (node == nullptr || compare_(node->key, key) > 0) {
    return false;
  }

  return true;
}

template<typename Key, class Comparator>
auto SkipList<Key, Comparator>::FindGreaterOrEqual(const Key &key, Node **prev) const -> Node* {

  int height = GetMaxHeight() - 1;

  auto x = head_;
  while(true) {
    auto node = x->Next(height);
    if (KeyIsAfterNode(key, node)) {
      // Keep searching in this list
      x = node;
    } else {
      if (prev != nullptr) prev[height] = x;
      if (height == 0) {
        return node;
      } else {
        height--;
      }

    }
  }
  return nullptr;
}

template<typename Key, class Comparator>
auto SkipList<Key, Comparator>::FindLessThan(const Key &key) const -> Node* {
  int height = GetMaxHeight() - 1;
  auto x = head_;
  while(true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    auto node = x->Next(height);
    if (node == nullptr || compare_(node->key, key) >= 0) {
      if (height == 0) {
        return x;
      } else {
        height--;
      }
    } else {
      x = node;
    }
    
  }
  assert(false);
  return nullptr;
}

template<typename Key, class Comparator>
auto SkipList<Key, Comparator>::FindLast() const -> Node* {
  Node *x = head_;
  int height = GetMaxHeight() - 1;
  while (true) {
    auto node = x->Next(height);
    if (node == nullptr) {
      if (height == 0) {
        return x;
      }
      height--;
    } else {
      x = node;
    }
  }
  assert(false);
}

template<typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key &key) {
  Node *prev[kMaxHeight];



  auto x = FindGreaterOrEqual(key, prev);
  assert(x == nullptr || !Equal(x->key, key));
  int new_height = RandomHeight();

  if (new_height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < new_height; ++i) {
      prev[i] = head_;
    }
    /**
     * 
     * It is ok to mutate max_height_without any synchronization
     * with concurrent readers. A concurrent reader that observes
     * the new value of max_height_will see either the old value of
     * new level pointers from head_(nullptr), or a new value seet in the loop below.
     * In the former case the reader will immediatedly drop to the next level since nullptr 
     * sorts afer all keys. In the latter case the reader will use the new node.
    */
    max_height_.store(new_height, std::memory_order_relaxed);
  }
  auto node = NewNode(key, new_height);

  for (int i = 0; i < new_height; ++i) {

    node->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, node);
  }

}

template<typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena *arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0, kMaxHeight)),
      max_height_(1),
      rand_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; ++i) {
    head_->SetNext(i, nullptr);
  }
}
}
#endif
