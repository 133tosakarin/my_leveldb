
#pragma once

/**
 * An iterator yields a sequence of key/value pairs from a souce.
 * The folloing class defines the interface. Multiple implementions
 * are provided by this library. In particular, iterators are provided
 * to access the contents of a Table or a DB
 * 
 * 
 * Multiple threads can invode const methods on an Iterator without
 * external synchronization, but if any of the threads may call a non-const method, 
 * all threads accessing the same Iterator must use external synchronization.
*/


#include "leveldb/slice.h"
#include "leveldb/status.h"


namespace my_leveldb {


class Iterator {
 public:
  Iterator();
  Iterator(const Iterator&) = delete;
  auto operator=(const Iterator&) -> Iterator& = delete;

  virtual ~Iterator();

  // An iterator is either positioned at a key/value pair, or
  // not valid. This method returns true iff the iterator is valid.
  virtual auto Valid() const -> bool = 0;

  // Position at the first key in the source. The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() = 0;

  // Position at the last key in the source. The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToLast() = 0;

  // Position at the first key in the source that is at or past target.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or post target.
  virtual void Seek(const Slice &target) = 0;

  // Moves to the next entry in the source. After this call, Valid() is 
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() = 0;

  // Moves to the previous entry in the source. After this call, Valid()
  // is true iff the iterator was not positioned at the first entry in the source
  // REQUIRES: Valid()
  virtual void Prev() = 0;

  // Return the key for the current entry. The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual auto key() const -> Slice = 0;

  // Return the value for the current entry. The underlying storate for
  // the returned slice is valid only the next modification of the iterator.
  // REQUIRES: Valid()
  virtual auto value() const -> Slice = 0;

  // If an error has occured, return it. Else return an ok status.
  virtual auto status() const -> Status = 0;
  /**
   * Clients are allowed to register function/arg1/arg2 triples that
   * will be invoked when iterator is destroyed
   * 
   * Note that unlike all of the preceding methods, this method is
   * not abstract and therefore clients should not override it.
  */
  using CleanupFunction = void (*)(void *arg1, void *arg2);
  void RegisterCleanup(CleanupFunction function, void *arg1, void *arg2);
 private:
  // Cleanup functions are stored in a single-linked list.
  // The list's head node is inlined in the iterator.
  struct CleanupNode {
    // True if the node is not used. Only head nodes might be unused.
    auto IsEmpty() const -> bool { return function == nullptr; }

    // Invokes the cleanup function.
    void Run() {
      assert(function != nullptr);
      (*function)(arg1, arg2);
    }

    // The head node is used if the function pointer is not null.
    CleanupFunction function;
    void *arg1;
    void *arg2;
    CleanupNode *next;
  };
  CleanupNode cleanup_node_;
};

// Return an empty iterator (yields nothing)
auto NewEmptyIterator() -> Iterator *;

// Return an empty iterator with the specified status.
auto NewErrorIterator(const Status &status) -> Iterator *;
}