

#pragma once

#include "db/dbformat.h"
#include "leveldb/db.h"

namespace my_leveldb {

class SnapshotList;

// Snapshots are kept in a double-linked list in the DB.
// Each SnapshotImpl corresponds to a particular sequence number.
class SnapshotImpl : public Snapshot {
 public:
  SnapshotImpl(SequenceNumber sequence_number)
      : sequence_number_(sequence_number) {}
  
  SequenceNumber sequence_number() const { return sequence_number_; }
 
 private:
  friend class SnapshotList;

  // SnapshotImpl is kept in a doubly-linked cicular list. The SnapshotList
  // implementation operates on the next/previous fields directly.
  SnapshotImpl *prev_;
  SnapshotImpl *next_;

  const SequenceNumber sequence_number_;
#if !defined(NDEBUG)
  SnapshotList *list_ = nullptr;
#endif  // !defined(NDEBUG)
};

class SnapshotList {
 public:
  SnapshotList() : head_{0} {
    head_.prev_ = &head_;
    head_.next_ = &head_;
  }

  auto empty() const -> bool { return head_.next_ == &head_; }
  SnapshotImpl* oldest() const {
    assert(!empty());
    return head_.next_;
  }

  SnapshotImpl* newest() const {
    assert(!empty());
    return head_.prev_;
  }

  // Creates a SnapshotImpl and appends it to the end of the list.
  SnapshotImpl* New(SequenceNumber sequence_number) {
    assert(empty() || newest()->sequence_number_ <= sequence_number);
    SnapshotImpl *snapshot = new SnapshotImpl(sequence_number);
#if !defined(NDEBUG)
    snapshot->list_ = this;
#endif
    snapshot->next_ = &head_;
    snapshot->prev_ = head_.prev_;
    snapshot->prev_->next_ = snapshot;
    snapshot->next_->prev_ = snapshot;
    return snapshot;
  }

  /**
   * Removes a SnapshotImpl from this list.
   * 
   * The snapshot must have been creaated by calling New() on 
   * this list.
   * 
   * The snapshot pointer should not be const, because its memory is
   * deallocated. However, that would force us to change DB::ReleaseSnapshot(),
   * which is in the API, and currently takes a const Snapshot.
  */
  void Delete(const SnapshotImpl *snapshot) {
#if !defined(NDEBUG)
    assert(snapshot->list_ == this);
#endif
    snapshot->prev_->next_ = snapshot->next_;
    snapshot->next_->prev_ = snapshot->prev_;
    delete snapshot;
  }
 private:
  // Dummy head of doubly-linked list of snapshots.
  SnapshotImpl head_;
};
}