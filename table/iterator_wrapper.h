
#pragma once

#include "leveldb/iterator.h"
#include "leveldb/slice.h"


namespace my_leveldb {

// A internal wrapper class with on interface similar to Iterator that
// caches the valid() and key() results for an underlying iterator.
// This can help avoid virutal function calls and also gives better
// cache Locality.
class IteratorWrapper {
 public:
  IteratorWrapper() : iter_(nullptr), valid_{false} {}

  explicit IteratorWrapper(Iterator *iter) : iter_(nullptr) { Set(iter); }

  ~IteratorWrapper() { delete iter_; }

  Iterator* iter() const { return iter_; }

  void Set(Iterator *iter) {
    delete iter_;
    iter_ = iter;
    if (iter == nullptr) {
      valid_ = false;
    } else {
      Update();
    }
  }

  bool Valid() const { return valid_; }
  Slice key() const { 
    assert(Valid());
    return key_;
  }

  Status status() const {
    assert(iter_);
    return iter_->status();
  }

  void Next() {
    assert(iter_);
    iter_->Next();
    Update();
  }

  void Prev() {
    assert(iter_);
    iter_->Prev();
    Update();
  }

  void Seek(const Slice &k) { 
    assert(iter_);
    iter_->Seek(k);
    Update();
  }

  void SeekToLast() { 
    assert(iter_);
    iter_->SeekToLast();
    Update();
  }

  void SeekToFirst() {
    assert(iter_);
    iter_->SeekToFirst();
    Update();
  } 

  Slice value() const {
    assert(iter_);
    return iter_->value();
  }
 private:
  void Update() {
    valid_ = iter_->Valid();
    if (valid_) {
      key_ = iter_->key();
    }
  }
  Iterator *iter_;
  bool valid_;
  Slice key_;
};

} // namespace my_leveldb