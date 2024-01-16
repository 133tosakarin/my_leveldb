#pragma once

#include <algorithm>
#include <string>

#include "leveldb/slice.h"


namespace my_leveldb {

#define REMOVE_COPY_CONSTRUCTOR(ClassName) \
  ClassName(const ClassName&) = delete; \
  ClassName& operator=(const ClassName&) = delete;



class Status {
 public:
 // Create a succes status.
  Status() noexcept : state_(nullptr) {}
  ~Status() { delete [] state_; }

  Status(const Status &rhs);
  auto operator=(const Status &rhs) noexcept -> Status&;

  // Return a success status_
  static auto OK() -> Status { return Status(); }

  // Return error status of an appropriate type
  static auto NotFound(const Slice &msg, const Slice &msg2 = Slice()) -> Status {
    return Status(kNotFound, msg, msg2);
  }

  static auto Corruption(const Slice &msg, const Slice &msg2 = Slice()) -> Status {
    return Status(kCorruption, msg, msg2);
  }

  static auto NotSupported(const Slice &msg, const Slice &msg2 = Slice()) -> Status {
    return Status(kNotSupported, msg, msg2);
  }

  static auto InvalidArgument(const Slice &msg, const Slice &msg2 = Slice()) -> Status {
    return Status(kInvalidArgument, msg, msg2);
  }

  static auto IOError(const Slice &msg, const Slice &msg2 = Slice()) -> Status {
    return Status(kIOError, msg, msg2);
  }


  // Return true iff the status indicates success.
  auto ok() const -> bool {
    return state_ == nullptr; 
  }

  // Returns true iff the status indicates a NotFound error.
  auto IsNotFound() const -> bool {
    return code() == kNotFound;
  }

  // Returns true iff the status indicate a Corruption error.
  auto IsCorruption() const -> bool {
    return code() == kCorruption;
  }

  // Returns true iff the status indicate a NotSupported error.
  auto IsNotSupportedError() const -> bool {
    return code() == kNotSupported;
  }

  // Returns true iff the status indicate an InvalidArgument error
  auto IsInvalidArgument() const -> bool {
    return code() == kInvalidArgument;
  }

  // Returns true iff the statsu indicate an IOError error
  auto IsIOError() const -> bool {
    return code() == kIOError;
  }

  auto ToString() const -> std::string;
 private:
  enum Code {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5
  };

   auto code() const -> Code {
    return (state_ == nullptr) ? kOk : static_cast<Code>(state_[4]);
  }

   Status(Code code, const Slice &msg, const Slice &msg2);
   static auto CopyState(const char *s) -> const char *;
  // OK status has a null state_. Otherwise, state_ is a new[] array
  // of the following form:
  // state_[0...3] == length of message
  // state_[4] == code
  // state[5..] == message 
   const char *state_;
};

inline Status::Status(const Status &rhs) {
  state_ = (rhs.state_ == nullptr ) ? nullptr : CopyState(rhs.state_);
}

inline auto Status::operator=(const Status &rhs) noexcept -> Status& {
  if (state_ != rhs.state_) {
    delete [] state_;
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
  }
  return *this;
}
} // namespace my_leveldb