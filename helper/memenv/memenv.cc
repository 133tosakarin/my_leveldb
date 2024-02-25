

#include "helper/memenv/memenv.h"


#include <cstring>
#include <limits>
#include <map>
#include <string>
#include <vector>
#include <mutex>

#include "leveldb/env.h"
#include "leveldb/status.h"
#include "port/port.h"


namespace my_leveldb {

namespace {

class FileState {
 public:
  // FileState are reference counted. The initial reference count is zero
  // and the caller must call Ref() at least once.
  FileState() : refs_{0}, size_{0} {}
  REMOVE_COPY_CONSTRUCTOR(FileState);

  // Increase the reference count.
  void Ref() {
    std::lock_guard lock(refs_mutex_);
    ++refs_;
  }

  // Decrease the reference count. Delete if this is the last reference.
  void Unref() {
    bool do_delete = false;
    {
      std::lock_guard lock(refs_mutex_);
      --refs_;
      assert(refs_ >= 0);
      if (refs_ <= 0) {
        do_delete = true;
      }
    }

    if (do_delete) {
      delete this;
    }
  }


  auto Size() const -> uint64_t {
    std::lock_guard lock(blocks_mutex_);
    return size_;
  }

  void Truncate() {
    std::lock_guard lock(blocks_mutex_);
    for (char *& block : blocks_) {
      delete [] block;
    }
    blocks_.clear();
    size_ = 0;
  }

  auto Read(uint64_t offset, size_t n, Slice *result, char *scratch) const -> Status {
    std::lock_guard lock(blocks_mutex_);
    if (offset > size_) {
      return Status::IOError("Offset greater than file size.");
    }

    const uint64_t available = size_ - offset;
    if (n > available) {
      n = static_cast<size_t>(available);
    }
    if (n == 0) {
      *result = Slice();
      return Status::OK();
    }

    assert(offset / kBlockSize <= std::numeric_limits<size_t>::max());
    size_t block = static_cast<size_t> (offset / kBlockSize);
    size_t block_offset = offset % kBlockSize;
    size_t bytes_to_copy = n;
    char *dst = scratch;
    while (bytes_to_copy > 0) {
      size_t avail = kBlockSize - block_offset;
      if (avail > bytes_to_copy) {
        avail = bytes_to_copy;
      }
      std::memcpy(dst, blocks_[block] + block_offset, avail);
      bytes_to_copy -= avail;
      block++;
      dst += avail;
      block_offset = 0;
    }

    *result = Slice(scratch, n);
    return Status::OK();
  }

  auto Append(const Slice &data) -> Status {
    const char *src = data.data();
    size_t src_len = data.size();

    std::lock_guard lock(blocks_mutex_);
    while (src_len > 0) {
      size_t avail;
      size_t offset = size_ % kBlockSize;

      if (offset != 0) {
        avail = kBlockSize - offset;
      } else {
        blocks_.push_back(new char[kBlockSize]);
        avail = kBlockSize;
      }

      if (avail > src_len) {
        avail = src_len;
      }
      std::memcpy(blocks_.back() + offset, src, avail);
      src_len -= avail;
      src += avail;
      size_ += avail;
    }

    return Status::OK();
  }


 private:
  enum { kBlockSize = 8 * 1024 };

  // Private since only Unref() should be used to delete it.
  ~FileState() { Truncate(); }
  std::mutex refs_mutex_;
  int refs_;     // use atomic ?

  mutable std::mutex blocks_mutex_;
  std::vector<char *> blocks_;
  uint64_t size_;
};

class SequentialFileImpl : public SequentialFile {
 public:
  explicit SequentialFileImpl(FileState *file) : file_(file), pos_(0) {
    file_->Ref();
  }

  ~SequentialFileImpl() override { file_->Unref(); }

  auto Read(size_t n, Slice *result, char *scratch) -> Status override {
    Status s = file_->Read(pos_, n, result, scratch);
    if (s.ok()) {
      pos_ += result->size();
    }
    return s;
  }

  auto Skip(uint64_t n) -> Status override {
    if (pos_ > file_->Size()) {
      return Status::IOError("pos_ > file_->Size()");
    }

    const uint64_t available = file_->Size() - pos_;
    if (n > available) {
      n = available;
    }

    pos_ += n;
    return Status::OK();
  }
 
 private:
  FileState *file_;
  uint64_t pos_;
};


class RandomAccessFileImple : public RandomAccessFile {
 public:
  explicit RandomAccessFileImple(FileState *file) : file_(file) { file_->Ref(); }

  ~RandomAccessFileImple() override { file_->Unref(); }

  auto Read(uint64_t offset, size_t n, Slice *result,
            char *scratch) const -> Status override {
    return file_->Read(offset, n, result, scratch);
  }
 private:

  FileState *file_;
};

class WritableFileImple : public WritableFile {
 public:
  explicit WritableFileImple(FileState *file) : file_(file) { file_->Ref(); }

  ~WritableFileImple() override { file_->Unref(); }

  auto Append(const Slice &data) -> Status override { return file_->Append(data); }

  auto Close() -> Status override { return Status::OK();}
  auto Flush() -> Status override { return Status::OK();}
  auto Sync() -> Status override { return Status::OK();}
 private:

  FileState *file_;
};

class NoOpLogger : public Logger {
 public:
  void Logv(const char *format, std::va_list ap) override {}
};

class InMemoryEnv : public EnvWrapper {

 public:

  explicit InMemoryEnv(Env *base_env) : EnvWrapper{base_env} {}

  ~InMemoryEnv() override {
    for (const auto &kvp: file_map_) {
      kvp.second->Unref();
    }
  }

  // Partial implementation of the Env interface.
  auto NewSequentialFile(const std::string &fname,
                         SequentialFile **result) -> Status override {
    std::lock_guard lock(mutex_);
    if (file_map_.find(fname) == file_map_.end()) {
      *result = nullptr;
      return Status::IOError(fname, "File not found");
    }

    *result = new SequentialFileImpl(file_map_[fname]);
    return Status::OK();
  }

  auto NewRandomAccessFile(const std::string &fname, RandomAccessFile **result) -> Status override {
    std::lock_guard lock(mutex_);
    if (file_map_.find(fname) == file_map_.end()) {
      *result = nullptr;
      return Status::IOError(fname, "File not found");
    }

    *result = new RandomAccessFileImple(file_map_[fname]);
    return Status::OK();
  }

  auto NewWritableFile(const std::string &fname, WritableFile **result) -> Status override {
    std::lock_guard lock(mutex_);
    FileSystem::iterator it = file_map_.find(fname);

    FileState *file;
    if (it == file_map_.end()) {
      // File is not currently open.
      file = new FileState();
      file->Ref();
      file_map_[fname] = file;
    } else {
      file = it->second;
      file->Truncate();
    }

    *result = new WritableFileImple(file_map_[fname]);
    return Status::OK();
  }

  auto NewAppendableFile(const std::string &fname, 
                         WritableFile **result) -> Status override {
    std::lock_guard lock(mutex_);
    FileState **ptr = &file_map_[fname];
    FileState *file = *ptr;
    if (file == nullptr) {
      file = new FileState();
      file->Ref();
    }
    *result = new WritableFileImple(file);
    return Status::OK();
  } 

  auto FileExists(const std::string &fname) -> bool override {
    std::lock_guard lock(mutex_);
    return file_map_.find(fname) != file_map_.end();
  }

  auto GetChildren(const std::string &dir,
                   std::vector<std::string> *result) -> Status override {
    std::lock_guard lock(mutex_);
    result->clear();

    for (const auto &kvp : file_map_) {
      const std::string &filename = kvp.first;
      if (filename.size() >= dir.size() + 1 && filename[dir.size()] == '/'
          && Slice(filename).starts_with(Slice(dir))) {
        result->push_back(filename.substr(dir.size() + 1));
      }
    }
    return Status::OK();
  }

  void RemoveFileInternal(const std::string &fname) {
    if (file_map_.find(fname) == file_map_.end()) {
      return;
    }
    file_map_[fname]->Unref();
    file_map_.erase(fname);
  }

  Status RemoveFile(const std::string &fname) {
    std::lock_guard lock(mutex_);
    if (file_map_.find(fname) == file_map_.end()) {
      return Status::IOError(fname, "File not found");
    }
    RemoveFileInternal(fname);
    return Status::OK();
  }

  Status CreateDir(const std::string &dirname) override { return Status::OK(); }
  Status RemoveDir(const std::string &dirname) override { return Status::OK(); }

  Status GetFileSize(const std::string &fname, uint64_t *file_size) override {
    std::lock_guard lock(mutex_);
    if (file_map_.find(fname) == file_map_.end()) {
      return Status::IOError(fname, "File not found");
    }
    *file_size = file_map_[fname]->Size();
    return Status::OK();
  }

  Status RenameFile(const std::string &src,
                    const std::string &target) override {
    std::lock_guard lock(mutex_);
    if (file_map_.find(src) == file_map_.end()) {
      return Status::IOError(src, "File not found");
    }
    RemoveFileInternal(target);
    file_map_[target] = file_map_[src];
    file_map_.erase(src);
    return Status::OK();
  }

  Status LockFile(const std::string &fname, FileLock **lock) override {
    *lock = new FileLock;
    return Status::OK();
  }

  Status UnlockFile(FileLock *lock) override {
    delete lock;
    return Status::OK();
  }

  Status GetTestDirectory(std::string *path) override {
    *path = "/test";
    return Status::OK();
  }

  Status NewLogger(const std::string &fname, Logger **result) override {
    *result = new NoOpLogger;
    return Status::OK();
  }
 private:
  // Map from filename to FileState objects, representing a simple file system.
  using FileSystem = std::map<std::string, FileState*>;

  std::mutex mutex_;
  FileSystem file_map_;
};
}

Env *NewMemEnv(Env *base_env) { return new InMemoryEnv{base_env}; }
}
