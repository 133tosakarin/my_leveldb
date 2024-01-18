
#include <unistd.h>
#include <fcntl.h>
#include <atomic>
#include <sys/mman.h>

#include "leveldb/env.h"


namespace my_leveldb {

// Set by EnvPosixTestHelper::SetReadOnlyMMapLimit() and MaxOpenFiles().
int g_open_read_only_file_limit = -1;

// Up to 1000 mmap regions for 64-bit binaries; non for 32-bit
constexpr const int kDefaultMmapLimit = (sizeof(void*) >= 8) ? 1000 : 0;

// Can be set using EnvPosixTestHelper::SetReadOnlyMMapLimit().
int g_mmap_limit = kDefaultMmapLimit;

// Common flags defined for all posix open operations
#if defined(HAVE_O_CLOEXEC)
constexpr const int kOpenBaseFlags = O_CLOEXEC;
#else
constexpr const int kOpenBaseFlags = 0;
#endif // defined(HAVE_O_CLOEXEC)

constexpr const size_t kWritableFileBufferSize = 65536;

Status PosixError(std::string const &context, int error_number) {
  if (error_number == ENOENT) {
    return Status::NotFound(context, std::strerror(error_number));
  } else {
    return Status::IOError(context, std::strerror(error_number));
  }
}

/**
 * Helper class to limit resource usage to avoid exhaustion.
 * Currently used to limit read-only file descriptors and mmap file usage
 * so that we do not run out of file descriptors or virtual memory, or run into
 * kernel performance problems for very large databases.
*/
class Limiter {
 public:
  // Limit maximum number of resources to |max_acquires|.
  Limiter(int max_acquires)
    :
#if !defined(NDEBUG)
      max_acquires_(max_acquires),
#endif // !defined(NDBUG)
      acquire_allowed_(max_acquires) {
    assert(max_acquires >= 0);
  }

  REMOVE_COPY_CONSTRUCTOR(Limiter);

  auto Acquire()-> bool {
    int old_acquire_allowed = 
        acquire_allowed_.fetch_sub(1, std::memory_order_relaxed);
    if (old_acquire_allowed > 0) return true;

    int pre_increment_acquires_allowed = 
      acquire_allowed_.fetch_add(1, std::memory_order_relaxed);
    // Silence compiler warnings about  arguments when NDBUG is defined.
    (void)pre_increment_acquires_allowed;

    // If the check below fails, Release() was called more times than acquire.
    assert(pre_increment_acquires_allowed < max_acquires_);
    return false;
  }

  void Release() {
    int old_acquire_allowed = 
        acquire_allowed_.fetch_add(1, std::memory_order_relaxed);

    (void)old_acquire_allowed;
    assert(old_acquire_allowed < max_acquires_);
  }

 private:
#if !defined(NDEBUG)
  // Catches an excessive number of Release() calls
  const int max_acquires_;
#endif // !defined(NDBUG)

  // The number of available resources.
  //
  // This is a counter and is not tied to the invariants of any other class, so
  // it can be operated on safely using std::memory_order_relaxed.
  std::atomic<int> acquire_allowed_;
};

// Implements sequential read access in a file using read().
//
// Instance of this class are thread-friendly buf not thread-safe, as required
// by the SequentialFile API.
class PosixSequentialFile final : public SequentialFile {
 public:
  PosixSequentialFile(std::string filename, int fd)
      : fd_(fd), filename_(std::move(filename)) {}
  
  ~PosixSequentialFile() override { close(fd_);}

  Status Read(size_t n, Slice *result, char *scratch) override {
    Status status;
    while (true) {
      ::size_t read_size = ::read(fd_, scratch, n);
      if (read_size < 0) {
        if (errno == EINTR) {
          continue;
        }
        status = PosixError(filename_, errno);
        break;
      }
      *result = Slice(scratch, read_size);
      break;
    }
    return status;
  }

  Status Skip(uint64_t n) override {
    if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
      return PosixError(filename_, errno);
    }
    return Status::OK();
  }
 private:
  const int fd_;
  const std::string filename_;
};

// Implements random read access in a file using pread().
//
// Instance of this class are thread-safe, as acquired by the RandomAccessFile
// API. Instance are immutable and Read() only calls thread-safe library func
class PosixRandomAccessFile final : public RandomAccessFile {
 public:
  PosixRandomAccessFile(std::string filename, int fd, Limiter *fd_limiter)
      : has_permanent_fd_ {fd_limiter_->Acquire()},
        fd_ {has_permanent_fd_ ? fd : -1},
        fd_limiter_ {fd_limiter},
        filename_(std::move(filename)) {
    if (!has_permanent_fd_) {
      assert(fd == -1);
      ::close(fd);  // The file will be opended on every read.
    }
  }

  ~PosixRandomAccessFile() override {
    if (has_permanent_fd_) {
      assert(fd_ != -1);
      ::close(fd_);
      fd_limiter_->Release();
    }
  }

  Status Read(uint64_t offset, size_t n, Slice *result,
              char *scratch) const override {
    int fd = fd_;
    if (!has_permanent_fd_) {
      fd = ::open(filename_.c_str(), O_RDONLY | kOpenBaseFlags);
      if (fd < 0) {
        return PosixError(filename_, errno);
      }
    }

    assert(fd != -1);

    Status status;
    ssize_t read_size = ::pread(fd, scratch, n, offset);
    *result = Slice(scratch, (read_size < 0) ? 0 : read_size);
    if (read_size < 0) {
      status = PosixError(filename_, errno);
    }

    if (!has_permanent_fd_) {
      assert(fd != fd_);
      ::close(fd);
    }
    return status;
  }
 private:

  const bool has_permanent_fd_; // If false, the file is opended on every read.
  const int fd_;                // -1 if has_permanet_fd_ is fales.
  Limiter *const fd_limiter_;
  const std::string filename_;
};


// Implements random read access in a file usign mmap().
//
// Instance of this class are thread-safe, as required by the RandomAccessFile
// AP.
class PosixMmapReadableFile final : public RandomAccessFile {
 public:

  // mmap_base[0, length-1] points to the memory-mmaped contents of the file. It 
  // must be the result of a successful call to mmap(). This instance takes 
  // over the ownership of the region
  //
  // |mmap_limiter| must outlive this instance. The caller must have already
  // acquired the right to use once mmap region, which will be released when this
  // instance is destroyed.
  PosixMmapReadableFile(std::string filename, char *mmap_base,
                        size_t length, Limiter *mmap_limiter)
      : mmap_base_(mmap_base) ,
        length_(length),
        mmap_limiter_(mmap_limiter),
        filename_(std::move(filename)) {}
 
  ~PosixMmapReadableFile() override {
    ::munmap(mmap_base_, length_);
    mmap_limiter_->Release();
  }

  Status Read(uint64_t offset, size_t n, Slice *result,
              char *scratch) const override {
    if (offset + n > length_) {
      *result = Slice();
      return PosixError(filename_, EINVAL);
    }

    *result = Slice(mmap_base_ + offset, n);
    return Status::OK();


  }
 private:
  char *const mmap_base_;
  const size_t length_;
  Limiter *const mmap_limiter_;
  std::string const filename_;
};

class PosixWritableFile final : public WritableFile {
 public:
  PosixWritableFile(std::string filename, int fd)
    : pos_{0},
      fd_{fd},
      is_manifest_(IsManifest(filename)),
      filename_(std::move(filename)),
      dirname_(Dirname(filename_)) {}

  ~PosixWritableFile() override {
    if (fd_ >= 0) {
      //  Ignoring any potential errors
      Close();
    }
  }

  Status Append(const Slice &data) override {
    size_t write_size = data.size();
    const char *write_data = data.data();

    // Fit as much as possible into buffer.
    size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
    std::memcpy(buf_ + pos_, write_data, copy_size);
    write_data += copy_size;
    write_size -= copy_size;
    pos_ += copy_size;
    if (write_size == 0) {
      return Status::OK();
    }

    // Can't fit in buffer, so need to do at lease one write.
    Status status = FlushBuffer();
    if (!status.ok()) {
      return status;
    }
    if (write_size < kWritableFileBufferSize) {
      std::memcpy(buf_, write_data, write_size);
      pos_ = write_size;
      return Status::OK();
    }
    return WriteUnbuffered(write_data, write_size);
  }

  Status Close() override {
    Status status = FlushBuffer();
    const int close_result = ::close(fd_);
    if (close_result < 0 && status.ok()) {
      status = PosixError(filename_, errno);
    }
    fd_ = -1;
    return status;
  }

  Status Flush() override { return FlushBuffer(); }

  Status Sync() override {
    // Ensure new files refered to by the manifest are in the filesystem.
    //
    // This needs to happen before the manifest file is flushed to disk, to
    // avoid crashing in a state where the manifet refers to files that are not
    // yet on disk.
    Status status = SyncDirIfManifest();
    if (!status.ok()) {
      return status;
    }

    status = FlushBuffer();
    if (!status.ok()) {
      return status;
    }
    return SyncFd(fd_, filename_);
  }
 private:

  Status FlushBuffer() {
    Status status = WriteUnbuffered(buf_, pos_);
    pos_ = 0;
    return status;
  }
  Status WriteUnbuffered(const char *data, size_t size) {
    while (size > 0) {
      ssize_t write_result = ::write(fd_, data, size);
      if (write_result < 0) {
        if (errno == EINTR) {
          continue;
        }
        return PosixError(filename_, errno);
      }

      data += write_result;
      size -= write_result;
    }
    return Status::OK();
  }

  Status SyncDirIfManifset() {
    Status status;
    if (!is_manifest_) {
      return status;
    }

    int fd = ::open(dirname_.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      status = PosixError(dirname_, errno);
    } else {
      status = SyncFd(fd, dirname_);
      ::close(fd);
    }
    return status;
  }
  char buf_[kWritableFileBufferSize];
  size_t pos_;
  int fd_;

  const bool is_manifest_;  // True if the file's name with MANIFEST.
  const std::string filename_;
  const std::string dirname_;   // The directory of filename_;
};
}