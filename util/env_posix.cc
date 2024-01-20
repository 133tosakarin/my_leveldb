
#include <unistd.h>
#include <fcntl.h>
#include <atomic>
#include <sys/mman.h>
#include <dirent.h>
#ifndef __Fuchsia__
#include <sys/resource.h>
#endif
#include <sys/stat.h>
#include <sys/types.h>

#include <mutex>
#include <set>
#include <condition_variable>
#include <queue>
#include <cstdio>
#include <thread>
#include <limits>


#include "leveldb/env.h"
#include "util/posix_logger.h"
#include "util/env_posix_test_helper.h"
#include "port/port.h"
namespace my_leveldb {
namespace {

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
    Status status = SyncDirIfManifset();
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

  // Ensure that all the caches associated with the given file descriptor's
  // data are flushed all the way to durable media, and can withstand power failures.
  //
  // The path argument is only used to populate the description string in the
  // returned Status if an error occurs.
  static Status SyncFd(int fd, std::string const& fd_path) {
#if HAVE_FULLFSYNC
    // On macOs and IOS, fsync() doesn't guarantee durability post power
    // failures. fcntl(F_FULLFSYNC) is required for that purpose. Some
    // filesystem don't support fcntl(F_FULLFSYNC), and require a fallback to fsync().
    if (::fcntl(fd, F_FULLFSYNC) == 0) {
      return Statsu::OK();
    }
#endif  // HAVE_FULLFSYNC

#if HAVE_FDDATASYNC
    bool sync_success = ::fddatasync(fd) == 0;
#else
    bool sync_success = ::fsync(fd) == 0;
#endif  // HAVE_FULLFSYNC

    if (sync_success) {
      return Status::OK();
    }
    return PosixError(fd_path, errno);
  }

  // Returns the directory name in a path pointing to a file.
  //
  // Returns "." if the path does not contain any directory separator.
  static std::string Dirname(const std::string &filename) {
    std::string::size_type separator_pos = filename.rfind('/');
    if (separator_pos == std::string::npos) {
      return std::string(".");
    }
    // The filename component should not contain a path separator. If it does,
    // the splitting was done incorrectly.
    assert(filename.find('/', separator_pos + 1) == std::string::npos);
    return filename.substr(0, separator_pos);
  }

  // Extracts the file name from a path pointing to a file.
  //
  // The returned Slice points to |filename|'s data buffer, so it is only valid
  // while |filename| is alive and unchanged.
  static Slice Basename(const std::string &filename) {
    std::string::size_type separator_pos = filename.rfind('/');
    if (separator_pos == std::string::npos) {
      return Slice(filename);
    }
    // The filename component should not contain a path separator. If it does,
    // the splitting was done incorrctly.

    return Slice(filename.data() + separator_pos + 1, filename.length() - separator_pos - 1);
  }

  // True if the given file is a manifest file.
  static bool IsManifest(const std::string &filename) {
    return Basename(filename).starts_with("MANIFEST");
  }
  char buf_[kWritableFileBufferSize];
  size_t pos_;
  int fd_;

  const bool is_manifest_;  // True if the file's name with MANIFEST.
  const std::string filename_;
  const std::string dirname_;   // The directory of filename_;
};

int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct ::flock file_lock_info;
  std::memset(&file_lock_info, 0, sizeof(file_lock_info));
  file_lock_info.l_type = (lock ? F_WRLCK : F_UNLCK);
  file_lock_info.l_whence = SEEK_SET;
  file_lock_info.l_start = 0;
  file_lock_info.l_len = 0;   // Lock/unlock entire file
  return ::fcntl(fd, F_SETLK, &file_lock_info);
}

// Instances are thread-safe because they are immutable.
class PosixFileLock : public FileLock {
 public:
  PosixFileLock(int fd, std::string filename)
      : fd_(fd), filename_(std::move(filename)) {}
  int fd() const { return fd_; }
  const std::string &filename() const { return filename_; }
 private:
  const int fd_;
  const std::string filename_;
};

// Tracks the files locked by PosixEnv::LockFile().
//
// We maintain a separate set instread of relying on fctnl(F_SETLK) because
// fcntl(F_SETLK) does not provide any protection against multiple uses from the
// same process.
// Instance are thread-safe because all member data is guarded by a mutex.
class PosixLockTable {
 public:
  bool Insert(const std::string &fname) {
    std::lock_guard lock(mutex_);
    bool success = locked_files_.insert(fname).second;
    return success;
  }
  
  void Remove(const std::string &fname) {
    std::lock_guard lock(mutex_);
    locked_files_.erase(fname);
  }

 private:

  std::mutex mutex_;
  std::set<std::string> locked_files_;
};

class PosixEnv : public Env {
 public:
  PosixEnv();
  ~PosixEnv() override {
    static constexpr char msg[] = 
        "PosixEnv singleton destroyed. Unsupported behavior!\n";
    std::fwrite(msg, 1, sizeof(msg), stderr);
    std::abort();
  }

  Status NewSequentialFile(const std::string &filename,
                            SequentialFile **result) override {
    int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }
    *result = new PosixSequentialFile(filename, fd);
    return Status::OK();
  }

  Status NewRandomAccessFile(const std::string &filename,
                            RandomAccessFile **result) override {
    int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }
    if (!mmap_limiter_.Acquire()) {
      *result = new PosixRandomAccessFile(filename, fd, &fd_limiter_);
      return Status::OK();
    }

    uint64_t file_size;
    Status status = GetFileSize(filename, &file_size);
    if (status.ok()) {
      void *mmap_base = 
          ::mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
      if (mmap_base != MAP_FAILED) {
        *result = new PosixMmapReadableFile(filename,
                                          reinterpret_cast<char*>(mmap_base),
                                          file_size, &mmap_limiter_);
      } else {
        status = PosixError(filename, errno);
      }
    }
    ::close(fd);
    if (!status.ok()) {
      mmap_limiter_.Release();
    }
    return status;
  }

  Status NewWritableFile(const std::string &filename,
                         WritableFile **result) override {
    int fd = ::open(filename.c_str(),
                    O_TRUNC | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }
    *result = new PosixWritableFile(filename, fd);
    return Status::OK();
  }

  Status NewAppendableFile(const std::string &filename,
                         WritableFile **result) override {
    int fd = ::open(filename.c_str(),
                    O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }
    *result = new PosixWritableFile(filename, fd);
    return Status::OK();
  }

  bool FileExists(const std::string &filename) override {
    return ::access(filename.c_str(), F_OK) == 0;
  }

  Status GetChildren(const std::string &directory_path,
                      std::vector<std::string> *result) override {
    result->clear();
    ::DIR *dir = ::opendir(directory_path.c_str());
    if (dir == nullptr) {
      return PosixError(directory_path, errno);
    }
    struct ::dirent *entry;
    while((entry = ::readdir(dir)) != nullptr) {
      result->emplace_back(entry->d_name);
    }
    ::closedir(dir);
    return Status::OK();
  }

  Status RemoveFile(const std::string &filename) override {
    if (::unlink(filename.c_str()) != 0) {
      PosixError(filename, errno);
    }
    return Status::OK();
  }

  Status CreateDir(const std::string &dirname) override {
    if (::mkdir(dirname.c_str(), 0755) != 0) {
      return PosixError(dirname, errno);
    }
    return Status::OK();
  }

  Status RemoveDir(const std::string &dirname) override {
    if (::rmdir(dirname.c_str()) != 0) {
      return PosixError(dirname, errno);
    }
    return Status::OK();
  }

  Status GetFileSize(const std::string &filename, uint64_t *size) override {
    struct ::stat file_stat;
    if (::stat(filename.c_str(), &file_stat) !=0) {
      *size = 0;
      return PosixError(filename, errno);
    }
    *size = file_stat.st_size;
    return Status::OK();
  }

  Status RenameFile(const std::string &from, const std::string &to) override {
    if (std::rename(from.c_str(), to.c_str()) != 0) {
      return PosixError(from, errno);
    }
    return Status::OK();
  }

  Status LockFile(const std::string &filename, FileLock **lock) override {
    *lock = nullptr;

    int fd = ::open(filename.c_str(), O_RDWR | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      return PosixError(filename, errno);
    }
    if (!locks_.Insert(filename)) {
      ::close(fd);
      return Status::IOError("lock " + filename, "already held by process");
    }

    if (LockOrUnlock(fd, true) == -1) {
      int lock_errno = errno;
      ::close(fd);
      locks_.Remove(filename);
      return PosixError("lock " + filename, lock_errno);
    }
    *lock = new PosixFileLock(fd, filename);
    return Status::OK();
  }

  Status UnlockFile(FileLock *lock) override {
    PosixFileLock *posix_file_lock = static_cast<PosixFileLock*>(lock);
    if (LockOrUnlock(posix_file_lock->fd(), false) == -1) {
      return PosixError("unlock " + posix_file_lock->filename(), errno);
    }
    locks_.Remove(posix_file_lock->filename());
    ::close(posix_file_lock->fd());
    delete posix_file_lock;
    return Status::OK();
  }

  void Schedule(void (*background_work_function)(void *background_work_arg),
                void *background_work_arg) override;
  
  void StartThread(void (*thread_main)(void *thread_main_arg),
                    void *thread_main_arg) override {
   std::thread new_thread(thread_main, thread_main_arg);
   new_thread.detach();
  }

  Status GetTestDirectory(std::string *result) override {
    const char *env = std::getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d",
                    static_cast<int>(::geteuid()));
      *result = buf;
    }

    // The Create Dir status is ignored because the directory may already exist.
    CreateDir(*result);
    return Status::OK();
  }

  Status NewLogger(const std::string &filename, Logger **result) override {
    int fd = open(filename.c_str(), O_APPEND | O_WRONLY | O_CREAT| kOpenBaseFlags, 0644);

    if (fd < 0) {
      *result = nullptr;                                 
      return PosixError(filename, errno);
    }

    FILE *fp = fdopen(fd, "w");
    if (fp == nullptr) {
      close(fd);
      *result = nullptr;
      return PosixError(filename, errno);
    } else {
      *result = new PosixLogger(fp);
      return Status::OK();
    }
  }

  uint64_t NowMicros() override {
    static constexpr uint64_t kUsecondPerSecond = 1000000;
    struct ::timeval tv;
    ::gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * kUsecondPerSecond + tv.tv_usec;
  }

  void SleepForMicroseconds(int micros) override {
    std::this_thread::sleep_for(std::chrono::microseconds(micros));
  }

  

 private:
  void BackgroundThreadMain();

  static void BackgroundThreadEntryPoint(PosixEnv *env) {
    env->BackgroundThreadMain();
  }
  struct BackgroundWorkItem {
    explicit BackgroundWorkItem(void (*function)(void *arg), void *arg)
        : function(function), arg(arg) {}
    void (*const function)(void *);
    void *const arg;
  };
  std::mutex background_work_mutex_;
  std::condition_variable background_work_cv_;
  bool started_background_thread_; // TODO atomic ?
  std::queue<BackgroundWorkItem> background_work_queue_;

  PosixLockTable locks_;  // Thread-safe.
  Limiter mmap_limiter_;  // Thread-safe.
  Limiter fd_limiter_;    // Thread-safe.
};

// Return the maximum number of concurrent mmaps.
int MaxMmaps() { return g_mmap_limit; }

// Returns the maximum number of read-only files to keep open.
int MaxOpenFiles() {
  if (g_open_read_only_file_limit >= 0) {
    return g_open_read_only_file_limit;
  }
#ifdef __Fuchsia__
  // Fushsia doesn't implement getrlimit.
  g_open_read_only_file_limit = 50;
#else
  struct ::rlimit rlim;
  if (::getrlimit(RLIMIT_NOFILE, &rlim)) {
    g_open_read_only_file_limit = 50;
  } else if (rlim.rlim_cur == RLIM_INFINITY) {
    g_open_read_only_file_limit = std::numeric_limits<int>::max();
  } else {
    // Allow use of %20 of available file descriptors for read-only files
    g_open_read_only_file_limit = rlim.rlim_cur / 5;
  }
#endif
  return g_open_read_only_file_limit;

}

} // namespace

PosixEnv::PosixEnv()
  : started_background_thread_ {false},
    mmap_limiter_(MaxMmaps()),
    fd_limiter_(MaxOpenFiles()) {}

void PosixEnv::Schedule(
    void (*background_work_function)(void *background_work_arg),
    void *background_work_arg) {
  std::unique_lock lock(background_work_mutex_);
  if (!started_background_thread_) {
    started_background_thread_ = true;
    std::thread background_thread(PosixEnv::BackgroundThreadEntryPoint, this);
    background_thread.detach();
  }
  if (background_work_queue_.empty()) {
    background_work_cv_.notify_one();
  }

  background_work_queue_.emplace(background_work_function, background_work_arg);
}

void PosixEnv::BackgroundThreadMain() {
  while (true) {
    std::unique_lock lock(background_work_mutex_);
    background_work_cv_.wait(lock, [&] { return !background_work_queue_.empty(); });

    assert(!background_work_queue_.empty());
    auto background_work_function = background_work_queue_.front().function;
    void *background_work_arg = background_work_queue_.front().arg;
    background_work_queue_.pop();
    lock.unlock();
    background_work_function(background_work_arg);
  }
}

namespace {
// Wraps an Env instance whose destructor is never created.
//
// Intended usage:
// using PlatformSingletonEnv = SingletonEnv<PlatformEnv>;
// void ConfigurePosixEnv(int param) {
//  PlatformSingletonEnv::AssertEnvNotInitialized();
//  // set global configuration flags.
//  }
// Env* Env::Default() {
//  static PlatformSingletonEnv default_env;
//  return default_env.env()
// }
template <typename EnvType>
class SingletonEnv {
 public:
  SingletonEnv() {
#if !defined(NDBUG)
    env_initialized_.store(true, std::memory_order_relaxed);
#endif
    static_assert(sizeof(env_storage_) >= sizeof(EnvType),
                  "env_storage_ will not fit the Env");
    static_assert(alignof(decltype(env_storage_)) >= alignof(EnvType),
                  "env_storage_ does not meet the Env's alignment needs");
    new (&env_storage_) EnvType();
  }
  ~SingletonEnv() = default;

  REMOVE_COPY_CONSTRUCTOR(SingletonEnv);

  Env *env() { return reinterpret_cast<Env*>(&env_storage_); }
  
  static void AssertEnvNotInitialized() {
#if !defined(NDBUG)
    assert(!env_initialized_.load(std::memory_order_relaxed));
#endif
  }
 private:
  typename std::aligned_storage<sizeof(EnvType), alignof(EnvType)>::type env_storage_;
#if !defined(NDEBUG)
  static std::atomic<bool> env_initialized_;
#endif  // !defined(DEBUG)
};

#if !defined(NDBUG)
template <typename EnvType>
std::atomic<bool> SingletonEnv<EnvType>::env_initialized_;
#endif

using PosixDefaultEnv = SingletonEnv<PosixEnv>;

} // namespace

void EnvPosixTestHelper::SetReadOnlyFDLimit(int limit) {
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_open_read_only_file_limit = limit;
}

void EnvPosixTestHelper::SetReadOnlyMMapLimit(int limit) {
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_mmap_limit;
}

Env* Env::Default() {
  static PosixDefaultEnv env_containter;
  return env_containter.env();
}
} // namespace my_leveldb


