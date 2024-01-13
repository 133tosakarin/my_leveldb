

#pragma once

#include "leveldb/status.h"
#include <vector>
#include <cstdarg>
#include <string>
#include <cstdint>

namespace my_leveldb {

class SequentialFile;
class RandomAccessFile;
class WritableFile;
class FileLock;
class Logger;
class Env {
 public:
  Env();
  Env(const Env&) = delete;
  auto operator=(const Env&) = delete;

  virtual ~Env();

  // Return a default enviroment suitable for the current operating
  // system. Sophisticated users may wish to provide their own Env
  // implementation instead of relying on this default enviroment.
  //
  // The result of Default() belongs to leveldb and must never be deleted.
  static auto Default() -> Env *;

  // Create an object that sequentially reads the file with the 
  // specified name. On success, stores a pointer to the new file
  // in *result and returns OK.
  // On failure stores nullptr in *result and returns non-OK. If
  // the file does not exist, returns a non-OK status. Implementation
  // should return a NotFound status when the file does not exist.
  // 
  // The returned file will only be accessed by one thread at a time.
  virtual auto NewSequentialFile(const std::string &fname,
                                 SequentialFile **result) -> Status = 0;

  // Create an object supporting random-access reads from the file
  // with the specified name. On success, stores a pointer to the
  // new file in *result and return OK. On failure stores nullptr in
  // *result and returns non-OK. If the file does not exist, returns a
  // non-OK status. Implementations should return a NotFound status when
  // the file does not exist.
  // 
  // The returned file will only be accessed by one thread at a time.
  virtual auto NewRandomAccessFile(const std::string &fname, 
                                   RandomAccessFile **result) -> Status = 0;

  // Create an object that writes to a new file 
  // with the specified name. On success, stores a pointer to the
  // new file in *result and return OK. On failure stores nullptr in
  // *result and returns non-OK.
  // 
  // The returned file will only be accessed by one thread at a time.
  virtual auto NewWritableFile(const std::string &fname,
                               WritableFile **result) -> Status = 0;

  // Create an object that either append to an existing file, or
  // writes to a new file (if the file does not exist to begin with).
  // On success, stores a pointer to the
  // new file in *result and return OK. On failure stores nullptr in
  // *result and returns non-OK.
  // 
  // The returned file will only be accessed by one thread at a time.
  //
  // May return an IsNotSupportedError error if this Env does
  // not allow appending to an existing file. Users of Env (including
  // the leveldb implementation) must be prepared to deal with
  // an Env that does not support appending.
  virtual auto NewAppendableFile(const std::string &fname,
                                 WritableFile **result) -> Status = 0;

  // Returns true iff the named file exists.
  virtual auto FileExists(const std::string &fname) -> bool = 0;

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *result dropped.
  virtual auto GetChildren(const std::string &dir, 
                           std::vector<std::string> *result ) -> Status = 0;

  // Delete the named file.
  //
  // The default implementation calls DeleteFile, to support legacy Env
  // implementations. Updated Env implementations must override
  // RemoveFile and ignore the existence of DeleteFile. Updated code calling
  // into the Env API must call RemoveFile instead of DeleteFile.
  //
  // A future release will remove DeleteDir and the default implementation
  // of RemoveDir
  virtual auto RemoveFile(const std::string &fname) -> Status;

  // DEPRECATED: Modern Env implementations should override RemoveFile instead.
  //
  // The default implementation calls RemovesFile, to support legacy
  // Env user code that calls this method on modern Env implementations. Modern Env user
  // code should call RemoveFile
  //
  // A future release will remove this method.
  virtual auto DeleteFile(const std::string &fname) -> Status;

  // Create the specified directory.
  virtual auto CreateDir(const std::string &dirname) -> Status;


  // Delete the specified directory.
  // 
  // The default implementation calls DeleteDir, to support legacy Env
  // implementations. Updated Env implementations must override RemoveDir and
  // ignore the existence of DeleteDir. Modern code calling into the Env
  // API must call RemoveDir instead of DeleteDir.
  virtual auto RemoveDir(const std::string &dirname) -> Status;

  // DEPRECATED: Modern Env implementations should override RemoveDir instead.
  //
  // The default implementation calls RemoveDir, to support legacy Env user
  // code that calls this method on modern Env implementations. Modern Env user
  // code should call RemoveDir.
  //
  // A future release will remove this method.
  virtual auto DeleteDir(const std::string& dirname) -> Status;

  // Store the size of fname in *file_size.
  virtual auto GetFileSize(const std::string &fname, uint64_t *file_size) -> Status = 0;

  // Rename file src to target
  virtual auto RenameFile(const std::string &src, 
                          const std::string &target) -> Status = 0;

  // Lock the specified file. User to prevent concurrent access to
  // the same db by multiple processes. On failure, stores nullptr in
  // *lock and returns non-OK.
  //
  // On success, stores a pointer to the object that represents the
  // acquired lock in *lock and returns OK. The caller should call
  // UnlockFile(*lock) to release the lock. If the process exists,
  // the lock will be atomically released.
  //
  // If somebody else already holds the lock, finishes immediately
  // with a failere. I.e., this call does not wait for existing locks to go away.
  //
  // May create the named file if it does not already exist.
  virtual auto LockFile(const std::string &fname, FileLock **lock) -> Status = 0;

  // Release the lock acquired by a previous successful call to LockFile.
  // Requires: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  virtual auto UnlockFile(FileLock *lock) -> Status = 0;


  // Arrange to run "(*function)(arg)" once in a background thread.
  //
  // "function" may run in an unspecified thread. Multiple functions
  // added to the same Env may run concurrently in different threads.
  // I.e., the caller may not assume that background work items are serialized.
  virtual void Schedule(void (*function)(void *arg), void *arg) = 0;

  // Start a new thread, invoking "function(arg)" within the new thread.
  // When "function(arg)" returns, the thread will be destroyed.
  virtual void StartThread(void (*function)(void *arg), void *arg) = 0;

  //*path is set to a temporary directory that can be used for testing. It may
  // or may not have just been created. The directory may or may not
  // differ between runs of the same process, but subsequent calls will 
  // return the same directory.
  virtual auto GetTestDirectory(std::string *path) -> Status = 0;

  // Create and return a log file for storing informational messages.
  virtual auto NewLogger(const std::string &fname, Logger **result) -> Status = 0;

  // Returns the number of micro-seconds since some fixed point in time. Only
  // useful for computing deltas of time.
  virtual auto NowMicros() -> uint64_t = 0;

  // Sleep/delay the thread for the prescribed number of micro-seconds.
  virtual void SleepForMicroseconds(int micros) = 0;
};


// A file abstraction for reading sequentailly through a file
class SequentialFile {
 public:
  SequentialFile() = default;

  REMOVE_COPY_CONSTRUCTOR(SequentialFile);

  virtual ~SequentialFile();

  // Read up to "n" bytes from the file. "scratch[0..n-1]" may be
  // written by this routine. Sets "*result" to the data that was
  // read (including if fewer that "n" bytes were successfully read).
  // May set "*result" to point at data in "scratch[0..n-1]", so
  // "scratch[0..n-1]" must be live when "*result" is used.
  // If an error was encounterd, returns a non-OK status.
  //
  // REQUIRES: External synchronization
  virtual auto Read(size_t n, Slice *result, char *scratch) -> Status = 0;

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  //
  // If end of file is reached, skipping will stop at the end of the 
  // file, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  virtual auto Skip(uint64_t n) -> Status = 0;
};

// A file abstraction for randomly reading the contents of a file.
class RandomAccessFile {
 public:
  RandomAccessFile() = default;
  REMOVE_COPY_CONSTRUCTOR(RandomAccessFile);

  virtual ~RandomAccessFile();

  // Read up to "n" bytes from the file starting at "offset".
  // "scratch[0..n-1]" may be written by this routine. Sets "*result"
  // to the data that was read (including if fewer that "n" bytes were
  // successfully read). May set "*result" to point at data in
  // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
  // "*result" is used. If an error was encounterd, returns a 
  // non-OK status.
  //
  // Safe for concurrent use by multiple threads.
  virtual auto Read(uint64_t offset, size_t n, Slice *result,
                    char *scratch) const -> Status = 0;
};

// A file abstraction for sequential writing. The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class WritableFile {
 public:
  WritableFile() = default;
  REMOVE_COPY_CONSTRUCTOR(WritableFile);

  virtual ~WritableFile();

  virtual auto Append(const Slice &data) -> Status = 0;
  virtual auto Close() -> Status = 0;
  virtual auto Flush() -> Status = 0;
  virtual auto Sync() -> Status = 0;
};

// An interface for writing log messages.
class Logger {
 public:
  Logger() = default;

  REMOVE_COPY_CONSTRUCTOR(Logger);

  virtual ~Logger();
  
  // Write an entry to the log file with the specified format.
  virtual void Logv(const char *format, std::va_list ap) = 0;
};


// Identifies a locked file.
class FileLock {
 public:
  FileLock() = default;
  
  REMOVE_COPY_CONSTRUCTOR(FileLock);

  virtual ~FileLock();
};

// Log the specified data to *info_log if info_log is non-null.
void Log(Logger *info_log, const char *format, ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((__format__(__printf__, 2, 3)))
#endif
;

// A utility routine: write "data" to the named file.
auto WriteStringToFile(Env *env, const Slice &data,
                        const std::string &fname) -> Status;

// A utility routine: read contents of named file into *data.
auto ReadFileToString(Env *env, const std::string &fname, 
                      std::string *data) -> Status;

// An implementation of Env that forwards all call to another Env.
// May be useful to clients who wish to override just part of the
// functionality of another Env.
class EnvWrapper : public Env {
 public:
  // Initialize an EnvWrapper that delegates all calls to *t.
  explicit EnvWrapper(Env *t) : target_(t) {}
  virtual ~EnvWrapper();

  // Return the target to which this Env forwards all calls.
  auto target() const -> Env * { return target_; }

  // The following text is boilerplate that forwards all methods to target().
  auto NewSequentialFile(const std::string &fname, SequentialFile **r) -> Status override{
    return target_->NewSequentialFile(fname, r);
  }

  Status NewRandomAccessFile(const std::string& f,
                             RandomAccessFile** r) override {
    return target_->NewRandomAccessFile(f, r);
  }
  Status NewWritableFile(const std::string& f, WritableFile** r) override {
    return target_->NewWritableFile(f, r);
  }
  Status NewAppendableFile(const std::string& f, WritableFile** r) override {
    return target_->NewAppendableFile(f, r);
  }
  bool FileExists(const std::string& f) override {
    return target_->FileExists(f);
  }
  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* r) override {
    return target_->GetChildren(dir, r);
  }
  Status RemoveFile(const std::string& f) override {
    return target_->RemoveFile(f);
  }
  Status CreateDir(const std::string& d) override {
    return target_->CreateDir(d);
  }
  Status RemoveDir(const std::string& d) override {
    return target_->RemoveDir(d);
  }
  Status GetFileSize(const std::string& f, uint64_t* s) override {
    return target_->GetFileSize(f, s);
  }
  Status RenameFile(const std::string& s, const std::string& t) override {
    return target_->RenameFile(s, t);
  }
  Status LockFile(const std::string& f, FileLock** l) override {
    return target_->LockFile(f, l);
  }
  Status UnlockFile(FileLock* l) override { return target_->UnlockFile(l); }
  void Schedule(void (*f)(void*), void* a) override {
    return target_->Schedule(f, a);
  }
  void StartThread(void (*f)(void*), void* a) override {
    return target_->StartThread(f, a);
  }
  Status GetTestDirectory(std::string* path) override {
    return target_->GetTestDirectory(path);
  }
  Status NewLogger(const std::string& fname, Logger** result) override {
    return target_->NewLogger(fname, result);
  }
  uint64_t NowMicros() override { return target_->NowMicros(); }
  void SleepForMicroseconds(int micros) override {
    target_->SleepForMicroseconds(micros);
  }


 private:
  Env *target_;
};
} // namespace my_leveldb


