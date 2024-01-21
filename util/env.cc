
#include "leveldb/env.h"

#include <cstdarg>

namespace my_leveldb {

Env::Env() = default;

Env::~Env() = default;

auto Env::NewAppendableFile(const std::string &fname, WritableFile **result) -> Status {
  return Status::NotSupported("NewAppendableFile", fname);
}

auto Env::RemoveDir(const std::string &dirname) -> Status {
  return DeleteDir(dirname);
}

auto Env::CreateDir(const std::string &dirname) -> Status {
  return Status::OK();
}

auto Env::DeleteDir(const std::string &dirname) -> Status {
  return RemoveDir(dirname);
}

auto Env::DeleteFile(const std::string &dirname) -> Status {
  return RemoveFile(dirname);
}
auto Env::RemoveFile(const std::string &dirname) -> Status {
  return DeleteFile(dirname);
}

SequentialFile::~SequentialFile() = default;
RandomAccessFile::~RandomAccessFile() = default;
WritableFile::~WritableFile() = default;

Logger::~Logger() = default;

FileLock::~FileLock() = default;

void Log(Logger *info_log, const char *format, ...) {
  if (info_log != nullptr) {
    std::va_list ap;
    va_start(ap, format);
    info_log->Logv(format, ap);
    va_end(ap);
  }
}

static auto DoWriteStringToFile(Env *env, const Slice &data,
                                const std::string &fname, bool should_sync) -> Status {
  WritableFile *file;
  Status s = env->NewWritableFile(fname, &file);
  if (!s.ok()) {
    return s;
  }

  s = file->Append(data);
  if (s.ok() && should_sync) {
    s = file->Sync();
  }
  if (s.ok()) {
    s = file->Close();
  }
  delete file; // Will auto-close if we did not close above
  if (!s.ok()) {
    env->RemoveFile(fname);
  }
  return s;
}

auto WriteStringToFile(Env *env, const Slice &data,
                        const std::string &fname) -> Status {
  return DoWriteStringToFile(env, data, fname, false);
}

auto ReadFileToString(Env *env, const std::string &fname, std::string *data) -> Status {
  SequentialFile *file;
  Status s = env->NewSequentialFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  static const int kBufferSize = 8192;
  char *space = new char[kBufferSize];
  while (true) {
    Slice fragment;
    s = file->Read(kBufferSize, &fragment, space);
    if (!s.ok()) {
      break;
    }
    data->append(fragment.data(), fragment.size());
    if (fragment.empty()) {
      break;
    }
  }
  delete [] space;
  delete file;
  return s;
}

EnvWrapper::~EnvWrapper() {}
}