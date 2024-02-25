
#include "db/filename.h"
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "leveldb/slice.h"
#include "leveldb/table.h"
#include "db/version_edit.h"
#include "util/logging.h"
#include "leveldb/env.h"
#include "db/write_batch_internal.h"
#include "leveldb/dumpfile.h"

namespace my_leveldb {

namespace {

bool GuessType(const std::string &fname, FileType *type) {
  size_t pos = fname.rfind('/');
  std::string basename;
  if (pos == std::string::npos) {
    basename = fname;
  } else {
    basename = std::string(fname.data() + pos + 1, fname.size() - pos - 1);
  }
  uint64_t ignored;
  return ParseFileName(basename, &ignored, type);
}

// Notified when log reader encounters corruption.
class CorruptionReporter : public log::Reader::Reporter {
 public:
  void Corruption(size_t bytes, const Status &s) override {
    std::string r = "corruption: ";
    AppendNumberTo(&r, bytes);
    r += " bytes; ";
    r += s.ToString();
    r.push_back('\n');
    dst_->Append(r);
  }
 WritableFile *dst_;
};

// Print contents of a log file. (*func)() is called on every record
Status PrintLogContents(Env *env, const std::string &fname,
                        void (*func)(uint64_t, Slice, WritableFile*),
                        WritableFile *dst) {
  SequentialFile *file;
  Status s = env->NewSequentialFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  CorruptionReporter reporter;
  reporter.dst_ = dst;
  log::Reader reader(file, &reporter, true, 0);
  Slice record;
  std::string scratch;
  while (reader.ReadRecord(&record, &scratch)) {
    (*func)(reader.LastRecordOffset(), record, dst);
  }
  delete file;
  return s;
}

class WriteBatchItemPrinter : public WriteBatch::Handler {
 public:
  void Put(const Slice &key, const Slice &value) override {
    std::string r = "  put '";
    AppendEscapeStringTo(&r, key);
    r += "' '";
    AppendEscapeStringTo(&r, value);
    r += "\n";
    dst_->Append(r);

  }

  void Delete(const Slice &key) override {
    std::string r = "   del   '";
    AppendEscapeStringTo(&r, key);
    r += "'\n";
    dst_->Append(r);
  };

  WritableFile *dst_;
};

// Called on every log record (each one of which is a WriteBatch)
// found in a kLogFile.
static void WriteBatchPrinter(uint64_t pos, Slice record, WritableFile *dst) {
  std::string r = "---- offset ";
  AppendNumberTo(&r, pos);
  r += ";";
  if (record.size() < 12) {
    r += "log record length ";
    AppendNumberTo(&r, record.size());
    r += " is too small\n";
    dst->Append(r);
    return;
  }

  WriteBatch batch;
  WriteBatchItemPrinter batch_item_printer;
  batch_item_printer.dst_ = dst;
  Status s = batch.Iterate(&batch_item_printer);
  if (!s.ok()) {
    dst->Append("   error: " + s.ToString() + "\n");
  }
}

Status DumpLog(Env *env, const std::string &fname, WritableFile *dst) {
  return PrintLogContents(env, fname, WriteBatchPrinter, dst);
}

// Called on every log record (each one of which s a WriteBath)
// found in a kDescriptorfile.
static void VersionEditPrinter(uint64_t pos, Slice record, WritableFile *dst) {
  std::string r = "--- offset";
  AppendNumberTo(&r, pos);
  r += "; ";
  VersionEdit edit;
  Status s = edit.DecodeFrom(record);
  if (!s.ok()) {
    r += s.ToString();
    r.push_back('\n');
  } else {
    r += edit.DebugString();
  }
  dst->Append(r);
}

Status DumpDescriptor(Env *env, const std::string &fname, WritableFile *dst) {
  return PrintLogContents(env, fname, VersionEditPrinter, dst);
}

Status DumpTable(Env *env, const std::string &fname, WritableFile *dst) {
  uint64_t file_size;
  RandomAccessFile *file = nullptr;
  Table *table = nullptr;
  Status s = env->GetFileSize(fname, &file_size);
  if (s.ok()) {
    s = env->NewRandomAccessFile(fname, &file);
  }
  if (s.ok()) {
    // We use the default comparator, which may or may not match 
    // the comparator used in this database.  However this could not
    // cause problems since we only use Table operations that do not 
    // require any comparasions.  In particular, we do not call Seek
    // or Prev.
    s = Table::Open(Options(), file, file_size, &table);
  }
  if (!s.ok()) {
    delete table;
    delete file;
    return s;
  }

  ReadOptions op;
  op.fill_cache = false;
  Iterator *iter = table->NewIterator(op);
  std::string r;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    r.clear();
    ParsedInternalKey key;
    if (!ParseInternalKey(iter->key(), &key)) {
      r = "badkey '";
      AppendEscapeStringTo(&r, iter->key());
      r += "' => '";
      AppendEscapeStringTo(&r, iter->value());
      r += "'\n";
      dst->Append(r);
    } else {
      r = "'";
      AppendEscapeStringTo(&r, key.user_key);
      r += "' @";
      AppendNumberTo(&r, key.sequence);
      r += " : ";
      if (key.type == kTypeDeletion) {
        r += "del";
      }  else if (key.type == kTypeValue) {
        r += "val";
      } else {
        AppendNumberTo(&r, key.type);
      }
      r += " => '";
      AppendEscapeStringTo(&r, iter->value());
      r += "'\n";
      dst->Append(r);

    }
  }
  s = iter->status();
  if (!s.ok()) {
    dst->Append("iterator error: " + s.ToString() + "\n");
  }

  delete iter;
  delete table;
  delete file;
  return Status::OK();
}

} // namespace

Status DumpFile(Env *env, const std::string &fname, WritableFile *dst) {
  FileType ftype;
  if (!GuessType(fname, &ftype)) {
    return Status::InvalidArgument(fname + ": unknown file type");
  }
  switch (ftype){
  case kLogFile/* constant-expression */:
    /* code */
    return DumpLog(env, fname, dst);
  case kDescriptorFile:
    return DumpDescriptor(env, fname, dst);
  case kTableFile:
    return DumpTable(env, fname, dst);
  default:
    break;
  }
  return Status::InvalidArgument(fname + ": not a dump-able file");
}


} // namespace my_leveldb