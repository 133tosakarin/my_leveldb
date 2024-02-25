
/**
 * 
 * We recover the contents of the descriptor from the other file that
 * we find.
 * (1) Any log files are first converted to tables
 * (2) We scan every table to compute
 *    (a) smallest/largest for the table
 *    (b) largest sequence number in the table
 * (3) We generate descriptor contents:
 *      - log number is set to zero
 *      - next-file-number is set to 1 + largest file number we found
 *      - last-sequence-number is set to largest sequence# found across
 *        all tables (see 2c)
 *      - compaction pointers are cleared
 *      - every table file is added at level 0
 * 
 * Possible optimization 1:
 *    (a) Compute total size and use to pick appropriate max-level M
 *    (b) Sort tables by largest sequence# in the table
 *    (c) For each table: if it overlaps earlier table, place in level-0,
 *        else place in level-M.
 * Possible optimization 2:
 *    Store per-table metadata (smallest, largest, largest-seq#, ...)
 *    in the table's meta section to speed up ScanTable.
*/

#include <vector>

#include "db/db_impl.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "db/builder.h"
#include "db/memtable.h"
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "db/table_cache.h"
#include "db/write_batch_internal.h"
#include "db/filename.h"
#include "leveldb/comparator.h"

namespace my_leveldb {

namespace {


class Repairer {
 public:

  Repairer(const std::string &dbname, const Options &options)
      : dbname_(dbname),
        env_(options.env),
        icmp_(options.comparator),
        ipolicy_(options.filter_policy),
        options_(SanitizedOptions(dbname, &icmp_, &ipolicy_, options)),
        owns_info_log_(options_.info_log != options.info_log),
        owns_cache_(options_.block_cache != options.block_cache),
        next_file_number_(1) {
    // TableCache can be small since we expect each table to be
    // opened once.
    table_cache_ = new TableCache(dbname_, options_, 10);
  }

  ~Repairer() {
    delete table_cache_;
    if (owns_info_log_) {
      delete options_.info_log;
    }
    if (owns_cache_) {
      delete options_.block_cache;
    }

  }

  Status Run() {
    Status s = FindFiles();
    if (s.ok()) {
      ConvertLogFileToTables();
      ExtractMetaData();
      s = WriteDescriptor();
    }
    if (s.ok()) {
      uint64_t bytes = 0;
      for (const auto &table : tables_) {
        bytes += table.meta.file_size;
      }
      Log(options_.info_log,
          "*** Repaired levelb %s; "
          "recovered %d files; %llu bytes"
          "Some data may have been lost."
          "****",
          dbname_.c_str(), (int)tables_.size(), bytes);
    }
    return s;
  }
 private:
  struct TableInfo {
    FileMetaData meta;
    SequenceNumber max_sequence;
  };

  Status FindFiles() {
    std::vector<std::string> filenames;
    Status s = env_->GetChildren(dbname_, &filenames);
    if (!s.ok()) {
      return s;
    }
    if (filenames.empty()) {
      return Status::IOError(dbname_, "repair found no files");
    }

    uint64_t number;
    FileType type;
    for (const auto &filename : filenames) {
      if (ParseFileName(filename, &number, &type)) {
        if (type == kDescriptorFile) {
          manifests_.push_back(filename);
        } else {
          if (number + 1 > next_file_number_) {
            next_file_number_ = number + 1;
          }
          if (type == kLogFile) {
            logs_.push_back(number);
          } else if (type == kTableFile) {
            table_numbers_.push_back(number);
          } else {
            // Ignore other files
          }
        }
      }
    }
    return s;
  }

  void ConvertLogFileToTables() {
    for (uint64_t log_number : logs_) {
      std::string logname = LogFileName(dbname_, log_number);
      Status s = ConvertLogToTable(log_number);
      if (!s.ok()) {
        Log(options_.info_log, "Log #%llu: ignoring conversion error: %s",
            log_number, s.ToString().c_str());
      }
      ArchiveFile(logname);
    }
  }

  Status ConvertLogToTable(uint64_t log) {
    struct LogReporter : public log::Reader::Reporter {
      Env *env;
      Logger *info_log;
      uint64_t lognum;
      void Corruption(size_t bytes, const Status &s) override {
        // We print error messages for corruption, but continue repairing.
        Log(info_log, "Log #%llu: dropping %d bytes; %s",
            lognum, static_cast<int>(bytes), s.ToString().c_str());
      }
    };

    // Open the log file
    std::string logname = LogFileName(dbname_, log);
    SequentialFile *lfile;
    Status s = env_->NewSequentialFile(logname, &lfile);
    if (!s.ok()) {
      return s;
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = options_.info_log;
    reporter.lognum = log;
    // We intentionally make log::Reader do checksumming so that
    // Corruptions cause entire commits to be skipped instead of
    // propagting bad information (like overly large sequence numbers).
    log::Reader reader(lfile, &reporter, false, 0);

    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;
    MemTable *mem = new MemTable(icmp_);
    mem->ref();
    int counter = 0;
    while (reader.ReadRecord(&record, &scratch)) {
      if (record.size() < 12) {
        reporter.Corruption(record.size(), Status::Corruption("log record too small"));
        continue;
      }
      WriteBatchInternal::SetContents(&batch, record);
      s = WriteBatchInternal::InsertInto(&batch, mem);
      if (s.ok()) {
        counter += WriteBatchInternal::Count(&batch);
      } else {
        Log(options_.info_log, "Log #%llu: ignoring %s",
            log, s.ToString().c_str());
        s =  Status::OK();  // Keep going with rest of file
      }
    }
    delete lfile;

    // Do not record a version edit for this conversion to a Table
    // since ExtractMetaData() will also generate edits
    FileMetaData meta;
    meta.number = next_file_number_++;
    Iterator *iter = mem->NewIterator();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    delete iter;
    mem->Unref();
    mem = nullptr;
    if (s.ok()) {
      if (meta.file_size > 0) {
        table_numbers_.push_back(meta.number);
      }
    }
    Log(options_.info_log, "Log #%llu: %d ops saved to Table #%llu %s",
        log, counter, meta.number, s.ToString().c_str());
    return s;
  }


  void ExtractMetaData() {
    for (uint64_t table_number : table_numbers_) {
      ScanTable(table_number);
    }
  }

  Iterator* NewTableIterator(const FileMetaData &meta) {
    ReadOptions r;
    r.verify_checksums = options_.paranoid_check;
    return table_cache_->NewIterator(r, meta.number, meta.file_size);
  }

  void ScanTable(uint64_t number) {
    TableInfo t;
    t.meta.number = number;
    std::string fname = TableFileName(dbname_, number);
    Status s = env_->GetFileSize(fname, &t.meta.file_size);
    if (!s.ok()) {
      // Try alternate file name.
      fname = SSTTableFileName(dbname_, number);
      Status s2 = env_->GetFileSize(fname, &t.meta.file_size);
      if (s2.ok()) {
        s = Status::OK();
      }
    }
    if (!s.ok()) {
      ArchiveFile(TableFileName(dbname_, number));
      ArchiveFile(SSTTableFileName(dbname_, number));
      Log(options_.info_log, "Table #%llu: dropped: %s",
          t.meta.number, s.ToString().c_str());
      return;
    }

    // Extract metadata by scanning through table.
    int counter = 0;
    Iterator *iter = NewTableIterator(t.meta);
    bool empty = true;
    ParsedInternalKey parsed;
    t.max_sequence = 0;
    Slice largest_key;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      if (!ParseInternalKey(key, &parsed)) {
        Log(options_.info_log, "Table #%llu: unparsable key %s",
            t.meta.number, EscapeString(key).c_str());
        continue;
      }

      counter++;
      if (empty) {
        empty = false;
        t.meta.smallest.DecodeFrom(key);
      }
      largest_key = key;
      if (parsed.sequence > t.max_sequence) {
        t.max_sequence = parsed.sequence;
      }
    }
    t.meta.largest.DecodeFrom(largest_key);

    if (!iter->status().ok()) {
      s = iter->status();
    }
    delete iter;
    Log(options_.info_log, "Table #%llu: %d entries %s",
        t.meta.number, counter, s.ToString().c_str());
    if (s.ok()) {
      tables_.push_back(t);
    } else {
      RepairTable(fname, t);  // RepairTable archives input file.
    }
  }

  void RepairTable(const std::string &src, TableInfo t) {
    // We will copy src contents to a new table and then rename the
    // new table over the source.

    // Create builder.
    std::string copy = TableFileName(dbname_, next_file_number_++);
    WritableFile *file;
    Status s = env_->NewWritableFile(copy, &file);
    if (!s.ok()) {
      return;
    }
    TableBuilder *builder = new TableBuilder(options_, file);

    // Copy data.
    Iterator *iter = NewTableIterator(t.meta);
    int counter = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      builder->Add(iter->key(), iter->value());
      counter++;
    }
    delete iter;

    ArchiveFile(src);
    if (counter == 0) {
      builder->Abandon(); // Nothing to save
    } else {
      s = builder->Finish();
      if (s.ok()) {
        t.meta.file_size = builder->FileSize();
      }
    }
    delete builder;
    file = nullptr;

    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (counter > 0 && s.ok()) {
      std::string orig = TableFileName(dbname_, t.meta.number);
      s = env_->RenameFile(copy, orig);
      if (s.ok()) {
        Log(options_.info_log, "Table #%llu: %d entries repaired",
            t.meta.number, counter);

      }
    }
    if (!s.ok()) {
      env_->RemoveFile(copy);
    }
  }

  Status WriteDescriptor() {
    std::string tmp = TempFileName(dbname_, 1);
    WritableFile *file;
    Status s = env_->NewWritableFile(tmp, &file);
    if (!s.ok()) {
      return s;
    }

    SequenceNumber max_sequence = 0;
    for (const TableInfo &table : tables_) {
      if (max_sequence < table.max_sequence) {
        max_sequence = table.max_sequence;
      }
    }
    edit_.SetComparatorName(icmp_.user_comparator()->Name());
    edit_.SetLogNumber(0);
    edit_.SetNextFile(next_file_number_);
    edit_.SetLastSequence(max_sequence);

    for (const TableInfo &table : tables_) {
      edit_.AddFile(0, table.meta.number, table.meta.file_size,
            table.meta.smallest, table.meta.largest);
    }

    {
      log::Writer log(file);
      std::string record;
      edit_.EncodeTo(&record);
      s = log.AddRecord(record);
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;
    if (!s.ok()) {
      env_->RemoveFile(tmp);
    } else {
      // Discard older manifests
      for (const std::string &manifest : manifests_) {
        ArchiveFile(dbname_ + "/" + manifest);
      }

      // Install new manifest
      s = env_->RenameFile(tmp, DescriptorFileName(dbname_, 1));
      if (s.ok()) {
        s = SetCurrentFile(env_, dbname_, 1);
      } else {
        env_->RemoveFile(tmp);
      }
    }
    return s;
  }

  void ArchiveFile(const std::string &fname) {
    // Move into another directory. E.g., for
    //  dir/foo
    // rename to
    // dir/lost/foo
    const char *slash = strrchr(fname.c_str(), '/');
    std::string new_dir;
    if (slash != nullptr) {
      new_dir.assign(fname.data(), slash - fname.data());
    }
    new_dir.append("/lost");
    env_->CreateDir(new_dir);   // Ignore error
    std::string new_file = new_dir;
    new_file.append("/");
    new_file.append(slash == nullptr ? fname.c_str() : slash + 1);
    Status s = env_->RenameFile(fname, new_file);
    Log(options_.info_log, "Archiving %s: %s\n", fname.c_str(), s.ToString().c_str());

  }

  const std::string dbname_;
  Env *const env_;
  InternalKeyComparator const icmp_;
  InternalFilterPolicy const ipolicy_;
  const Options options_;
  bool owns_info_log_;
  bool owns_cache_;
  TableCache *table_cache_;
  VersionEdit edit_;

  std::vector<std::string> manifests_;
  std::vector<uint64_t> table_numbers_;
  std::vector<uint64_t> logs_;
  std::vector<TableInfo> tables_;
  uint64_t next_file_number_;
};

} // namespace 

} // namespace my_leveldb