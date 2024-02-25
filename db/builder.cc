

#include "db/builder.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "spdlog/spdlog.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "db/filename.h"

namespace my_leveldb {

Status BuildTable(const std::string dbname, Env *env, const Options &options,
                  TableCache *table_cache, Iterator *iter, FileMetaData *meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile *file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder *builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    for(; iter->Valid(); iter->Next()) {
      // spdlog::info("write key({})--value({}) to sstable({})", iter->key().ToString(), iter->value().ToString(), fname);
      key = iter->key();
      builder->Add(key, iter->value());
    }

    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator *it = table_cache->NewIterator(ReadOptions(), meta->number, meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

} // namespace my_leveldb