
#include "leveldb/table.h"
#include "leveldb/options.h"
#include "spdlog/spdlog.h"
#include "leveldb/env.h"
#include "table/filter_block.h"
#include "table/block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "leveldb/filter_policy.h"
#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "table/two_level_iterator.h"
namespace my_leveldb {

struct Table::Rep {

  ~Rep() {
    delete filter;
    delete [] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile *file;
  uint64_t cache_id;
  FilterBlockReader *filter;
  const char *filter_data;

  BlockHandle metaindex_handle;   // Handle to metaindex_block: saved from footer
  Block *index_block;
};

Status Table::Open(const Options &options, RandomAccessFile *file,
                    uint64_t size, Table **table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) {
    return s;
  }

  Footer footer;
  s = footer.DecodeFrom(&footer_input);

  // Read the  index block
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_check) {
    opt.verify_checksums = true;
  }
  /**
   * each key-value pair in index_block is point to a data block
   * key is larger than or equal maximum key-value in data block and  less than key of next data block minimum by specifec comparator,
   * which implement by FindShortSeparator in order to reduce index block volume
   * value is blockHandle of data block, at last index block as possible as in cache
  */
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    Block *index_block = new Block(index_block_contents);
    Rep *rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer); // each key-value pair point to a meta block, at present, leveldb is just a meta block,
                                // it reserves bloom filter consist by keys in sstable 
  }

  return s;
}

void Table::ReadMeta(const Footer &footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;   // Do not need any metadata
  }

  // TODO(): skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_check) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errrors since meta info is not needed for operation
    return;
  }
  Block *meta = new Block(contents);

  Iterator *iter = meta->NewIterator(ByteWiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    // if metaindex block is not null, read bloom Filter(default) to sstable
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice &filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start 
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_check) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data(); // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void *arg, void *ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCacheBlock(const Slice &key, void *value) {
  Block *block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void *arg, void *h) {
  Cache *cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle *handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert on index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void *arg, const ReadOptions &options,
                            const Slice &index_value) {
  Table *table = reinterpret_cast<Table*>(arg);
  Cache *block_cache = table->rep_->options.block_cache;
  Block *block = nullptr;
  Cache::Handle *cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);

  // We intentionally allow extra stuff in index_value so that
  // we can add more features in future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed32(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block *>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                                        &DeleteCacheBlock);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator *iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions &options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
  
}

Status Table::InternalGet(const ReadOptions &options, const Slice &k, void *arg,
                          void (*handle_result)(void *, const Slice&, const Slice&)) {
  Status s;
  Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader *filter = rep_->filter;
    BlockHandle handle;
    // spdlog::info("key: {}, key.size(): {}", k.ToString(), k.size());
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
      // spdlog::info("not found key: ({} with size: {}), offset: {}", k.ToString(), k.size(), handle.offset());
    } else {
      Iterator *block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }

  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice &key) const {
  Iterator *index_iter = 
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}
}