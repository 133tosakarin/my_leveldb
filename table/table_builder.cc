

#include "leveldb/table_builder.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "spdlog/spdlog.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "spdlog/spdlog.h"
#include "util/coding.h"
#include "leveldb/filter_policy.h"
namespace my_leveldb {

struct TableBuilder::Rep {
  Rep(const Options &opt, WritableFile *f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr 
                        ? nullptr 
                        : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile *file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;    // Either Finish of Abandon() has been called.
  FilterBlockBuilder *filter_block;

  /**
   * We do not emit the index entry for a block until we have seen the
   * first key for the next data block.  This allows us to use shorter
   * keys in the index block.  For example, consider a block boundary
   * between the keys "the quick brown fox" and "the who". We van use
   * "the r" as the key for the index block entry since it is >= all entries it >= all
   * entries in the first block and < all entries in subsequent
   * blocks.
   * 
   * Invariant: r->pending_index_entry is true only if data_block is empty.
  */
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options &option, WritableFile *file)
    : rep_(new Rep(option, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed); // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

auto TableBuilder::ChangeOptions(const Options &options) -> Status {

  // Note: if more fields are addred to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a table.
  if (rep_->options.comparator != options.comparator) {
    return Status::InvalidArgument("changing comparator while builing table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice &key, const Slice &value) {
  Rep *r = rep_;
  assert(!r->closed);
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  if (r->filter_block != nullptr) {
    // spdlog::info("add key({})---value({}) to filter_block", key.ToString(), key.size());
    r->filter_block->AddKey(key);
  }
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);
    // spdlog::trace(fmt::format("Add key: {}, value: {}",
    //             key.ToString(), value.ToString()));

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    // spdlog::info(fmt::format("estimated_block_size >= r-options.block_size"
    //                            "with {} , then call Flush()", estimated_block_size));
    Flush();
  }

}

void TableBuilder::Flush() {
  // spdlog::trace(fmt::format("flush Table to File"));
  Rep *r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);

  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }

  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
  
  // spdlog::info(fmt::format("flush finish status.message: {}", status().ToString()));
}

void TableBuilder::WriteBlock(BlockBuilder *block, BlockHandle *handle) {
  // File format contains a sequence of blocks where each block has:
  // block_data: uint8[n]
  // type: uint8
  // crc: uint32
  assert(ok());
  Rep *r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;
    case kSnappyCompression: {
      std::string *compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
    case  kZstdCompression: {
      std::string *compressed = &r->compressed_output;
      if (port::Zstd_Compress(r->options.zstd_compression_level, raw.data(),
                            raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {

        // Zstd not supported, or compressed Less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    } 
  }

  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice &block_contents,
                                CompressionType type, BlockHandle *handle) {
  Rep *r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1); // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep *r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    Slice raw = r->filter_block->Finish();
    // spdlog::info("write filter_block ({}), size: {}", raw.ToString(), raw.size());
    WriteRawBlock(raw, kNoCompression,
                   &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(posttreLease): Add stats and other meta blocks
    
    WriteBlock(&meta_index_block, &metaindex_block_handle);

  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
    // spdlog::trace(fmt::format("Write index block with offset: {}, size: {}",
    //            index_block_handle.offset(), index_block_handle.size()));
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep *r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }
} // namespace my_leveldb