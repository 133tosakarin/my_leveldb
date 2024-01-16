
#pragma once


#include <cstdint>
#include <string>

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/table_builder.h"

namespace my_leveldb {

class Block;
class RandomAccessFile;
struct ReadOptions;

// BlockHandle is a pointer to the extent of a file that stores a data\
// block or a meta block
class BlockHandle {
 public:
  // Maximum encoding length of a BlockHandle
  enum { kMaxEncodedLength = 10 + 10} ;

  BlockHandle();

  // The offset of the block in the file
  auto offset() const -> uint64_t { return offset_; }

  void set_offset(uint64_t offset) { offset_ = offset; }

  uint64_t size() const { return size_; }
  void set_size(uint64_t size) { size_ = size; }

  void EncodeTo(std::string *dst) const;
  auto DecodeFrom(Slice *input) -> Status;


 private:
  uint64_t offset_;
  uint64_t size_;
};

// Footer encapsulates the fixed information stored at the tail 
// end of every table file.
class Footer {
 public:
  // Encoded length of a Footer.  Note that the serialization of a
  // Footer will always occupy exactly this many bytes. It consists
  // of two block handles and a magic number.
  enum { kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8 };

  Footer() = default;

  // The block handle for the metaindex block of the table
  const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
  void set_metaindex_handle(const BlockHandle &h) { metaindex_handle_ = h; }

  // The block handle for the index block of the table
  const BlockHandle& index_handle() const { return index_handle_; }
  void set_index_handle(const BlockHandle &h) { index_handle_ = h; }

  void EncodeTo(std::string *dst) const;
  Status DecodeFrom(Slice *input);



 private:
  BlockHandle metaindex_handle_;
  BlockHandle index_handle_;
};

// magic number
static constexpr uint64_t kTableMagicNumber =  0xdb4775248b80fb57ull;

// 1-byte type + 32-bit crc
static constexpr size_t kBlockTrailerSize = 5;

struct BlockContents {
  Slice data;          // Actual contents of data
  bool cachable;       // True iff data can be cached
  bool heap_allocated; // True iff caller should delete[] data.data()
};


// Read the block identified by "handle" from "file". On failure
// return non-OK. On success fill *result and return OK.
Status ReadBlock(RandomAccessFile *file, const ReadOptions &options,
                  const BlockHandle &handle, BlockContents *result);

// Implementations details follow. Clients should ignore.

inline BlockHandle::BlockHandle()
    : offset_(~static_cast<uint64_t>(0)), size_(~static_cast<uint64_t>(0)) {}
} // namespace leveldb