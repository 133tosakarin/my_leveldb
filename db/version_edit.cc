
#include "db/version_edit.h"

#include "util/coding.h"

namespace my_leveldb {

// Tag numbers for serialized VersionEdit.  These numbers are
// written to disk and should not be changed.
enum Tag {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeleteFile = 6,
  kNewFile = 7,
  // 8 was used for large value refs
  kPrevLogNumber = 9
};

void VersionEdit::Clear() {
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  next_file_number_ = 0;
  last_sequence_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  compact_pointers_.clear();
  deleted_files_.clear();
  new_files_.clear();
}

void VersionEdit::EncodeTo(std::string *dst) const {
  if (has_comparator_) {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32(dst, kLogNumber);
    PutVarint64(dst, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32(dst, kPrevLogNumber);
    PutVarint64(dst, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {
    PutVarint32(dst, kLastSequence);
    PutVarint64(dst, last_sequence_);
  }

  for (size_t i = 0; i < compact_pointers_.size(); ++i) {
    PutVarint32(dst, kCompactPointer);
    PutVarint32(dst, compact_pointers_[i].first); // level
    PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode());
  }

  for (const auto &[level, filenumber] : deleted_files_) {
    // LOG_INFO("encode delete_file[%d, %lld] to dst %s", level, filenumber, dst->c_str());
    PutVarint32(dst, kDeleteFile);
    PutVarint32(dst, level); // level
    PutVarint64(dst, filenumber); // file number
  }

  for (size_t i = 0; i < new_files_.size(); ++i) {
    const FileMetaData &f = new_files_[i].second;
    // LOG_INFO("encode new file #level-%d---number-%d---size-%lld", 
    //         new_files_[i].first, f.number, f.file_size);
    PutVarint32(dst, kNewFile);
    PutVarint32(dst, new_files_[i].first);  // level
    PutVarint64(dst, f.number);
    PutVarint64(dst, f.file_size);
    PutLengthPrefixedSlice(dst, f.smallest.Encode());
    PutLengthPrefixedSlice(dst, f.largest.Encode());
  }
}

static bool GetInternalKey(Slice *input, InternalKey *dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    return dst->DecodeFrom(str);
  } else {
    return false;
  }
}

static bool GetLevel(Slice *input, int *level) {
  uint32_t v;
  if (GetVarint32(input, &v) && v < config::kNumLevels) {
    *level = v;
    return true;
  }
  return false;
}

Status VersionEdit::DecodeFrom(const Slice &src) {
  Clear();
  Slice input = src;
  const char *msg = nullptr;
  uint32_t tag;

  // Temporary storage for parsing
  int level;
  uint64_t number;
  FileMetaData f;
  Slice str;
  InternalKey key;

  while (msg == nullptr && GetVarint32(&input, &tag)) {
    switch (tag) {
      case Tag::kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;
      case Tag::kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;
      case Tag::kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg ="next file number";
        }
        break;
      case Tag::kCompactPointer:
        if (GetLevel(&input, &level) && GetInternalKey(&input, &key)) {
          compact_pointers_.emplace_back(level, key);
        } else {
          msg = "compaction pointer";
        }
        break;
      case Tag::kDeleteFile:
        if (GetLevel(&input, &level) && GetVarint64(&input, &number)) {
          deleted_files_.emplace(level, number);
        } else {
          msg = "deleted file";
        }
        break;
      case Tag::kNewFile:
        if (GetLevel(&input, &level)
            && GetVarint64(&input, &f.number)
            && GetVarint64(&input, &f.file_size)
            && GetInternalKey(&input, &f.smallest)
            && GetInternalKey(&input, &f.largest)) {
          new_files_.emplace_back(level, f);
        } else {
          msg = "new-file entry";
        }
        break;
      case Tag::kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;
      case Tag::kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "prev log number";
        }
        break;
      default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == nullptr && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != nullptr) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString() const {
  std::string r;
  r.append("VersionEdit {");
  if (has_comparator_) {
    r.append("\n Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n NextFile: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_) {
    r.append("\n LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  

  for (size_t i = 0; i < compact_pointers_.size(); ++i) {
    r.append("\n CompactPointer: ");
    AppendNumberTo(&r, compact_pointers_[i].first);
    r.append(" ");
    r.append(compact_pointers_[i].second.DebugString());
  }

  for (const auto &deleted_files_kvp : deleted_files_) {
    r.append("\n RemoveFile: ");
    AppendNumberTo(&r, deleted_files_kvp.first);
    r.append(" ");
    AppendNumberTo(&r, deleted_files_kvp.second);
  }

  for (size_t i = 0; i < new_files_.size(); ++i) {
    const FileMetaData &f = new_files_[i].second;
    r.append("\n AddFile: ");
    AppendNumberTo(&r, new_files_[i].first);
    r.append(" ");
    AppendNumberTo(&r, f.number);
    r.append(" ");
    AppendNumberTo(&r, f.file_size);
    r.append(" ");
    r.append(" ");
    r.append(f.smallest.DebugString());
    r.append(" .. ");
    r.append(f.largest.DebugString());
  }
  r.append("\n}\n");
  return r;
}

} // namespace my_leveldb