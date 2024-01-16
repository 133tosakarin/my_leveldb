
#pragma once


#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "util/hash.h"
#include "leveldb/status.h"

namespace my_leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as 
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must matchthe regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);
  REMOVE_COPY_CONSTRUCTOR(FilterBlockBuilder);

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice &key);
  Slice Finish();

 private:
  void GenerateFilter();
  
  const FilterPolicy *policy_;
  std::string keys_;            // Flattened key contents
  std::vector<size_t> start_;   // Starting index in keys_ of each key
  std::string result_;          // Filter data computed so far
  std::vector<Slice> tmp_keys_; // policy_->CreateFilter() argument
  std::vector<uint32_t> filter_offsets_;
};

class FilterBlockReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live
  FilterBlockReader(const FilterPolicy* policy, const Slice &contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice &key);




 private:
  const FilterPolicy *policy_;
  const char *data_;    // Pointer to filter data (at block-start)
  const char *offset_;  // Pointer to begining of offset array (at block-end)
  size_t num_;          // Number of entries in offset array
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};
}