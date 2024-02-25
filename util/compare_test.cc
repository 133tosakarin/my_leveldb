

#include <vector>
#include <algorithm>

#include "leveldb/comparator.h"
#include "db/dbformat.h"
#include "gtest/gtest.h"

namespace my_leveldb {

TEST(ComparatorTest, SortTest) {
  InternalKeyComparator icmp(ByteWiseComparator());
  std::vector<InternalKey> keys;
  keys.emplace_back("100", 1, kTypeValue);
  keys.emplace_back("100", 2, kTypeValue);
  keys.emplace_back("100", 3, kTypeValue);
  keys.emplace_back("100", 4, kTypeValue);
  keys.emplace_back("200", 1, kTypeValue);
  keys.emplace_back("200", 2, kTypeValue);
  keys.emplace_back("200", 3, kTypeValue);
  keys.emplace_back("200", 3, kTypeValue);
  keys.emplace_back("200", 4, kTypeValue);

  std::sort(keys.begin(), keys.end(), [&icmp](const InternalKey &a, const InternalKey &b) {
    return icmp.Compare(a, b) < 0;
  });

  for (const auto &key : keys) {
    std::fprintf(stdout, "%s\n", key.DebugString().c_str());
  }

}


}

