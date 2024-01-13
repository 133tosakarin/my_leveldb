
#include "db/skiplist.h"

#include <atomic>
#include <set>

#include "gtest/gtest.h"

#include "util/arena.h"
#include "util/random.h"
#include "spdlog/spdlog.h"

namespace my_leveldb {
using Key = uint64_t;
struct Comparator {
  int operator()(const Key &a, const Key &b) const {
    if (a > b) {
      return 1;
    }
    else if (a == b) {
      return 0;
    }
    else {
      return -1;
    }
  }
};

TEST(SkipTest, Empty) {
  Arena arena;
  Comparator cmp;
  SkipList<Key, Comparator> list(cmp, &arena);

  ASSERT_TRUE(!list.Contains(10));

  SkipList<Key, Comparator>::Iterator iter(&list);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToFirst();
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToLast();
  ASSERT_TRUE(!iter.Valid());
  iter.Seek(1000);
  ASSERT_TRUE(!iter.Valid());

}

TEST(SkipTest, InsertAndLookup) {
  const int N = 1000;
  const int R = 5000;
  Random rnd(1000);
  std::set<Key> keys;
  Arena arena;
  Comparator cmp;
  SkipList<Key, Comparator> list(cmp, &arena);
  for (int i = 0; i < N; ++i) {
    Key key = rnd.Next() % R;
    if (keys.insert(key).second) {
      list.Insert(key);
      spdlog::info("insert key: {}", key);
    }
  }
  {
    SkipList<Key, Comparator>::Iterator iter(&list);
    spdlog::info("smallest: {}, largest: {}", *keys.begin(), *keys.rbegin());
    iter.SeekToFirst();
    spdlog::info("smallest: {}, largest: {}", iter.key(), (iter.SeekToLast(), iter.key()));
  }

  for (int i = 0; i < R; ++i) {
    if (list.Contains(i)) {
      spdlog::info("equal val: {}", i);
      ASSERT_EQ(keys.count(i), 1);
    } else {
      ASSERT_EQ(keys.count(i), 0);
    }
  }
  {
    SkipList<Key, Comparator>::Iterator iter(&list);
    ASSERT_TRUE(!iter.Valid());
    iter.Seek(0);
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), iter.key());

    iter.SeekToLast();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.rbegin()), iter.key());
  }
  // Forward iteration test
  for (int i = 0; i < R; ++i) {
    SkipList<Key, Comparator>::Iterator iter(&list);
    iter.Seek(i);

    // Compare against model iterator
    std::set<Key>::iterator model_iter = keys.lower_bound(i);
    for (int j = 0; j < 3; ++j) {
      if (model_iter == keys.end()) {
        ASSERT_TRUE(!iter.Valid());
        break;
      } else {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*model_iter, iter.key());
        ++model_iter;
        iter.Next();
      }
    }
  }

  // Backward iteration test
  {
    SkipList<Key, Comparator>::Iterator iter(&list);
    iter.SeekToLast();

    // Compare against model iterator
    for (std::set<Key>::reverse_iterator model_iter = keys.rbegin(); 
          model_iter != keys.rend(); ++model_iter) {
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(*model_iter, iter.key());
      iter.Prev();
    }

    ASSERT_TRUE(!iter.Valid());
  }
}
}