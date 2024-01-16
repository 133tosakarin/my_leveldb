
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

/**
 * We want to make sure that with a single writer and multiple 
 * concurrent readers (with no synchronization other than when a 
 * reader's iterator is created), the reader always observes all the
 * data that was present in the skip list when the iterator was 
 * constructed. Because insertions are happening concurrently, we may 
 * also observe new values that were inserted since the iterator was
 * constructed, but we should never miss any values that were present
 * at iterator construction time.
 * 
 * We gernate multi-part keys:
 *        <key, gen, hash>
 * where:
 *      key is in range[0..K-1]
 *      gen is a generation number for key
 *      hash is hash(key, gen)
 * 
 * The insertion code picks a random key, sets gen to be 1 + the last
 * generation number inserted for that key, and sets hash to Hash(key, gen)
 * 
 * At the begining of a read, we snapshot the last inserted
 * generation number for each key. We then iterate, including random
 * calls to Next() and Seek(). For every key we encounter, we
 * check that it is either expected given the initial snaphot or has
 * been concurrently added since the iterator started.
*/
class ConcurrentTest {
 private:
  static constexpr uint32_t K = 4;
};
}