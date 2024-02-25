
#include <string>
#include <algorithm>
#include <map>

#include "gtest/gtest.h"
#include "db/write_batch_internal.h"
#include "leveldb/slice.h"
#include "spdlog/spdlog.h"
#include "db/memtable.h"
#include "leveldb/write_batch.h"
#include "leveldb/comparator.h"
#include "leveldb/table.h"
#include "leveldb/env.h"
#include "leveldb/options.h"
#include "db/db_impl.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "util/testutil.h"


namespace my_leveldb {

// Return reverse of "key".
// Used to test non-lexicographic comparators.
static std::string Reverse(const Slice &key) {
  std::string str(key.ToString());
  std::reverse(str.begin(), str.end());
  return str; 
}

namespace {

class HelperSpdlog {
 public:
  HelperSpdlog() {
   spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] (%s:%#)  [thread %t] %v  ");
   // spdlog::error("global variance");
  }
};

HelperSpdlog g_helper;
class ReverseKeyComparator : public Comparator {
 public:
  const char *Name() const override {
    return "leveldb.ReverseBytesiseComparator";
  }

  int Compare(const Slice &a, const Slice &b) const override {
    return ByteWiseComparator()->Compare(Reverse(a), Reverse(b));
  }

  void FindShortestSeparator(std::string *start,
                             const Slice &limit) const override {
    std::string s = Reverse(*start);
    std::string l = Reverse(limit);
    ByteWiseComparator()->FindShortestSeparator(&s, l);
    *start = Reverse(s);
  }

  void FindShortSuccessor(std::string *key) const override {
    std::string s = Reverse(*key);
    ByteWiseComparator()->FindShortSuccessor(&s);
    *key = Reverse(s);
  }
};

} // namespace

static ReverseKeyComparator reverse_key_comparator;

static void Increment(const Comparator *cmp, std::string *key) {
  if (cmp == ByteWiseComparator()) {
    key->push_back('\0');
  } else {
    assert(cmp == &reverse_key_comparator);
    std::string rev = Reverse(*key);
    rev.push_back('\0');
    *key = Reverse(rev);
  }
}

// An STl comparator that uses a Comparator
namespace {

struct STLLessThan {
  const Comparator *cmp;
  STLLessThan() : cmp(ByteWiseComparator()) {}
  STLLessThan(const Comparator *c) : cmp(c) {}
  bool operator()(const std::string &a, const std::string &b) const {
    return cmp->Compare(Slice(a), Slice(b)) < 0;
  }
};
} // namespace

class StringSink : public WritableFile {
 public:
  ~StringSink() override = default;

  const std::string& contents() const { return contents_; }

  Status Close() override { return Status::OK(); }
  Status Flush() override { return Status::OK(); }
  Status Sync() override { return Status::OK(); }

  Status Append(const Slice &data) override {
    contents_.append(data.data(), data.size());
    return Status::OK();
  }

 private:

  std::string contents_;
};

class StringSource : public RandomAccessFile {
 public:
  StringSource(const Slice &contents)
    : contents_(contents.data(), contents.size()) {}
  
  ~StringSource() override = default;

  uint64_t Size() const { return contents_.size(); }

  Status Read(uint64_t offset, size_t n, Slice *result,
              char *scratch) const override {
    if (offset >= contents_.size()) {
      return Status::InvalidArgument("invalid Read offset");
    }
    if (offset + n > Size()) {
      n = contents_.size() - offset;
    }
    std::memcpy(scratch, &contents_[offset], n);
    *result = Slice(scratch, n);
    return Status::OK();
  }
 private:
  std::string contents_;
};

using KVMap = std::map<std::string, std::string, STLLessThan>;

// Helper class for tests to unify the interface between
// BlockBuilder/TableBuilder and Block/Table.
class Constructor {
 public:
  explicit Constructor(const Comparator *cmp) : data_(STLLessThan(cmp)) {}
  virtual ~Constructor() = default;

  void Add(const std::string &key, const Slice &value) {
    data_[key] = value.ToString();
  }

  // Finish constructing the data structure with all the keys that
  // have been added so far.  Returns the keys in sorted  order in "*keys"
  // and stores the key/value pairs in "*kvmap"
  void Finish(const Options &options, std::vector<std::string> *keys,
              KVMap *kvmap) {
    *kvmap = data_;
    keys->clear();
    for (const auto &kvp : data_) {
      keys->push_back(kvp.first);
    }
    data_.clear();
    Status s = FinishImpl(options, *kvmap);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  // Construct the data structure from the data in "data"
  virtual Status FinishImpl(const Options &options, const KVMap &data) = 0;

  virtual Iterator* NewIterator() const = 0;

  const KVMap& data() const { return data_; }

  virtual DB* db() const { return nullptr; }  // Overridden in DBConstructor
 private:
  KVMap data_;
};

class BlockConstructor : public Constructor {
 public:
  explicit BlockConstructor(const Comparator *cmp)
      : Constructor(cmp), comparator_(cmp), block_(nullptr) {}
  ~BlockConstructor() override { delete block_; }
  Status FinishImpl(const Options &options, const KVMap &data) override {
    delete block_;
    block_ = nullptr;
    BlockBuilder builder(&options);

    for (const auto &[k, v] : data) {
      builder.Add(k, v);
    }
    // Open the block
    data_ = builder.Finish().ToString();
    BlockContents contents;
    contents.data = data_;
    contents.cachable = false;
    contents.heap_allocated = false;
    block_ = new Block(contents);
    return Status::OK();
  }

  Iterator* NewIterator() const override {
    return block_->NewIterator(comparator_);
  }
 private:
  const Comparator *const comparator_;
  std::string data_;
  Block *block_;
  BlockConstructor();
};

class TableConstructor : public Constructor {
 public:
  TableConstructor(const Comparator *cmp) 
      : Constructor(cmp),
        source_(nullptr),
        table_(nullptr) {}
  ~TableConstructor() override { Reset(); }

  Status FinishImpl(const Options &options, const KVMap &data) override {
    Reset();
    StringSink sink;
    TableBuilder builder(options, &sink);

    for (const auto &[k, v] : data) {
      builder.Add(k, v);
      EXPECT_LEVELDB_OK(builder.status());
    }
    Status s = builder.Finish();
    EXPECT_LEVELDB_OK(s);
    EXPECT_EQ(sink.contents().size(), builder.FileSize());

    // Open the table
    source_ = new StringSource(sink.contents());
    Options table_options;
    table_options.comparator = options.comparator;
    return Table::Open(table_options, source_, sink.contents().size(), &table_);
  }

  Iterator* NewIterator() const override {
    return table_->NewIterator(ReadOptions());
  }

  uint64_t ApproximateOffsetOf(const Slice &key) const {
    return table_->ApproximateOffsetOf(key);
  }
 private:
  void Reset() {
    delete table_;
    delete source_;
    table_ = nullptr;
    source_ = nullptr;
  }

  StringSource *source_;
  Table *table_;

  TableConstructor();
};

// A helper class that converts internal format keys into user keys
class KeyConvertingIterator : public Iterator {
 public:
  explicit KeyConvertingIterator(Iterator *iter) : iter_(iter) {}

  REMOVE_COPY_CONSTRUCTOR(KeyConvertingIterator);

  ~KeyConvertingIterator() override { delete iter_; }

  bool Valid() const override { return iter_->Valid(); }
  void Seek(const Slice &target) override {
    ParsedInternalKey ikey(target, kMaxSequenceNumber, kTypeValue);
    std::string encoded;
    AppendInteralKey(&encoded, ikey);
    iter_->Seek(encoded);
  }

  void SeekToFirst() override { iter_->SeekToFirst(); }
  void SeekToLast() override { iter_->SeekToLast(); }

  void Next() override { iter_->Next(); }
  void Prev() override { iter_->Prev(); }

  Slice key() const override {
    assert(Valid());
    ParsedInternalKey key;
    if (!ParseInternalKey(iter_->key(), &key)) {
      status_ = Status::Corruption("malformed internal key");
      return Slice("corrupted key");
    }
    return key.user_key;
  }

  Slice value() const override { return iter_->value(); }
  Status status() const override {
    return status_.ok() ? iter_->status() : status_;
  }
 private:
  mutable Status status_;
  Iterator *iter_;
};

class MemTableConstructor : public Constructor {
 public:
  explicit MemTableConstructor(const Comparator *cmp)
      : Constructor(cmp), 
        internal_comparator_(cmp){
    memtable_ = new MemTable(internal_comparator_);
    memtable_->ref();
  }

  ~MemTableConstructor() override { memtable_->Unref(); }
  Status FinishImpl(const Options &options, const KVMap &data) override {
    memtable_->Unref();
    memtable_ = new MemTable(internal_comparator_);
    memtable_->ref();
    int seq = 1;
    for (const auto &[k, v] : data) {
      memtable_->Add(seq, kTypeValue, k, v);
      seq++;
    }
    return Status::OK();
  }

  Iterator* NewIterator() const override {
    return new KeyConvertingIterator(memtable_->NewIterator());
  }
 private:
  const InternalKeyComparator internal_comparator_;
  MemTable *memtable_;
};

class DBConstructor : public Constructor {
 public:
  explicit DBConstructor(const Comparator *cmp)
      : Constructor(cmp), 
        comparator_(cmp) {
    db_ = nullptr;
    NewDB();
  }

  ~DBConstructor() override { delete db_; }

  Status FinishImpl(const Options &options, const KVMap &data) override {
    delete db_;
    db_ = nullptr;
    NewDB();
    for (const auto &[k, v] : data) {
      WriteBatch batch;
      batch.Put(k, v);
      EXPECT_TRUE(db_->Write(WriteOptions(), &batch).ok());
    }
    return Status::OK();
  }

  Iterator* NewIterator() const override {
    return db_->NewIterator(ReadOptions());
  }

  DB* db() const override { return db_; }

 private:
  void NewDB() {
    std::string name = testing::TempDir() + "table_testdb";

    Options options;
    options.comparator = comparator_;
    Status s = DestroyDB(name, options);
    ASSERT_TRUE(s.ok()) << s.ToString();

    options.create_if_missing = true;
    options.error_if_exists = true;
    options.write_buffer_size = 10000;    // Something small to force merging
    s = DB::Open(options, name, &db_);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }
  const Comparator *const comparator_;
  DB *db_;
};

enum TestType { TABLE_TEST, BLOCK_TEST, MEMTABLE_TEST, DB_TEST };

struct TestArgs {
  TestType type;
  bool reverse_compare;
  int restart_internal;
};

static constexpr TestArgs KTestArgList[] = {
  {TABLE_TEST, false, 16},
  {TABLE_TEST, false, 1},
  {TABLE_TEST, false, 1024},
  {TABLE_TEST, true, 16},
  {TABLE_TEST, true, 1},
  {TABLE_TEST, true, 1024},

  {BLOCK_TEST, false, 16},
  {BLOCK_TEST, false, 1},
  {BLOCK_TEST, false, 1024},
  {BLOCK_TEST, true, 16},
  {BLOCK_TEST, true, 1},
  {BLOCK_TEST, true, 1024},

  // Restart internal does not matter for memtables
  {MEMTABLE_TEST, false, 16},
  {MEMTABLE_TEST, true, 16},

  // Do not bother with restart interval variations for DB
  {DB_TEST, false, 16},
  {DB_TEST, true, 16},
};

static constexpr int kNumTestArgs = sizeof(KTestArgList) / sizeof(KTestArgList[0]);

class Harness : public testing::Test {
 public:
  Harness() : constructor_(nullptr) {}

  void Init(const TestArgs &args) {
    delete constructor_;
    constructor_ = nullptr;
    options_ = Options();

    options_.block_restart_interval = args.restart_internal;
    // Use shorter block size for tests to exercies block boundary
    // conditions more
    options_.block_size = 256;
    if (args.reverse_compare) {
      options_.comparator = &reverse_key_comparator;
    }
    switch (args.type) {
      case TABLE_TEST:
        constructor_ = new TableConstructor(options_.comparator);
        break;
      case BLOCK_TEST:
        constructor_ = new BlockConstructor(options_.comparator);
        break;
      case MEMTABLE_TEST:
        constructor_ = new MemTableConstructor(options_.comparator);
        break;
      case DB_TEST:
        constructor_ = new DBConstructor(options_.comparator);
        break;
    }
  }

  ~Harness() { delete constructor_; }

  void Add(const std::string &key, const std::string &value) {
    constructor_->Add(key, value);
  }

  void Test(Random *rnd) {
    std::vector<std::string> keys;
    KVMap data;
    constructor_->Finish(options_, &keys, &data);

    TestForwardScan(keys, data);
    TestBackwardScan(keys, data);
    TestRandomAccess(rnd, keys, data);
  }

  void TestForwardScan(const std::vector<std::string> &keys,
                       const KVMap &data) {
    Iterator *iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    iter->SeekToFirst();
    for (auto it = data.begin(); it != data.end(); ++it) {
      ASSERT_EQ(ToString(data, it), ToString(iter));
      iter->Next();
    }
    ASSERT_TRUE(!iter->Valid());
    delete iter;
  }

  void TestBackwardScan(const std::vector<std::string> &keys,
                        const KVMap &data) {
    Iterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    iter->SeekToLast();
    for (auto it = data.rbegin(); it != data.rend(); ++it) {
      ASSERT_EQ(ToString(data, it), ToString(iter));
      iter->Prev();
    }
    ASSERT_TRUE(!iter->Valid());
    delete iter;
  }

  void TestRandomAccess(Random *rnd, const std::vector<std::string> &keys,
                        const KVMap &data) {
    static constexpr bool kVerbose = false;
    Iterator *iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    auto model_iter = data.begin();
    if (kVerbose) std::fprintf(stderr, "---\n");
    for (int i = 0; i < 200; ++i) {
      const int toss = rnd->Uniform(5);
      switch (toss) {
        case 0: {
          if (iter->Valid()) {
            if (kVerbose) std::fprintf(stderr, "Next\n");
            iter->Next();
            ++model_iter;
            ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          }
          break;
        }
        case 1: {
          if (kVerbose) std::fprintf(stderr, "SeekToFirst\n");
          iter->SeekToFirst();
          model_iter = data.begin();
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }
        case 2: {
          std::string key = PickRandomKey(rnd, keys);
          model_iter = data.lower_bound(key);
          if (kVerbose) std::fprintf(stderr, "Seek '%s'\n", EscapeString(key).c_str());
          iter->Seek(Slice(key));
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }
        case 3: {
          if (iter->Valid()) {
            if (kVerbose) std::fprintf(stderr, "Prev\n");
            iter->Prev();
            if (model_iter == data.begin()) {
              model_iter = data.end();  // Wrap around to invalid value
            } else {
              --model_iter;
            }
            ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          }
          break;
        }
        case 4: {
          if (kVerbose) std::fprintf(stderr, "SeekToLast\n");
          iter->SeekToLast();
          if (keys.empty()) {
            model_iter = data.end();
          } else {
            std::string last = data.rbegin()->first;
            model_iter = data.lower_bound(last);
          }
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }
      }
    }
    delete iter;
  }
  std::string ToString(const KVMap &data, const KVMap::const_iterator &it) {
    if (it == data.end()) {
      return "END";
    } else {
      return "'" + it->first + "->" + it->second + "'";
    }
  }
  
  std::string ToString(const KVMap &data, const KVMap::const_reverse_iterator &it) {
    if (it == data.rend()) {
      return "END";
    } else {
      return "'" + it->first + "->" + it->second + "'";
    }
  }

  std::string ToString(const Iterator *it) {
    if (!it->Valid()) {
      return "END";
    } else {
      return "'" + it->key().ToString() + "->" + it->value().ToString() + "'";
    }
  }

  std::string PickRandomKey(Random *rnd, const std::vector<std::string> &keys) {
    if (keys.empty()) {
      return "foo";
    } else {
      const int index = rnd->Uniform(keys.size());
      std::string result = keys[index];
      switch (rnd->Uniform(3)) {
        case 0:
          // Return an existing key
          break;
        case 1: {
          // Attempt to return something smaller than an existing key
          if (!result.empty() && result[result.size() - 1] > '\0') {
            result[result.size() - 1]--;
          }
          break;
        }
        case 2: {
          // Return something larger than an existing key
          Increment(options_.comparator, &result);
          break;
        }
      }
      return result;
    }
  }

  // Return nulltr if not running against a DB
  DB *db() const { return constructor_->db(); }
 private:
  Options options_;
  Constructor *constructor_;
};

TEST_F(Harness, Empty) {
  for (int i = 0; i < kNumTestArgs; ++i) {
    Init(KTestArgList[i]);
    Random rnd(test::RandomSeed() + 1);
    Test(&rnd);
  }
}

// Special test for a block with no restart entries.  The c++
// leveldb code never generates such blocks, but the Java version of
// leveldb  seems to.
TEST_F(Harness, ZeroRestartPointsInBlock) {
  char data[sizeof(uint32_t)];
  memset(data, 0, sizeof(data));
  BlockContents contents;
  contents.data = Slice(data, sizeof(data));
  contents.cachable = false;
  contents.heap_allocated = false;
  Block block(contents);
  Iterator *iter = block.NewIterator(ByteWiseComparator());
  iter->SeekToFirst();
  ASSERT_TRUE(!iter->Valid());
  iter->SeekToLast();
  ASSERT_TRUE(!iter->Valid());
  iter->Seek("foo");
  ASSERT_TRUE(!iter->Valid());
  delete iter;
  
}

// Test the empty key
TEST_F(Harness, SimpleEmptyKey) {
  for (int i = 0; i < kNumTestArgs; ++i) {
    Init(KTestArgList[i]);
    Random rnd(test::RandomSeed() + 1);
    Add("", "v");
    Test(&rnd);
  }
}

TEST_F(Harness, SimpleSingle) {

  for (int i = 0; i < kNumTestArgs; ++i) {
    Init(KTestArgList[i]);
    Random rnd(test::RandomSeed() + 1);
    Add("abc", "v");
    Test(&rnd);
  }
}

TEST_F(Harness, SimpleMulti) {
  for (int i = 0; i < kNumTestArgs; ++i) {
    Init(KTestArgList[i]);
    Random rnd(test::RandomSeed() + 3);
    Add("abc", "v");
    Add("abcd", "v");
    Add("ac", "v");
    Test(&rnd);
  }
}

TEST_F(Harness, SimpleSpecialKey) {
  for (int i = 0; i < kNumTestArgs; ++i) {
    Init(KTestArgList[i]);
    Random rnd(test::RandomSeed() + 3);
    Add("\xff\xff", "v");
    Test(&rnd);
  }
}

TEST_F(Harness, Randomized) {
  spdlog::set_level(spdlog::level::trace);
  spdlog::enable_backtrace(10);
  for (int i = 0; i < kNumTestArgs; ++i) {
    Init(KTestArgList[i]);
    Random rnd(test::RandomSeed() + 5);
    for (int num_entries = 0; num_entries < 2000;
          num_entries += (num_entries < 50) ? 1 : 200) {
      if ((num_entries % 10) == 0) {
        std::fprintf(stderr, "case %d of %d: num_entries = %d\n",
              (i + 1), int(kNumTestArgs), num_entries);
      }
      for (int e = 0; e < num_entries; ++e) {
        std::string v;
        Add(test::RandomKey(&rnd, rnd.Skewd(4)),
            test::RandomString(&rnd, rnd.Skewd(5), &v).ToString());
      }
      Test(&rnd);
    }
  }
}

TEST_F(Harness, RandomizedLongDB) {
  Random rnd(test::RandomSeed());
  TestArgs args = {DB_TEST, false, 16};
  Init(args);
  constexpr int num_entries = 100000;
  for (int e = 0; e < num_entries; ++e) {
    std::string v;
    Add(test::RandomKey(&rnd, rnd.Skewd(4)),
        test::RandomString(&rnd, rnd.Skewd(5), &v).ToString());
  }
  Test(&rnd);

  // We must have created enough data to force merging
  int files = 0;
  for (int level = 0; level < config::kNumLevels; ++level) {
    std::string value;
    char name[100];
    std::snprintf(name, sizeof(name), "leveldb.num-files-at-level%d", level);
    ASSERT_TRUE(db()->GetProperty(name, &value));
    files += atoi(value.c_str());
  }
  ASSERT_GT(files, 0);
}


TEST(MemTableTest, Simple) {
  InternalKeyComparator cmp(ByteWiseComparator());
  MemTable *memtable = new MemTable(cmp);
  memtable->ref();
  WriteBatch batch;
  WriteBatchInternal::SetSequence(&batch, 100);
  batch.Put("k1", "v1");
  batch.Put("k2", "v2");
  batch.Put("k3", "v3");
  batch.Put("largekey", "vlarge");
  ASSERT_TRUE(WriteBatchInternal::InsertInto(&batch, memtable).ok());

  Iterator *iter = memtable->NewIterator();
  iter->SeekToFirst();
  while (iter->Valid()) {
    std::fprintf(stderr, "key: '%s' -> '%s'\n",
                  iter->key().ToString().c_str(),iter->value().ToString().c_str());
    iter->Next();
  }

  delete iter;
  memtable->Unref();
}

static bool Between(uint64_t val, uint64_t low, uint64_t high) {
  bool result = (val >= low) && (val <= high);
  if (!result) {
    spdlog::error(fmt::format("value {} is not in range [{}, {}]",
                  val, low, high));
  }
  return result;
}

TEST(TableTest, ApproximateOffsetOfPlain) {
  TableConstructor c(ByteWiseComparator());
  c.Add("k01", "hello");
  c.Add("k02", "hello2");
  c.Add("k03", std::string(10000, 'x'));
  c.Add("k04", std::string(200000,'x'));
  c.Add("k05", std::string(300000,'x'));
  c.Add("k06", "hello3");
  c.Add("k07", std::string(100000, 'x'));
  std::vector<std::string> keys;
  KVMap kvmap;
  Options options;
  options.block_size = 1024;
  options.compression = kNoCompression;
  c.Finish(options, &keys, &kvmap);

  ASSERT_TRUE(Between(c.ApproximateOffsetOf("abc"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01a"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k02"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k03"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04"), 10000, 11000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04a"), 210000, 211000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k05"), 210000, 211000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k06"), 510000, 511000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k07"), 510000, 511000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("xyz"), 610000, 611000));
}

static bool CompressionSupported(CompressionType type) {
  std::string out;
  Slice in = "aaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  if (type == kSnappyCompression) {
    return port::Snappy_Compress(in.data(), in.size(), &out);
  } else if (type == kZstdCompression) {
    return port::Zstd_Compress(/*level=*/1, in.data(), in.size(), &out);
  }
  return false;
}

class CompressionTableTest
    : public testing::TestWithParam<std::tuple<CompressionType>> {};

INSTANTIATE_TEST_SUITE_P(CompressionTests, CompressionTableTest,
                         testing::Values(kSnappyCompression,
                                         kZstdCompression));

TEST_P(CompressionTableTest, ApproximateOffsetOfCompressed) {
  CompressionType type = testing::get<0>(GetParam());
  if (!CompressionSupported(type)) {
    GTEST_SKIP() << "skipping compression test: " << type;
  }

  Random rnd(301);
  TableConstructor c(ByteWiseComparator());
  std::string tmp;
  c.Add("k01", "hello");
  c.Add("k02", test::CompressibleString(&rnd, 0.25, 10000, &tmp));
  c.Add("k03", "hello3");
  c.Add("k04", test::CompressibleString(&rnd, 0.25, 10000, &tmp));
  std::vector<std::string> keys;
  KVMap kvmap;
  Options options;
  options.block_size = 1024;
  options.compression = type;
  c.Finish(options, &keys, &kvmap);

  // Expected upper and lower bounds of space used by compressiblee strings
  static constexpr int kSlop = 1000;  // Compressor effectiveness varies.
  constexpr int expected = 2500;      // 10000 * compression ratio (0.25)
  constexpr int min_z = expected - kSlop;
  constexpr int max_z = expected + kSlop;

  ASSERT_TRUE(Between(c.ApproximateOffsetOf("abc"), 0, kSlop));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01"), 0, kSlop));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k02"), 0, kSlop));
  // Have now emitted a large compressible sring, so adjust expected offset.
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k03"), min_z, max_z));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04"), min_z, max_z));
  // Have now emitted a large compressible sring, so adjust expected offset.
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("xyz"), 2 * min_z, 2 * max_z));

}

} // namespace my_leveldb