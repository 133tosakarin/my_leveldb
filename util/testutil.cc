

#include "util/testutil.h"
#include "util/random.h"

#include <string>


namespace my_leveldb {

namespace test {

Slice RandomString(Random *rnd, int len, std::string *dst) {
  dst->resize(len);
  for (int i = 0; i < len; ++i) {
    (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));
  }
  return Slice(*dst);
}

std::string RandomKey(Random *rnd, int len) {
  static constexpr char kTestChars[] = {
    '\0', '\1', 'a', 'b', 'c', 'd', 'e',
    '\xfd', '\xfe', '\xff'
  };

  std::string result;
  for (int i = 0; i < len; ++i) {
    result += kTestChars[rnd->Uniform(sizeof(kTestChars))];
  }
  return result;
}

Slice CompressibleString(Random *rnd, double compressible_fraction, size_t len,
                         std::string *dst) {
  int raw = static_cast<int>(len * compressible_fraction);
  if (raw < 1) raw = 1;
  std::string raw_data;
  RandomString(rnd, raw, &raw_data);

  // Duplicate the random data until we have filled "len" bytes
  dst->clear();
  while(dst->size() < len) {
    dst->append(raw_data);
  }
  dst->resize(len);
  return Slice(*dst);
}

} // namespace test
} // namespace my_leveldb