
#include <cstdlib>
#include <limits>
#include "leveldb/env.h"
#include "leveldb/slice.h"

namespace my_leveldb {

void AppendNumberTo(std::string *str, uint64_t num) {
  char buf[30];
  std::snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(num));
  str->append(buf);
}

void AppendEscapeStringTo(std::string *str, const Slice &value) {
  for (size_t i = 0; i < value.size(); ++i) {
    char c = value[i];
    if (c >= ' ' && c <= '~') {
      str->push_back(c);
    } else {
      char buf[10];
      std::snprintf(buf, sizeof(buf), "\\x%02x", static_cast<unsigned int>(c) & 0xff);
      str->append(buf);
    }
  }
}

auto NumberToString(uint64_t num) -> std::string {
  std::string r;
  AppendNumberTo(&r, num);
  return r;
}

auto EscapeString(const Slice &value) -> std::string {
  std::string r;
  AppendEscapeStringTo(&r, value);
  return r;
}

auto ConsumeDecimalNumber(Slice *in, uint64_t *val) -> bool {
  // Constants that will be optimized away.
  constexpr const uint64_t kMaxUin64 = std::numeric_limits<uint64_t>::max();
  constexpr const char kLastDigitOfMaxUint64 = 
       '0' + static_cast<char>(kMaxUin64 % 10);
  
  uint64_t value = 0;

  // reinterpret_cast-ing from char* to uint8_t* to avoid signedness.
  const uint8_t *start = reinterpret_cast<const uint8_t*>(in->data());

  const uint8_t *end = start + in->size();
  auto current = start;
  for (; current != end; ++current) {
    const auto ch = *current;
    if (ch < '0' || ch > '9') break;

    // Overflow check.
    // kMaxUint64 / 10 is also constant and will be optimized away.
    if (value > kMaxUin64 / 10 ||
        (value == kMaxUin64 / 10 && ch > kLastDigitOfMaxUint64)) {
      return false;
    }

    value = (value * 10) + (ch - '0');
  }

  *val = value;
  const size_t digits_consumed = current - start;
  in->remove_prefix(digits_consumed);
  return digits_consumed != 0;
}
}