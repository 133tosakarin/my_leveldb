
#pragma once

#include <cstdint>
#include <cstdio>
#include <string>

#include "spdlog/spdlog.h"

#define LOG_INFO(msg, ...) \
  std::fprintf(stdout,"%s:%d  " msg "\n", __FILE__, __LINE__, \
        ##__VA_ARGS__)
namespace my_leveldb {

class Slice;
class WritableFile;

// Append a human-readable printout of "num" to *str
void AppendNumberTo(std::string *str, uint64_t num);

// Append a human-reaable printout of "value" to *str.
// Escape any non-printable characters found in "value".
void AppendEscapeStringTo(std::string *str, const Slice &value);

// Return a human-readable printout of "num"
auto NumberToString(uint64_t num) -> std::string;

// Return a human-readable version of "value".
// Escape any non-printable characters found in "value".
auto EscapeString(const Slice &value) -> std::string;

// Parse a human-readable number from "*in" into *value. On success,
// advance "*in" past the consumed number and sets "*val" to the
// numeric value. Otherwise, return false and leaves *in an
// unspecified state.
auto ConsumeDecimalNumber(Slice *in, uint64_t *val) -> bool;
}