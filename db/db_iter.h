

#pragma once

#include <cstdint>

#include "db/dbformat.h"
#include "leveldb/db.h"

namespace my_leveldb {

class DBImpl;

// Return a new iterator that convers internal keys (yielded by
// "*internal_iter") that were live at the specified "sequenec" number
// into appropriate user keys.
Iterator* NewDBIterator(DBImpl *db, const Comparator *user_key_comparator,
                        Iterator *internal_iter,  SequenceNumber sequence,
                        uint32_t seed);
} // namespace my_leveldb