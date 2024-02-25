
#include "leveldb/options.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"


namespace my_leveldb {

Options::Options() : comparator(ByteWiseComparator()), env(Env::Default()) {}
}