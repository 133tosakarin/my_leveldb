

#pragma once

#include <string>

#include "leveldb/env.h"
#include "leveldb/export.h"
#include "leveldb/status.h"

namespace my_leveldb {


/**
 * Dump the contents of the file named by fname in text format to
 * *dst.  Makes a sequence of dst->Append() calls;  each call is
 * passed the newline-terminated text corresponding to a single 
 * item found in the file.
 * 
 * Returns a non-OK result if fname does not name a leveldb storage
 * file, or if the file cannot be read.
*/
Status DumpFile(Env *env, const std::string &, WritableFile *);

}