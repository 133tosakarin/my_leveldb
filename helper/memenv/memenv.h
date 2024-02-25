
#pragma once

#include "leveldb/export.h"

namespace my_leveldb {

class Env;

// Returns a new enviroment that stores its data in memory and delegates
// all non-file-storage tasks to base_env. The caller must delete the result
// when it is no longer needed.
// *base_env must remain live while the result is in use.
Env *NewMemEnv(Env *base_env);
}