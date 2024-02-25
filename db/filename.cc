

#include <cassert>
#include <cstdio>

#include "db/filename.h"
#include "db/dbformat.h"
#include "leveldb/env.h"
#include "util/logging.h"

namespace my_leveldb {

// A utility routine: write "data" to the named file and Sync() it.
auto WriteStringToFileSync(Env *env, const Slice &data,
                           const std::string &fname) -> Status;

static auto MakeFileName(const std::string &dbname, uint64_t number,
                         const char *suffix) -> std::string {
  char buf[100];
  std::snprintf(buf, sizeof(buf), "/%06llu.%s",
               static_cast<unsigned long long>(number), suffix);
  return dbname + buf;
}


auto LogFileName(const std::string &dbname, uint64_t number) -> std::string {
  assert(number > 0);
  return MakeFileName(dbname, number, "log");
}


// Return the name of the sstable with the specified number
// in the db named by "dbname". The result will be prefixed with
// "dbname".
auto TableFileName(const std::string &dbname, uint64_t number) -> std::string {
  assert(number > 0);
  return MakeFileName(dbname, number, "ldb");
}




// Return the legacy file name of an sstable with the specified number
// in the db named by "dbname". The result will be prefixed with
// "dbname".
auto SSTTableFileName(const std::string &dbname, uint64_t number) -> std::string {
  assert(number > 0);
  return MakeFileName(dbname, number, "sst");
}




// Return the name of the descriptor file for  the 
//  db named by "dbname" and specified incarnation number. The result will be prefixed with
// "dbname".
auto DescriptorFileName(const std::string &dbname, uint64_t number) -> std::string {
  assert(number > 0);
  char buf[100];
  std::snprintf(buf, sizeof(buf), "/MANIFEST-%06llu",
               static_cast<unsigned long long>(number));
  return dbname + buf;
}



// Return the name of the current file.  This file contains the name
// of the current manifest file.  The result will be prefixed with
// "dbname".
auto CurrentFileName(const std::string &dbname) -> std::string {
  return dbname + "/CURRENT";
}


// Return the name of the lock file for the db named by "dbname".   
// The result will be prefixed with "dbname".
auto LockFileName(const std::string &dbname) -> std::string {
  return dbname + "/LOCK";
}


// Return the name of a temporary file for the db named by "dbname".   
// The result will be prefixed with "dbname".
auto TempFileName(const std::string &dbname, uint64_t number) -> std::string {
  assert(number > 0);
  return MakeFileName(dbname, number, "dbtmp");
}


// Return the name of the info log file for the db named by "dbname".   
// The result will be prefixed with "dbname".
auto InfoLogFileName(const std::string &dbname) -> std::string {
  return dbname + "/LOG";
}


// Return the name of the old info file for the db named by "dbname".   
// The result will be prefixed with "dbname".
auto OldInfoLogFileName(const std::string &dbname) -> std::string {
  return dbname + "/LOG.old";
}

// Owned filenames have the form:
//      dbname/CURRENT
//      dbname/LOGK
//      dbname/LOG
//      dbname/LOG.old
//      dbname/MANIFEST-[0-9]+
//      dbname/[0-9]+.(log|sst|ldb)
auto ParseFileName(const std::string &filename, uint64_t *number,
                   FileType *type) -> bool {
  Slice rest(filename);
  if (rest == "CURRENT") {
    *number = 0;
    *type = kCurrentFile;
  } else if (rest == "LOCK") {
    *number = 0;
    *type = kDBLockFile;
  } else if (rest == "LOG" || rest == "LOG.old") {
    *number = 0;
    *type = kInfoLogFile;
  } else if (rest.starts_with("MANIFEST-")) {
    rest.remove_prefix(strlen("MANIFEST-"));
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kDescriptorFile;
    *number = num;
  } else {
    // Avoid strtoull() to keep filename format independent of the
    // current locale
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    Slice suffix = rest;
    if (suffix == Slice(".log")) {
      *type = kLogFile;
    } else if (suffix == Slice(".sst") || suffix == Slice(".ldb")) {
      *type = kTableFile;
    } else if (suffix == Slice(".dbtmp")) {
      *type = kTempFile;
    } else {
      return false;
    }
    *number = num;
  }
  return true;
}

// Make the CURRENT file point to the descriptor file with the 
// specified number.
auto SetCurrentFile(Env *env, const std::string &dbname,
                    uint64_t descriptor_number) -> Status {
  // Remove Leading "dbname/" and add newline to manifest file name
  std::string manifest = DescriptorFileName(dbname, descriptor_number);
  Slice contents = manifest;
  assert(contents.starts_with(dbname + "/"));
  contents.remove_prefix(dbname.size() + 1);
  std::string tmp = TempFileName(dbname, descriptor_number);
  Status s = WriteStringToFileSync(env, contents.ToString() + "\n", tmp);
  if (s.ok()) {
    s = env->RenameFile(tmp, CurrentFileName(dbname));
  }
  if (!s.ok()) {
    env->RemoveFile(tmp);
  }
  return s;
}


}

