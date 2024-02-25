

#pragma once

#include <cstdint>
#include <string>

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "port/port.h"

namespace my_leveldb {

class Env;

enum FileType {
  kLogFile,
  kDBLockFile,
  kTableFile,
  kDescriptorFile,
  kCurrentFile,
  kTempFile,
  kInfoLogFile    // Either the current one, or an old one
};

// Return the name of the log file with the specified number
// in the db named by "dbname". The result will be prefixed with
// "dbname".
auto LogFileName(const std::string &dbname, uint64_t number) -> std::string;


// Return the name of the sstable with the specified number
// in the db named by "dbname". The result will be prefixed with
// "dbname".
auto TableFileName(const std::string &dbname, uint64_t number) -> std::string;


// Return the legacy file name of an sstable with the specified number
// in the db named by "dbname". The result will be prefixed with
// "dbname".
auto SSTTableFileName(const std::string &dbname, uint64_t number) -> std::string;


// Return the name of the descriptor file for  the 
//  db named by "dbname" and specified incarnation number. The result will be prefixed with
// "dbname".
auto DescriptorFileName(const std::string &dbname, uint64_t number) -> std::string;

// Return the name of the current file.  This file contains the name
// of the current manifest file.  The result will be prefixed with
// "dbname".
auto CurrentFileName(const std::string &dbname) -> std::string;


// Return the name of the lock file for the db named by "dbname".   
// The result will be prefixed with "dbname".
auto LockFileName(const std::string &dbname) -> std::string;


// Return the name of a temporary file for the db named by "dbname".   
// The result will be prefixed with "dbname".
auto TempFileName(const std::string &dbname, uint64_t number) -> std::string;


// Return the name of the info log file for the db named by "dbname".   
// The result will be prefixed with "dbname".
auto InfoLogFileName(const std::string &dbname) -> std::string;


// Return the name of the old info file for the db named by "dbname".   
// The result will be prefixed with "dbname".
auto OldInfoLogFileName(const std::string &dbname) -> std::string;

// If filename is a leveldb file, store the type of the file in *type
// The number encoded in the filename is stored in *number. If the
// filename was successfully parsed, return true. Else return false
auto ParseFileName(const std::string &filename, uint64_t *number,
                   FileType *type) -> bool;

// Make the CURRENT file point to the descriptor file with the 
// specified number.
auto SetCurrentFile(Env *env, const std::string &dbname,
                    uint64_t descriptor_number) -> Status;

}   // namespace my_leveldb