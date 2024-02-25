
#pragma once

#if !defined(MY_LEVELDB_EXPORT)

#if defined(MY_LEVELDB_SHARED_LIBRARY)
#if defined(_WIN32)

#if defined(MY_LEVELDB_COMPILE_LIBRARY)
#define MY_LEVELDB_EXPORT __declspec(dllexport)
#else
#define MY_LEVELDB_EXPORT __declspec(dllimport)
#endif  // defined(MY_LEVELDB_COMPILE_LIBRARY)

#else  // defined(_WIN32)
#if defined(MY_LEVELDB_COMPILE_LIBRARY)
#define MY_LEVELDB_EXPORT __attribute__((visibility("default")))
#else
#define MY_LEVELDB_EXPORT
#endif
#endif  // defined(_WIN32)

#else  // defined(MY_LEVELDB_SHARED_LIBRARY)
#define MY_LEVELDB_EXPORT
#endif

#endif  // !defined(MY_LEVELDB_EXPORT)