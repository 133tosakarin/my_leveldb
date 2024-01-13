#ifndef STORAGE_MY_LEVELDB_PORT_PORT_CONFIG_H_
#define STORAGE_MY_LEVELDB_PORT_PORT_CONFIG_H_

// Define to 1 if you have a definition for fdatasync() in <unistd.h>.
#if !defined(HAVE_FDATASYNC)
#cmakedefine01 HAVE_FDATASYNC
#endif  // !defined(HAVE_FDATASYNC)

// Define to 1 if you have a definition for F_FULLFSYNC in <fcntl.h>.
#if !defined(HAVE_FULLFSYNC)
#cmakedefine01 HAVE_FULLFSYNC
#endif  // !defined(HAVE_FULLFSYNC)

// Define to 1 if you have a definition for O_CLOEXEC in <fcntl.h>.
#if !defined(HAVE_O_CLOEXEC)
#cmakedefine01 HAVE_O_CLOEXEC
#endif  // !defined(HAVE_O_CLOEXEC)

// Define to 1 if you have Google CRC32C.
#if !defined(HAVE_CRC32C)
#cmakedefine01 HAVE_CRC32C
#endif  // !defined(HAVE_CRC32C)

// Define to 1 if you have Google Snappy.
#if !defined(HAVE_SNAPPY)
#cmakedefine01 HAVE_SNAPPY
#endif  // !defined(HAVE_SNAPPY)

// Define to 1 if you have Zstd.
#if !defined(HAVE_Zstd)
#cmakedefine01 HAVE_ZSTD
#endif  // !defined(HAVE_ZSTD)

#endif  // STORAGE_MY_LEVELDB_PORT_PORT_CONFIG_H_