cmake_minimum_required(VERSION 3.9)

project(my_leveldb VERSION 1.23.0 LANGUAGES C CXX)


# C standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_C_STANDARD)
  # This project can use C11, but will gracefully decay down to C89.
  set(CMAKE_C_STANDARD 17)
  set(CMAKE_C_STANDARD_REQUIRED OFF)
  set(CMAKE_C_EXTENSIONS OFF)
endif(NOT CMAKE_C_STANDARD)

# C++ standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_CXX_STANDARD)
  # This project requires C++11.
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS OFF)
endif(NOT CMAKE_CXX_STANDARD)

if (WIN32)
  set(MY_LEVELDB_PLATFORM_NAME MY_LEVELDB_PLATFORM_WINDOWS)
  add_definitions((-D_UNICODE -DUNICODE))
else (WIN32)
  set(MY_LEVELDB_PLATFORM_NAME MY_LEVELDB_PLATFORM_POSIX)
endif (WIN32)


option(MY_LEVELDB_BUILD_TESTS "Build LevelDB's unit tests" ON)
option(MY_LEVELDB_BUILD_BENCHMARKS "Build LevelDB's benchmarks" ON)
option(MY_LEVELDB_INSTALL "Install LevelDB's header and library" ON)

include(CheckIncludeFile)
check_include_file("unistd.h" HAVE_UNISTD_H)

include(CheckLibraryExists)
check_library_exists(crc32c crc32c_value "" HAVE_CRC32C)
check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
check_library_exists(zstd zstd_compress "" HAVE_ZSTD)
check_library_exists(tcmalloc malloc "" HAVE_TCMALLOC)

include(CheckCXXSymbolExists)
# Using check_cxx_symbol_exists() instead of check_c_symbol_exists() because
# we're including the header from C++, and feature detection should use the same
# compiler language that the project will use later. Principles aside, some
# versions of do not expose fdatasync() in <unistd.h> in standard C mode
# (-std=c11), but do expose the function in standard C++ mode (-std=c++11).
check_cxx_symbol_exists(fdatasync "unistd.h" HAVE_FDATASYNC)
check_cxx_symbol_exists(F_FULLFSYNC "fcntl.h" HAVE_FULLFSYNC)
check_cxx_symbol_exists(O_CLOEXEC "fcntl.h" HAVE_O_CLOEXEC)

# Test whether -Wthread-safety is available. See
# https://clang.llvm.org/docs/ThreadSafetyAnalysis.html
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-Wthread-safety -ggdb HAVE_CLANG_THREAD_SAFETY)

# Used by googletest.
check_cxx_compiler_flag(-Wno-missing-field-initializers -ggdb
                        MY_LEVELDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)

include(CheckCXXSourceCompiles)

# Test whether C++17 __has_include is available.
check_cxx_source_compiles("
#if defined(__has_include) &&  __has_include(<string>)
#include <string>
#endif
int main() { std::string str; return 0; }
" HAVE_CXX17_HAS_INCLUDE)

set(MY_LEVELDB_PUBLIC_INCLUDE_DIR "include/leveldb")
set(MY_LEVELDB_PORT_CONFIG_DIR "include/port")

message("${MY_LEVELDB_PUBLIC_INCLUDE_DIR}")

configure_file(
  "port/port_config.h.in"
  "${PROJECT_BINARY_DIR}/${MY_LEVELDB_PORT_CONFIG_DIR}/port_config.h"
)

include_directories(
  "${PROJECT_SOURCE_DIR}/include"
  "${PROJECT_SOURCE_DIR}/util"
  "${PROJECT_BINARY_DIR}/include"
  "."
)
message("proj: ${PROJECT_SOURCE_DIR}")

if(BUILD_SHARED_LIBS)
  # Only export LEVELDB_EXPORT symbols from the shared library.
  message("add_compile_options")
  add_compile_options(-fvisibility=hidden -ggdb)
endif(BUILD_SHARED_LIBS)
# add_compile_definitions(-ggdb)
# Must be included before CMAKE_INSTALL_INCLUDEDIR is used.
include(GNUInstallDirs)

add_library(my_leveldb "")

target_sources(my_leveldb
  PRIVATE
    "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
    # "db/builder.cc"
    # "db/builder.h"
    # "db/c.cc"
    # "db/db_impl.cc"
    # "db/db_impl.h"
    # "db/db_iter.cc"
    # "db/db_iter.h"
     "db/dbformat.cc"
     "db/dbformat.h"
    # "db/dumpfile.cc"
    # "db/filename.cc"
    # "db/filename.h"
     "db/log_format.h"
     "db/log_reader.cc"
     "db/log_reader.h"
     "db/log_writer.cc"
     "db/log_writer.h"
     "db/memtable.cc"
     "db/memtable.h"
    # "db/repair.cc"
     "db/skiplist.h"
    # "db/snapshot.h"
    # "db/table_cache.cc"
    # "db/table_cache.h"
    # "db/version_edit.cc"
    # "db/version_edit.h"
    # "db/version_set.cc"
    # "db/version_set.h"
    # "db/write_batch_internal.h"
    # "db/write_batch.cc"
     "port/port_stdcxx.h"
     "port/port.h"
    # "port/thread_annotations.h"
     "table/block_builder.cc"
     "table/block_builder.h"
     "table/block.cc"
     "table/block.h"
     "table/filter_block.cc"
     "table/filter_block.h"
     "table/format.cc"
     "table/format.h"
    # "table/iterator_wrapper.h"
    # "table/iterator.cc"
    # "table/merger.cc"
    # "table/merger.h"
    # "table/table_builder.cc"
    # "table/table.cc"
    # "table/two_level_iterator.cc"
    # "table/two_level_iterator.h"
     "util/arena.cc"
     "util/arena.h"
     "util/bloom.cc"
     "util/cache.cc"
     "util/coding.cc"
     "util/coding.h"
     "util/comparator.cc"
     "util/crc32c.cc"
     "util/crc32c.h"
     "util/env.cc"
     "util/filter_policy.cc"
    "util/hash.cc"
     "util/hash.h"
     "util/logging.cc"
     "util/logging.h"
    # "util/mutexlock.h"
     "util/no_destructor.h"
     "util/options.cc"
     "util/random.h"
     "util/status.cc"

  # Only CMake 3.3+ supports PUBLIC sources in targets exported by "install".
  $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>
    # "${MY_LEVELDB_PUBLIC_INCLUDE_DIR}/c.h"
     "${MY_LEVELDB_PUBLIC_INCLUDE_DIR}/cache.h"
     "${MY_LEVELDB_PUBLIC_INCLUDE_DIR}/comparator.h"
     "${MY_LEVELDB_PUBLIC_INCLUDE_DIR}/db.h"
    # "${MY_LEVELDB_PUBLIC_INCLUDE_DIR}/dumpfile.h"
     "${MY_LEVELDB_PUBLIC_INCLUDE_DIR}/env.h"
     "${MY_LEVELDB_PUBLIC_INCLUDE_DIR}/export.h"
     "${MY_LEVELDB_PUBLIC_INCLUDE_DIR}/filter_policy.h"
     "${MY_LEVELDB_PUBLIC_INCLUDE_DIR}/iterator.h"
     "${MY_LEVELDB_PUBLIC_INCLUDE_DIR}/options.h"
     "${MY_LEVELDB_PUBLIC_INCLUDE_DIR}/slice.h"
     "${MY_LEVELDB_PUBLIC_INCLUDE_DIR}/status.h"
     "${MY_LEVELDB_PUBLIC_INCLUDE_DIR}/table_builder.h"
    # "${MY_LEVELDB_PUBLIC_INCLUDE_DIR}/table.h"
    # "${MY_LEVELDB_PUBLIC_INCLUDE_DIR}/write_batch.h"
)

if (WIN32)
  target_sources(my_leveldb
    PRIVATE
      "util/env_windows.cc"
      "util/windows_logger.h" 
  )
else (WIN32)
  message("posix")
  target_sources(my_leveldb
    PRIVATE 
       # "util/env_posix.cc"
       "util/posix_logger.h"
  )
endif (WIN32)

# MemEnv is not part of the interface and could be pulled to a separate library.
target_sources(my_leveldb
  PRIVATE
     "helper/memenv/memenv.cc"
     "helper/memenv/memenv.h"
)

target_include_directories(my_leveldb
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
message("thid_dir: ${PROJECT_SOURCE_DIR}/include")
set_target_properties(my_leveldb
  PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

target_compile_definitions(my_leveldb
  PRIVATE
    # Used by include/export.h when building shared libraries.
    MY_LEVELDB_COMPILE_LIBRARY
    # Used by port/port.h.
    ${MY_LEVELDB_PLATFORM_NAME}=1
)
if (NOT HAVE_CXX17_HAS_INCLUDE)
  target_compile_definitions(my_leveldb
    PRIVATE
      MY_LEVELDB_HAS_PORT_CONFIG_H=1
  )
endif(NOT HAVE_CXX17_HAS_INCLUDE)

if(BUILD_SHARED_LIBS)
  target_compile_definitions(my_leveldb
    PUBLIC
      # Used by include/export.h.
      MY_LEVELDB_SHARED_LIBRARY
  )
endif(BUILD_SHARED_LIBS)

if(HAVE_CLANG_THREAD_SAFETY)
  message("opend -ggdb")
  target_compile_options(my_leveldb
    PUBLIC
      -Werror -Wthread-safety -g)
endif(HAVE_CLANG_THREAD_SAFETY)

if(HAVE_CRC32C)
  target_link_libraries(my_leveldb crc32c)
endif(HAVE_CRC32C)
if(HAVE_SNAPPY)
  target_link_libraries(my_leveldb snappy)
endif(HAVE_SNAPPY)
if(HAVE_ZSTD)
  target_link_libraries(my_leveldb zstd)
endif(HAVE_ZSTD)
if(HAVE_TCMALLOC)
  target_link_libraries(my_leveldb tcmalloc)
endif(HAVE_TCMALLOC)

message(":${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -ggdb")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
message(STATUS "CMAKE_CXX_FLAGS_DEBUG: ${CMAKE_CXX_FLAGS_DEBUG}")
add_compile_options(-O0 -ggdb)
# Needed by port_stdcxx.h
# find_package(Threads REQUIRED)
# target_link_libraries(leveldb Threads::Threads)
# 
# add_executable(leveldbutil
#   "db/leveldbutil.cc"
# )
# target_link_libraries(leveldbutil leveldb)

if(MY_LEVELDB_BUILD_TESTS)
  enable_testing()
  message("my_leveldb_build_tests")
  # Prevent overriding the parent project's compiler/linker settings on Windows.
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(install_gtest OFF)
  set(install_gmock OFF)
  set(build_gmock ON)

  # This project is tested using GoogleTest.
  add_subdirectory("third_party/googletest")
  add_subdirectory("third_party/spdlog")

  # GoogleTest triggers a missing field initializers warning.
  if(MY_LEVELDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)
    set_property(TARGET gtest
        APPEND PROPERTY COMPILE_OPTIONS -Wno-missing-field-initializers -ggdb)
    set_property(TARGET gmock
        APPEND PROPERTY COMPILE_OPTIONS -Wno-missing-field-initializers -ggdb)
  endif(MY_LEVELDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)
  message("myleveldb: ${my_leveldb_tests}, ${PROJECT_BINARY_DIR}/${MY_LEVELDB_PORT_CONFIG_DIR}/port_config.h")
  add_executable(my_leveldb_tests "")

  target_sources(my_leveldb_tests
    PRIVATE
      # "db/fault_injection_test.cc"
      # "issues/issue178_test.cc"
      # "issues/issue200_test.cc"
      # "issues/issue320_test.cc"
       "${PROJECT_BINARY_DIR}/${MY_LEVELDB_PORT_CONFIG_DIR}/port_config.h"
      # "util/env_test.cc"
      # "util/status_test.cc"
      # "util/no_destructor_test.cc"
       "util/testutil.cc"
       "util/testutil.h"
  )
  message("my_leveldb_tests: ${my_leveldb_tests}")
  if(NOT BUILD_SHARED_LIBS)
    target_sources(my_leveldb_tests
      PRIVATE
        # "db/autocompact_test.cc"
        # "db/corruption_test.cc"
        # "db/db_test.cc"
        # "db/dbformat_test.cc"
        # "db/filename_test.cc"
        # "db/log_test.cc"
        # "db/recovery_test.cc"
         "db/skiplist_test.cc"
        # "db/version_edit_test.cc"
        # "db/version_set_test.cc"
        # "db/write_batch_test.cc"
        # "helpers/memenv/memenv_test.cc"
         "table/filter_block_test.cc"
        # "table/table_test.cc"
        # "util/arena_test.cc"
        # "util/bloom_test.cc"
        # "util/cache_test.cc"
        # "util/coding_test.cc"
        # "util/crc32c_test.cc"
        # "util/hash_test.cc"
        # "util/logging_test.cc"
          "util/no_destructor_test.cc"
    )
  endif(NOT BUILD_SHARED_LIBS)
  target_link_libraries(my_leveldb_tests my_leveldb gmock gtest gtest_main)
  target_compile_definitions(my_leveldb_tests
    PRIVATE
      ${MY_LEVELDB_PLATFORM_NAME}=1
  )
  if (NOT HAVE_CXX17_HAS_INCLUDE)
    target_compile_definitions(my_leveldb_tests
      PRIVATE
        MY_LEVELDB_HAS_PORT_CONFIG_H=1
    )
  endif(NOT HAVE_CXX17_HAS_INCLUDE)

  add_test(NAME "my_leveldb_tests" COMMAND "my_leveldb_tests")

  function(my_leveldb_test test_file)
    get_filename_component(test_target_name "${test_file}" NAME_WE)

    add_executable("${test_target_name}" "")
    target_sources("${test_target_name}"
      PRIVATE
        "${PROJECT_BINARY_DIR}/${MY_LEVELDB_PORT_CONFIG_DIR}/port_config.h"
         "util/testutil.cc"
         "util/testutil.h"

        "${test_file}"
    )
    target_link_libraries("${test_target_name}" my_leveldb gmock gtest)
    target_compile_definitions("${test_target_name}"
      PRIVATE
        ${MY_LEVELDB_PLATFORM_NAME}=1
    )
    if (NOT HAVE_CXX17_HAS_INCLUDE)
      target_compile_definitions("${test_target_name}"
        PRIVATE
          MY_LEVELDB_HAS_PORT_CONFIG_H=1
      )
    endif(NOT HAVE_CXX17_HAS_INCLUDE)

    add_test(NAME "${test_target_name}" COMMAND "${test_target_name}")
  endfunction(my_leveldb_test)

  # my_leveldb_test("db/c_test.c")

  if(NOT BUILD_SHARED_LIBS)
    # TODO(costan): This test also uses
    #               "util/env_{posix|windows}_test_helper.h"
    if (WIN32)
      my_leveldb_test("util/env_windows_test.cc")
    else (WIN32)
      message("env_posix_test")
      my_leveldb_test("util/env_posix_test.cc")
    endif (WIN32)
  endif(NOT BUILD_SHARED_LIBS)
endif(MY_LEVELDB_BUILD_TESTS)
