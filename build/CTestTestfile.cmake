# CMake generated Testfile for 
# Source directory: /root/my_leveldb
# Build directory: /root/my_leveldb/build
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(my_leveldb_tests "/root/my_leveldb/build/my_leveldb_tests")
set_tests_properties(my_leveldb_tests PROPERTIES  _BACKTRACE_TRIPLES "/root/my_leveldb/CMakeLists.txt;356;add_test;/root/my_leveldb/CMakeLists.txt;0;")
add_test(env_posix_test "/root/my_leveldb/build/env_posix_test")
set_tests_properties(env_posix_test PROPERTIES  _BACKTRACE_TRIPLES "/root/my_leveldb/CMakeLists.txt;382;add_test;/root/my_leveldb/CMakeLists.txt;394;my_leveldb_test;/root/my_leveldb/CMakeLists.txt;0;")
subdirs("third_party/googletest")
subdirs("third_party/spdlog")
