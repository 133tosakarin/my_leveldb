# CMake generated Testfile for 
# Source directory: /root/my_leveldb
# Build directory: /root/my_leveldb/build
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(my_leveldb_tests "/root/my_leveldb/build/my_leveldb_tests")
set_tests_properties(my_leveldb_tests PROPERTIES  _BACKTRACE_TRIPLES "/root/my_leveldb/CMakeLists.txt;355;add_test;/root/my_leveldb/CMakeLists.txt;0;")
subdirs("third_party/googletest")
subdirs("third_party/spdlog")
