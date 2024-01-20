
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <sys/resource.h>

#include <vector>
#include <unordered_set>


#include "gtest/gtest.h"
#include "leveldb/env.h"
#include "util/env_posix_test_helper.h"
namespace {

// Exit codes for the helper process spawned by TetCloseOnExec* tests.
// Useful for debugging test failures.
constexpr int kTextCloseOnExecHelperExecFailedCode = 61;
constexpr int kTestCloseOnExecHelperDup2FailCode = 62;
constexpr int kTestCloseOnExecHelperFoundOpenFdCode = 63;

// Global set by main and read in TestCloseOnExec.
//
// The argv[0] value is stored in std::vector instead of a std::string because
// std::string does not return a mutable pointer to its buffer util C++17;.
//
// The vector stores the string pointed to by argv[0], plus the trailing null.

std::vector<char>* GetArgvZero() {
  static std::vector<char> program_name;
  return &program_name;
}

// Command-line switch used to run this test as CloseOnExecSwitch helper.
static constexpr char kTestCloseOnExecSwitch[] = "--test-close-on-exec-helper";
// Executred in a separate process by TestCloseOnExec* tests.
//
// main() delegates to this function when the test executable is launched with
// a special command-line switch. TestCloseOnExec* tests fork() + exec() the test
// executable and pass the special command-line switch
//
// When main() delegates to this function, the process probes whether a given
// file descriptor is open, and communicates the result via its exit code.
int TestCloseOnExecHelperMain(char *pid_arg) {
  int fd = std::atoi(pid_arg);
  // when given the same file descriptor twice, dup2() returns -1 if the 
  // file descriptor is closed, or the given file descriptor if it is open.
  if (::dup2(fd, fd) == fd) {
    std::fprintf(stderr, "Unexecpted open fd %d\n", fd);
    return kTestCloseOnExecHelperDup2FailCode;
  }
  // Double-check that dup2() is saying the file descriptor is closed.
  if (errno != EBADF) {
    std::fprintf(stderr, "Unexpected errno after calling dup2 on fd %d: %s\n",
                              fd, std::strerror(errno));
    return kTestCloseOnExecHelperDup2FailCode;
  }
  return 0;
}

// File descriptors are small non-negavive integers.
//
// Returns void so the implementation can use ASSERT_EQ.
void GetMaxFileDescriptor(int *result_fd) {
  ::rlimit fd_rlimit;
  ASSERT_EQ(0, ::getrlimit(RLIMIT_NOFILE, &fd_rlimit));
  *result_fd = fd_rlimit.rlim_cur;
}

// Iterates through all possible FDs and returns the currently open ones.
//
// Return void so the implementation can use ASSERT_EQ.
void GetOpenFileDescriptors(std::unordered_set<int> *open_fds) {
 int max_fd = 0;
 GetMaxFileDescriptor(&max_fd);
 for (int fd = 0; fd < max_fd; ++fd) {
  if (::dup2(fd, fd) != fd) {
    // When given the same file descriptor twice, dup2 returns -1 if the 
    // file descriptor is closed, or the given file descriptor if it is open.
    //
    // Double-check that dup2() is saying the fd is closed.
    ASSERT_EQ(EBADF, errno)
        << "dup2() should set errno to EBADF on closed file descriptors";
    continue;
  }
  open_fds->insert(fd);
 }
}

// Finds an FD open since a previous call to GetOpenFileDescriptors().
//
// |baseline_open_fds| is the result of a previous GetOpenFileDescriptos()
// call. Assumes that exactly one FD was opened since that call.
//
// Returns void so the implementation can use ASSERT_EQ.
void GetNewlyOpenedFileDescriptor(
      const std::unordered_set<int> &baseline_open_fds, int *result_fd) {
  std::unordered_set<int> open_fds;
  GetOpenFileDescriptors(&open_fds);
  for (int fd : baseline_open_fds) {
    ASSERT_EQ(1, open_fds.count(fd))
      << "Previously opended file descriptor was closed during test setup";
    open_fds.erase(fd);
  }
  ASSERT_EQ(1, open_fds.size())
    << "Expected exactly one newly opened file descriptor during test setup";
  *result_fd = *open_fds.begin();
}


}