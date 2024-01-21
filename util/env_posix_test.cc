
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/wait.h>

#include <vector>
#include <unordered_set>


#include "gtest/gtest.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "util/env_posix_test_helper.h"
#include "util/testutil.h"

#if HAVE_O_CLOEXEC

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

// Check that a fork() + exec()-ed child process does not have an extra open FD.
void CheckCloseOnExecDoesNotLeakFDs(
    const std::unordered_set<int> &baseline_open_fds) {
  // Prepare the argument list for the child process.
  // execv() wants mutable buffers
  char switch_buffer[sizeof(kTestCloseOnExecSwitch)];
  std::memcpy(switch_buffer, kTestCloseOnExecSwitch, sizeof(kTestCloseOnExecSwitch));

  int probed_fd;
  GetNewlyOpenedFileDescriptor(baseline_open_fds, &probed_fd);
  std::string fd_string = std::to_string(probed_fd);
  std::vector<char> fd_buffer(fd_string.begin(), fd_string.end());
  fd_buffer.emplace_back('0');

  // The helper process is launched with the command below.
  //    env_posix_tests --test-close-on-exec-helper 3
  char *child_argv[] = { GetArgvZero()->data(), switch_buffer, fd_buffer.data(), nullptr };

  constexpr int kForkInChildProcessReturnValue = 0; 

  int child_pid = fork();
  if (child_pid == kForkInChildProcessReturnValue) {
    ::execv(child_argv[0], child_argv);
    std::fprintf(stderr, "Error spawing child process: %s\n", strerror(errno));
    std::exit(kTextCloseOnExecHelperExecFailedCode);
  }

  int child_status = 0;
  ASSERT_EQ(child_pid, ::waitpid(child_pid, &child_status, 0));
  ASSERT_TRUE(WIFEXITED(child_status))
      << "The helper process did not exit with an exit code";
  ASSERT_EQ(0, WEXITSTATUS(child_status))
      << "The helper process encountered an error";
}

} // namespace

#endif

namespace my_leveldb {
static constexpr int kReadOnlyFileLimit = 4;
static const int kMmapLimit = 4;

class EnvPosixTest : public testing::Test {
 public:
 
 static void SetFileLimits(int read_only_file_limit, int mmap_limit) {
  EnvPosixTestHelper::SetReadOnlyFDLimit(read_only_file_limit);
  EnvPosixTestHelper::SetReadOnlyMMapLimit(mmap_limit);
 }

 EnvPosixTest() : env_(Env::Default()) {}
 Env *env_;
};

TEST_F(EnvPosixTest, TestOpenOnRead) {
  // Write some test data to a single file that will be opened |n| times.
  std::string test_dir;
  ASSERT_MY_LEVELDB_OK(env_->GetTestDirectory(&test_dir));
  std::string test_file = test_dir + "/open_on_read.txt"  ;

  FILE *f = std::fopen(test_file.c_str(), "we");
  ASSERT_TRUE(f != nullptr);
  const char kFileData[] = "abcdefghijklmnoparstuvwxyz";
  fputs(kFileData, f);
  std::fclose(f);

  // Open test file some number above the sum of the two limits to force
  // open-on-read behavior of POSIX Env Leveldb::RandomAccessFile.
  constexpr int kNumFiles = kReadOnlyFileLimit + kMmapLimit + 5;
  my_leveldb::RandomAccessFile *files[kNumFiles] = { 0 };
  for (int i = 0; i < kNumFiles; ++i) {
    ASSERT_MY_LEVELDB_OK(env_->NewRandomAccessFile(test_file, &files[i]));
  }

  char scratch;
  Slice read_result;
  for (int i = 0; i < kNumFiles; ++i) {
    ASSERT_MY_LEVELDB_OK(files[i]->Read(i, 1, &read_result, &scratch));
    ASSERT_EQ(kFileData[i], read_result[0]);
  }

  for (int i = 0; i < kNumFiles; ++i) {
    delete files[i];
  }

  ASSERT_MY_LEVELDB_OK(env_->RemoveFile(test_file));
}
}