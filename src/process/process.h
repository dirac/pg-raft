#pragma once

#include <sys/types.h>

#include <format>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

// Manages the lifecycle of a child process.
//
// start() forks and execs the process. stop() sends SIGTERM and waits up to a
// timeout before sending SIGKILL. The destructor kills and reaps the child if
// it is still running.
//
// stdout and stderr are captured line-by-line. Override on_stdout_line() and
// on_stderr_line() to handle output; the defaults forward each line to stderr.
// These callbacks are invoked from dedicated reader threads, so overrides must
// be thread-safe. When a running Process is moved, the reader threads continue
// dispatching on the moved-from object — safe as long as the moved-from object
// outlives the subsequent wait().
class Process {
 public:
  Process(std::filesystem::path executable, std::vector<std::string> args);
  virtual ~Process();

  Process(Process const&)            = delete;
  Process& operator=(Process const&) = delete;

  // Forks and execs the process. Returns false if already running or exec
  // fails.
  bool start();

  // Sends SIGTERM; if the process has not exited within timeout_ms, sends
  // SIGKILL and waits.
  void stop(int timeout_ms = 5000);

  // Sends SIGKILL without waiting.
  void kill();

  // Blocks until the process exits and returns its exit code. Negative values
  // indicate termination by signal (-SIGNUM).
  int wait();

  // Non-blocking check. Returns the exit code if the process has exited,
  // otherwise std::nullopt.
  std::optional<int> try_wait();

  // True when the process has been started and not yet fully reaped.
  bool is_running() const;

  pid_t pid() const { return pid_; }

  std::string to_string() const {
    std::string result = std::format("[{0}] {1}", pid_, executable_);
    return std::accumulate(
        std::begin(args_), std::end(args_), std::move(result), [](auto result, auto const& arg) {
          return result + " " + arg;
        });
  }

 protected:
  virtual void on_stdout_line(std::string_view line);
  virtual void on_stderr_line(std::string_view line);

 private:
  static int decode_status(int status);
  void       read_lines(int fd, bool is_stdout);
  void       join_readers();

  std::filesystem::path const    executable_;
  std::vector<std::string> const args_;
  pid_t                          pid_ = -1;
  std::thread                    stdout_thread_;
  std::thread                    stderr_thread_;
};
