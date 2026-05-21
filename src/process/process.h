#pragma once

#include <optional>
#include <string>
#include <vector>

#include <sys/types.h>

// Manages the lifecycle of a child process.
//
// start() forks and execs the process. stop() sends SIGTERM and waits up to a
// timeout before sending SIGKILL. The destructor kills and reaps the child if
// it is still running.
class Process {
 public:
  Process(std::string executable, std::vector<std::string> args);
  ~Process();

  Process(const Process&) = delete;
  Process& operator=(const Process&) = delete;
  Process(Process&&) noexcept;
  Process& operator=(Process&&) noexcept;

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

 private:
  static int decode_status(int status);

  std::string executable_;
  std::vector<std::string> args_;
  pid_t pid_ = -1;
};
