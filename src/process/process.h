#pragma once

#include "src/base/base.h"
#include "src/base/io_threads.h"
#include "src/base/logger.h"

using namespace std::placeholders;
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
  Process(asio::io_context&        io_context,
          std::filesystem::path    executable,
          std::vector<std::string> args);

  Process(std::filesystem::path executable, std::vector<std::string> args)
      : Process(io_context(), std::move(executable), std::move(args)) {}

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

  template <bool err>
  void readline(boost::system::error_code ec, std::size_t) {
    auto& buffer = err ? stderr_buffer_ : stdout_buffer_;

    // On eof or error read the rest as a line.
    if (ec) {
      elog(ec, __func__);
      if (!buffer.empty() && buffer.back() != '\n')
        buffer.push_back('\n');
    }

    auto pos = buffer.find('\n');

    while (pos != string::npos) {
      if (err)
        on_stderr_line(std::string_view{buffer.data(), pos});
      else
        on_stdout_line(std::string_view{buffer.data(), pos});

      buffer.erase(0, pos + 1);
      pos = buffer.find('\n');
    }

    if (!ec) {
      if (err)
        asio::async_read_until(*stderr_pipe_,
                               asio::dynamic_buffer(buffer),
                               '\n',
                               std::bind(&Process::readline<err>, this, _1, _2));
      else
        asio::async_read_until(*stdout_pipe_,
                               asio::dynamic_buffer(buffer),
                               '\n',
                               std::bind(&Process::readline<err>, this, _1, _2));
    } else
      io_latch_->count_down();
  }

  asio::io_context& io_context_;

  std::filesystem::path const    executable_;
  std::vector<std::string> const args_;
  pid_t                          pid_ = -1;

  string stdout_buffer_;
  string stderr_buffer_;

  std::optional<boost::asio::readable_pipe> stdout_pipe_;
  std::optional<boost::asio::readable_pipe> stderr_pipe_;
  std::unique_ptr<std::latch>               io_latch_{nullptr};
};
