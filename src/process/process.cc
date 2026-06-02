#include "src/process/process.h"

#include "src/base/logger.h"

Process::Process(asio::io_context&        io_context,
                 std::filesystem::path    executable,
                 std::vector<std::string> args)
    : io_context_(io_context), executable_(std::move(executable)), args_(std::move(args)) {}

Process::~Process() {
  if (is_running()) {
    kill();
    wait();
  }
}

bool Process::start() {
  if (is_running())
    return false;

  log(INFO, "{}", to_string());

  int out_fds[2], err_fds[2];
  if (pipe(out_fds) < 0)
    return false;
  if (pipe(err_fds) < 0) {
    close(out_fds[0]);
    close(out_fds[1]);
    return false;
  }

  pid_ = fork();
  if (pid_ < 0) {
    close(out_fds[0]);
    close(out_fds[1]);
    close(err_fds[0]);
    close(err_fds[1]);
    pid_ = -1;
    return false;
  }

  if (pid_ == 0) {
    // Child: wire up stdout/stderr, then exec. _exit on failure to avoid
    // flushing parent stdio buffers.
    close(out_fds[0]);
    close(err_fds[0]);
    dup2(out_fds[1], STDOUT_FILENO);
    dup2(err_fds[1], STDERR_FILENO);
    close(out_fds[1]);
    close(err_fds[1]);

    std::vector<char const*> argv;
    argv.push_back(executable_.c_str());
    for (auto const& arg : args_)
      argv.push_back(arg.c_str());
    argv.push_back(nullptr);
    execv(executable_.c_str(), const_cast<char* const*>(argv.data()));
    _exit(127);
  }

  // Parent: close write ends and start reader threads.
  close(out_fds[1]);
  close(err_fds[1]);

  stdout_pipe_ = asio::readable_pipe{io_context_, out_fds[0]};
  stderr_pipe_ = asio::readable_pipe{io_context_, err_fds[0]};

  io_latch_ = make_unique<std::latch>(2);

  readline<true>(boost::system::error_code{}, 0);
  readline<false>(boost::system::error_code{}, 0);

  return true;
}

void Process::kill() {
  if (pid_ > 0) {
    ::kill(pid_, SIGKILL);
  }
}

void Process::stop(int timeout_ms) {
  if (pid_ <= 0)
    return;

  ::kill(pid_, SIGTERM);

  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    if (try_wait().has_value()) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  kill();
  wait();
}

int Process::wait() {
  int exit_code = -1;
  if (pid_ > 0) {
    int status = 0;
    waitpid(pid_, &status, 0);
    pid_      = -1;
    exit_code = decode_status(status);
    io_latch_->wait();
  }
  return exit_code;
}

std::optional<int> Process::try_wait() {
  if (pid_ <= 0)
    return std::nullopt;

  int   status = 0;
  pid_t result = waitpid(pid_, &status, WNOHANG);
  if (result == 0)
    return std::nullopt;

  pid_ = -1;
  io_latch_->wait();
  return decode_status(status);
}

bool Process::is_running() const {
  return pid_ > 0;
}

void Process::on_stdout_line(std::string_view line) {
  log(INFO, "{}", line);
}

void Process::on_stderr_line(std::string_view line) {
  log(ERROR, "{}", line);
}

// ── private ───────────────────────────────────────────────────────────────────

int Process::decode_status(int status) {
  if (WIFEXITED(status))
    return WEXITSTATUS(status);
  if (WIFSIGNALED(status))
    return -WTERMSIG(status);
  return -1;
}
