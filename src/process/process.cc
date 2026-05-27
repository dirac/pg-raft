#include "src/process/process.h"

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <thread>

Process::Process(std::string executable, std::vector<std::string> args)
    : executable_(std::move(executable)), args_(std::move(args)) {}

Process::~Process() {
  if (is_running()) {
    kill();
    wait();
  }
  join_readers();
}

Process::Process(Process&& other) noexcept
    : executable_(std::move(other.executable_)),
      args_(std::move(other.args_)),
      pid_(other.pid_),
      stdout_thread_(std::move(other.stdout_thread_)),
      stderr_thread_(std::move(other.stderr_thread_)) {
  other.pid_ = -1;
}

Process& Process::operator=(Process&& other) noexcept {
  if (this != &other) {
    if (is_running()) {
      kill();
      wait();
    }
    join_readers();
    executable_ = std::move(other.executable_);
    args_ = std::move(other.args_);
    pid_ = other.pid_;
    stdout_thread_ = std::move(other.stdout_thread_);
    stderr_thread_ = std::move(other.stderr_thread_);
    other.pid_ = -1;
  }
  return *this;
}

bool Process::start() {
  if (is_running()) return false;

  std::cerr << to_string() << std::endl;

  int out_fds[2], err_fds[2];
  if (pipe(out_fds) < 0) return false;
  if (pipe(err_fds) < 0) { close(out_fds[0]); close(out_fds[1]); return false; }

  pid_ = fork();
  if (pid_ < 0) {
    close(out_fds[0]); close(out_fds[1]);
    close(err_fds[0]); close(err_fds[1]);
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

    std::vector<const char*> argv;
    argv.push_back(executable_.c_str());
    for (const auto& arg : args_) argv.push_back(arg.c_str());
    argv.push_back(nullptr);
    execv(executable_.c_str(), const_cast<char* const*>(argv.data()));
    _exit(127);
  }

  // Parent: close write ends and start reader threads.
  close(out_fds[1]);
  close(err_fds[1]);
  stdout_thread_ = std::thread(&Process::read_lines, this, out_fds[0], true);
  stderr_thread_ = std::thread(&Process::read_lines, this, err_fds[0], false);
  return true;
}

void Process::kill() {
  if (pid_ > 0) {
    ::kill(pid_, SIGKILL);
  }
}

void Process::stop(int timeout_ms) {
  if (pid_ <= 0) return;

  ::kill(pid_, SIGTERM);

  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    if (try_wait().has_value()) {
      join_readers();
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
    pid_ = -1;
    exit_code = decode_status(status);
  }
  join_readers();
  return exit_code;
}

std::optional<int> Process::try_wait() {
  if (pid_ <= 0) return std::nullopt;

  int status = 0;
  pid_t result = waitpid(pid_, &status, WNOHANG);
  if (result == 0) return std::nullopt;

  pid_ = -1;
  return decode_status(status);
}

bool Process::is_running() const {
  return pid_ > 0;
}

void Process::on_stdout_line(std::string_view line) {
  std::cerr << line << '\n';
}

void Process::on_stderr_line(std::string_view line) {
  std::cerr << line << '\n';
}

// ── private ───────────────────────────────────────────────────────────────────

void Process::read_lines(int fd, bool is_stdout) {
  char buf[4096];
  std::string partial;
  for (;;) {
    ssize_t n = read(fd, buf, sizeof(buf));
    if (n <= 0) break;
    partial.append(buf, static_cast<size_t>(n));
    std::string::size_type pos;
    while ((pos = partial.find('\n')) != std::string::npos) {
      if (is_stdout) on_stdout_line(std::string_view(partial).substr(0, pos));
      else           on_stderr_line(std::string_view(partial).substr(0, pos));
      partial.erase(0, pos + 1);
    }
  }
  if (!partial.empty()) {
    if (is_stdout) on_stdout_line(partial);
    else           on_stderr_line(partial);
  }
  close(fd);
}

void Process::join_readers() {
  if (stdout_thread_.joinable()) stdout_thread_.join();
  if (stderr_thread_.joinable()) stderr_thread_.join();
}

int Process::decode_status(int status) {
  if (WIFEXITED(status)) return WEXITSTATUS(status);
  if (WIFSIGNALED(status)) return -WTERMSIG(status);
  return -1;
}
