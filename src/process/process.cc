#include "src/process/process.h"

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <thread>

Process::Process(std::string executable, std::vector<std::string> args)
    : executable_(std::move(executable)), args_(std::move(args)) {}

Process::~Process() {
  if (IsRunning()) {
    Kill();
    Wait();
  }
}

Process::Process(Process&& other) noexcept
    : executable_(std::move(other.executable_)),
      args_(std::move(other.args_)),
      pid_(other.pid_) {
  other.pid_ = -1;
}

Process& Process::operator=(Process&& other) noexcept {
  if (this != &other) {
    if (IsRunning()) {
      Kill();
      Wait();
    }
    executable_ = std::move(other.executable_);
    args_ = std::move(other.args_);
    pid_ = other.pid_;
    other.pid_ = -1;
  }
  return *this;
}

bool Process::Start() {
  if (IsRunning()) return false;

  pid_ = fork();
  if (pid_ < 0) {
    pid_ = -1;
    return false;
  }
  if (pid_ == 0) {
    // Child: build argv and exec. _exit on failure to avoid flushing parent
    // stdio buffers.
    std::vector<const char*> argv;
    argv.push_back(executable_.c_str());
    for (const auto& arg : args_) {
      argv.push_back(arg.c_str());
    }
    argv.push_back(nullptr);
    execv(executable_.c_str(), const_cast<char* const*>(argv.data()));
    _exit(127);
  }
  return true;
}

void Process::Kill() {
  if (pid_ > 0) {
    kill(pid_, SIGKILL);
  }
}

void Process::Stop(int timeout_ms) {
  if (pid_ <= 0) return;

  kill(pid_, SIGTERM);

  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    if (TryWait().has_value()) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  Kill();
  Wait();
}

int Process::Wait() {
  if (pid_ <= 0) return -1;

  int status = 0;
  waitpid(pid_, &status, 0);
  pid_ = -1;
  return DecodeStatus(status);
}

std::optional<int> Process::TryWait() {
  if (pid_ <= 0) return std::nullopt;

  int status = 0;
  pid_t result = waitpid(pid_, &status, WNOHANG);
  if (result == 0) return std::nullopt;

  pid_ = -1;
  return DecodeStatus(status);
}

bool Process::IsRunning() const {
  return pid_ > 0;
}

int Process::DecodeStatus(int status) {
  if (WIFEXITED(status)) return WEXITSTATUS(status);
  if (WIFSIGNALED(status)) return -WTERMSIG(status);
  return -1;
}
