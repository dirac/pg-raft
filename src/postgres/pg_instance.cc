#include "src/postgres/pg_instance.h"

#include <signal.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <thread>

// ── helpers ───────────────────────────────────────────────────────────────────

static std::vector<std::string> build_postgres_args(
    const std::string& data_dir, const std::vector<std::string>& extra) {
  std::vector<std::string> args = {"-D", data_dir};
  args.insert(args.end(), extra.begin(), extra.end());
  return args;
}

// ── PgInstance ────────────────────────────────────────────────────────────────

PgInstance::PgInstance(std::string pg_bin, std::string data_dir,
                       std::vector<std::string> extra_args)
    : pg_bin_(std::move(pg_bin)),
      data_dir_(std::move(data_dir)),
      extra_args_(std::move(extra_args)),
      postgres_(pg_bin_ + "/postgres",
                build_postgres_args(data_dir_, extra_args_)) {}

PgInstance::~PgInstance() {
  if (is_running()) stop();
}

// ── initialisation ────────────────────────────────────────────────────────────

bool PgInstance::init(std::vector<std::string> initdb_args) {
  std::vector<std::string> args = {"-D", data_dir_};
  args.insert(args.end(), initdb_args.begin(), initdb_args.end());
  Process p(pg_bin_ + "/initdb", std::move(args));
  return p.start() && p.wait() == 0;
}

// ── configuration ─────────────────────────────────────────────────────────────

bool PgInstance::write_config(const std::map<std::string, std::string>& settings) {
  std::string content = "\n";
  for (const auto& [k, v] : settings)
    content += k + " = " + v + "\n";
  return append_file(data_dir_ + "/postgresql.conf", content);
}

bool PgInstance::write_hba(const std::vector<std::string>& lines) {
  std::string content;
  for (const auto& line : lines)
    content += line + "\n";
  return append_file(data_dir_ + "/pg_hba.conf", content);
}

// ── process control ───────────────────────────────────────────────────────────

bool PgInstance::start() {
  return postgres_.start();
}

void PgInstance::stop(StopMode mode, int timeout_ms) {
  if (!postgres_.is_running()) return;

  int sig;
  switch (mode) {
    case StopMode::Smart:     sig = SIGTERM; break;
    case StopMode::Fast:      sig = SIGINT;  break;
    case StopMode::Immediate: sig = SIGQUIT; break;
  }
  ::kill(postgres_.pid(), sig);

  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    if (postgres_.try_wait().has_value()) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  postgres_.kill();
  postgres_.wait();
}

void PgInstance::reload() {
  if (postgres_.is_running())
    ::kill(postgres_.pid(), SIGHUP);
}

void PgInstance::promote() {
  if (postgres_.is_running())
    ::kill(postgres_.pid(), SIGUSR1);
}

bool PgInstance::is_running() const {
  return postgres_.is_running();
}

bool PgInstance::wait_ready(const std::string& host, uint16_t port,
                             int timeout_s) const {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::seconds(timeout_s);
  while (std::chrono::steady_clock::now() < deadline) {
    Process p(pg_bin_ + "/pg_isready",
              {"-h", host, "-p", std::to_string(port), "-d", "postgres", "-q"});
    p.start();
    if (p.wait() == 0) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
  return false;
}

// ── backup / restore ──────────────────────────────────────────────────────────

bool PgInstance::backup(const std::string& dest_dir, StopMode mode) {
  if (is_running()) stop(mode);
  return copy_dir(data_dir_, dest_dir);
}

bool PgInstance::restore(const std::string& backup_dir) {
  return copy_dir(backup_dir, data_dir_);
}

bool PgInstance::restore_and_start(const std::string& backup_dir) {
  return restore(backup_dir) && start();
}

// ── private helpers ───────────────────────────────────────────────────────────

bool PgInstance::copy_dir(const std::string& src, const std::string& dst) {
  // -R: recursive, -p: preserve mode/timestamps.  When dst does not exist,
  // cp creates it as a replica of src (not as a subdirectory inside it).
  Process p("/bin/cp", {"-Rp", src, dst});
  return p.start() && p.wait() == 0;
}

bool PgInstance::append_file(const std::string& path,
                              const std::string& content) {
  std::ofstream f(path, std::ios::app);
  f << content;
  return f.good();
}
