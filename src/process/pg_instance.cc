#include "src/process/pg_instance.h"

#include <dirent.h>
#include <errno.h>
#include <signal.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <thread>

#include "src/base/logger.h"

void PgInstance::PostgresProcess::on_stderr_line(std::string_view line) {
  // Find the first all-uppercase word immediately followed by ':'. In a
  // PostgreSQL log line the severity label (LOG, ERROR, FATAL, …) is always
  // the first such token, appearing before the message body.
  for (std::size_t colon = line.find(':');
       colon != std::string_view::npos;
       colon = line.find(':', colon + 1)) {
    std::size_t end = colon, start = end;
    while (start > 0 && (std::isupper(static_cast<unsigned char>(line[start - 1])) ||
                         std::isdigit(static_cast<unsigned char>(line[end - 1]))))
      --start;
    if (start == end || !std::isupper(static_cast<unsigned char>(line[start]))) continue;
    std::string_view word = line.substr(start, end - start);
    if (word == "PANIC" || word == "FATAL" || word == "ERROR")
      logger::error("{}", line);
    else if (word == "WARNING")
      logger::warn("{}", line);
    else if (word == "NOTICE" || word == "INFO" || word == "LOG")
      logger::info("{}", line);
    else
      logger::debug("{}", line);
    return;
  }

  logger::debug("{}", line);
}

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

bool PgInstance::create_raft_wal() {
  const std::string pg_raft_wal = data_dir_ + "/pg_raft_wal";
  if (mkdir(pg_raft_wal.c_str(), 0700) != 0 && errno != EEXIST)
    return false;

  const std::string pg_wal = data_dir_ + "/pg_wal";
  DIR* dp = opendir(pg_wal.c_str());
  if (!dp) return false;

  bool ok = true;
  struct dirent* de;
  while ((de = readdir(dp)) != nullptr) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
      continue;
    std::string src = pg_wal + "/" + de->d_name;
    std::string dst = pg_raft_wal + "/" + de->d_name;
    if (rename(src.c_str(), dst.c_str()) != 0) { ok = false; break; }
  }
  closedir(dp);
  return ok;
}

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
  Process p("/usr/bin/rsync", {"-a", "--exclude=pg_raft_wal", data_dir_ + "/", dest_dir});
  return p.start() && p.wait() == 0;
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
