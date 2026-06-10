#include "src/process/pg_instance.h"

// ── helpers ───────────────────────────────────────────────────────────────────

static std::vector<std::string> build_postgres_args(std::string const&              data_dir,
                                                    std::vector<std::string> const& extra) {
  std::vector<std::string> args = {"-D", data_dir};
  args.insert(args.end(), extra.begin(), extra.end());
  return args;
}

static bool has_arg(std::vector<std::string> const& args, std::string_view arg) {
  return std::accumulate(
      std::begin(args), std::end(args), false, [arg](bool result, auto const& elem) {
        return result || elem == arg;
      });
}

// ── PgInstance ────────────────────────────────────────────────────────────────

PgInstance::PgInstance(std::filesystem::path    pg_bin,
                       std::filesystem::path    data_dir,
                       std::vector<std::string> extra_args)
    : pg_bin_(std::move(pg_bin)),
      data_dir_(std::move(data_dir)),
      extra_args_(std::move(extra_args)),
      postgres_(pg_bin_ / "postgres", build_postgres_args(data_dir_, extra_args_)) {}

PgInstance::~PgInstance() {
  if (is_running())
    stop();
}

// ── initialisation ────────────────────────────────────────────────────────────

bool PgInstance::create_raft_wal(std::filesystem::path raft_dir) {
  std::filesystem::path const pg_raft_wal = raft_dir / "pg_wal";
  if (mkdir(pg_raft_wal.c_str(), 0700) != 0 && errno != EEXIST)
    return false;

  std::filesystem::path const pg_wal = data_dir_ / "pg_wal";
  DIR*                        dp     = opendir(pg_wal.c_str());
  if (!dp)
    return false;

  bool           ok = true;
  struct dirent* de;
  while ((de = readdir(dp)) != nullptr) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
      continue;
    std::filesystem::path src = pg_wal / de->d_name;
    std::filesystem::path dst = pg_raft_wal / de->d_name;
    if (rename(src.c_str(), dst.c_str()) != 0) {
      ok = false;
      break;
    }
  }
  closedir(dp);
  return ok;
}

bool PgInstance::init(std::vector<std::string> initdb_args) {
  if (!(has_arg(initdb_args, "-k") || has_arg(initdb_args, "--data-checksums")))
    initdb_args.emplace_back("--data-checksums");

  std::vector<std::string> args = {"-D", data_dir_};
  args.insert(args.end(), initdb_args.begin(), initdb_args.end());
  Process p(pg_bin_ / "initdb", std::move(args));
  return p.start() && p.wait() == 0;
}

// ── configuration ─────────────────────────────────────────────────────────────

static std::map<std::string, std::string> const kDefaultConfig = {
    {"wal_sync_method", "fsync"},
    {"wal_recycle", "off"},
    {"listen_addresses", "'*'"},
    {"wal_keep_size", "5GB"},
};

static std::vector<std::string> const kDefaultHbaConfig = {
    // TYPE  DATABASE        USER            ADDRESS                 METHOD
    // Allow all IPv4 connections
    "host    all             all             0.0.0.0/0               scram-sha-256",
    // Allow all IPv6 connections
    "host    all             all             ::/0                    scram-sha-256",
};

bool PgInstance::write_config(std::map<std::string, std::string> const& settings) {
  std::map<std::string, std::string> merged = kDefaultConfig;
  for (auto const& [k, v] : settings)
    merged[k] = v;

  std::string content = "\n";
  for (auto const& [k, v] : merged)
    content += k + " = " + v + "\n";
  return append_file(data_dir_ / "postgresql.conf", content);
}

bool PgInstance::write_hba(std::vector<std::string> const& lines) {
  std::string content;
  for (auto const& line : lines)
    content += line + "\n";
  for (auto const& line : kDefaultHbaConfig)
    content += line + "\n";
  return append_file(data_dir_ / "pg_hba.conf", content);
}

// ── process control ───────────────────────────────────────────────────────────

bool PgInstance::start() {
  return postgres_.start();
}

void PgInstance::stop(StopMode mode, int timeout_ms) {
  if (!postgres_.is_running())
    return;

  int sig;
  switch (mode) {
    case StopMode::Smart:
      sig = SIGTERM;
      break;
    case StopMode::Fast:
      sig = SIGINT;
      break;
    case StopMode::Immediate:
      sig = SIGQUIT;
      break;
  }
  ::kill(postgres_.pid(), sig);

  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    if (postgres_.try_wait().has_value())
      return;
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

bool PgInstance::wait_ready(uint16_t port, int timeout_s) const {
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
  while (std::chrono::steady_clock::now() < deadline) {
    Process p(pg_bin_ / "pg_isready",
              {"-h", "localhost", "-p", std::to_string(port), "-d", "postgres", "-q"});
    p.start();
    if (p.wait() == 0)
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
  return false;
}

// ── backup / restore ──────────────────────────────────────────────────────────

bool PgInstance::backup(std::filesystem::path const& dest_dir, StopMode mode) {
  if (is_running())
    stop(mode);
  return copy_dir(data_dir_, dest_dir);
}

bool PgInstance::restore(std::filesystem::path const& backup_dir) {
  return copy_dir(backup_dir, data_dir_);
}

bool PgInstance::restore_and_start(std::filesystem::path const& backup_dir) {
  return restore(backup_dir) && start();
}

// ── private helpers ───────────────────────────────────────────────────────────

bool PgInstance::copy_dir(std::filesystem::path const& src, std::filesystem::path const& dst) {
  // -a: archive (recursive + preserve permissions/timestamps/symlinks).
  // Trailing slash on src copies contents into dst, not src itself into dst.
  Process p("/usr/bin/rsync", {"-a", src.string() + "/", dst.string()});
  return p.start() && p.wait() == 0;
}

bool PgInstance::append_file(std::filesystem::path const& path, std::string const& content) {
  std::ofstream f(path, std::ios::app);
  f << content;
  return f.good();
}
