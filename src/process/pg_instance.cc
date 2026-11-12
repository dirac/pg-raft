#include "src/process/pg_instance.h"

#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>

#include <fstream>
#include <sstream>

// ── helpers ───────────────────────────────────────────────────────────────────

static std::vector<std::string> build_postgres_args(std::string const& data_dir, uint16_t port) {
  return {"-D", data_dir, "-p", std::to_string(port)};
}

static bool has_arg(std::vector<std::string> const& args, std::string_view arg) {
  return std::accumulate(
      std::begin(args), std::end(args), false, [arg](bool result, auto const& elem) {
        return result || elem == arg;
      });
}

// ── PgInstance ────────────────────────────────────────────────────────────────

PgInstance::PgInstance(std::filesystem::path pg_bin,
                       std::filesystem::path data_dir,
                       uint16_t              port,
                       Role                  role)
    : pg_bin_(std::move(pg_bin)),
      data_dir_(std::move(data_dir)),
      port_(port),
      role_(role),
      postgres_(pg_bin_ / "postgres", build_postgres_args(data_dir_, port_)) {}

PgInstance::~PgInstance() {
  if (is_running())
    stop();
}

// ── initialisation ────────────────────────────────────────────────────────────

bool PgInstance::create_raft_wal(std::filesystem::path raft_dir) {
  std::lock_guard             lk(mu_);
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
  std::lock_guard lk(mu_);
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
    {"wal_level", "replica"},
    {"listen_addresses", "'*'"},
    {"fsync", "on"},
    {"synchronous_commit", "on"},
};

static std::vector<std::string> const kDefaultHbaConfig = {
    // TYPE  DATABASE        USER            ADDRESS                 METHOD
    // Allow all IPv4 connections
    "host    all             all             0.0.0.0/0               scram-sha-256",
    // Allow all IPv6 connections
    "host    all             all             ::/0                    scram-sha-256",
    // Allow and trust local connections
    "host    all             all              127.0.0.1/32           trust",
    "host    all             all             ::1/128                 trust",

};

bool PgInstance::write_config(std::map<std::string, std::string> const& settings) {
  std::lock_guard                    lk(mu_);
  std::map<std::string, std::string> merged = kDefaultConfig;
  for (auto const& [k, v] : settings)
    merged[k] = v;

  std::string content = "\n";
  for (auto const& [k, v] : merged)
    content += k + " = " + v + "\n";
  return append_file(data_dir_ / "postgresql.conf", content);
}

bool PgInstance::write_hba(std::vector<std::string> const& lines) {
  std::lock_guard lk(mu_);
  std::string     content;
  for (auto const& line : lines)
    content += line + "\n";
  for (auto const& line : kDefaultHbaConfig)
    content += line + "\n";
  return append_file(data_dir_ / "pg_hba.conf", content);
}

// ── process control ───────────────────────────────────────────────────────────

bool PgInstance::start() {
  std::lock_guard lk(mu_);
  return postgres_.start();
}

void PgInstance::stop(StopMode mode, int timeout_ms) {
  std::lock_guard lk(mu_);
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

  std::optional<int> code;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (!code && std::chrono::steady_clock::now() < deadline) {
    code = postgres_.try_wait();
    if (!code)
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  if (!code) {
    postgres_.kill();
    code = postgres_.wait();
  }

  // Killed by a signal (kill(), or the SIGKILL above): it could not release its IPC.
  if (*code < 0)
    release_ipc();
}

namespace {

#if !defined(__APPLE__)
union semun {
  int              val;
  struct semid_ds* buf;
  unsigned short*  array;
};
#endif

constexpr int kPGSemaMagic = 537;  // sysv_sema.c

}  // namespace

int PgInstance::release_ipc() {
  std::lock_guard lk(mu_);

  std::ifstream            in(data_dir_ / "postmaster.pid");
  std::vector<std::string> lines;
  for (std::string line; std::getline(in, line);)
    lines.push_back(line);
  if (lines.size() < 7)
    return 0;

  pid_t postmaster = 0;
  long  key        = 0;
  int   id         = -1;
  try {
    postmaster = static_cast<pid_t>(std::stol(lines.at(0)));
    std::istringstream ss(lines.at(6));
    ss >> key >> id;
  } catch (std::exception const&) {
    return 0;
  }
  if (postmaster <= 0 || key == 0 || ::kill(postmaster, 0) == 0)
    return 0;  // not a killed postmaster's file, or it still runs

  int removed = 0;

  // The segment: still the one postmaster.pid names, and nothing attached (no backend alive).
  if (int shmid = ::shmget(static_cast<key_t>(key), 0, 0); shmid >= 0 && shmid == id) {
    struct shmid_ds ds{};
    if (::shmctl(shmid, IPC_STAT, &ds) != 0 || ds.shm_nattch > 0)
      return 0;
    if (::shmctl(shmid, IPC_RMID, nullptr) == 0) {
      log(INFO,
          "Removed the shared memory segment {} of the killed postmaster {}",
          shmid,
          postmaster);
      ++removed;
    }
  }

  // The semaphore sets: PostgreSQL takes keys from the data directory's inode (the segment's key)
  // on, skipping those in use.
  for (long k = key; k < key + 256; ++k) {
    int semid = ::semget(static_cast<key_t>(k), 0, 0);
    if (semid < 0)
      continue;
    struct semid_ds ds{};
    union semun     arg{};
    arg.buf = &ds;
    if (::semctl(semid, 0, IPC_STAT, arg) != 0 || ds.sem_nsems < 1)
      continue;
    int const magic = static_cast<int>(ds.sem_nsems) - 1;
    if (::semctl(semid, magic, GETVAL) != kPGSemaMagic ||
        ::semctl(semid, magic, GETPID) != postmaster)
      continue;
    if (::semctl(semid, 0, IPC_RMID) == 0)
      ++removed;
  }
  if (removed > 0)
    log(INFO, "Released {} System V IPC objects of the killed postmaster {}", removed, postmaster);
  return removed;
}

void PgInstance::reload() {
  std::lock_guard lk(mu_);
  if (postgres_.is_running())
    ::kill(postgres_.pid(), SIGHUP);
}

void PgInstance::kill() {
  pid_t const postmaster = postgres_.pid();
  if (postmaster <= 0)
    return;

  ::kill(postmaster, SIGSTOP);
  for (pid_t child : child_pids(postmaster)) {
    ::kill(-child, SIGKILL);  // its group: whatever it forked (archive_command, ...)
    ::kill(child, SIGKILL);
  }
  ::kill(postmaster, SIGKILL);
}

pid_t PgInstance::pid() const {
  return postgres_.pid();
}

void PgInstance::promote() {
  std::lock_guard lk(mu_);
  if (postgres_.is_running()) {
    // What pg_ctl promote does: the postmaster promotes on SIGUSR1 only if the promote file
    // exists (PostgreSQL 12+). The promotion itself is asynchronous: wait for
    // pg_is_in_recovery() to be false.
    std::ofstream(data_dir_ / "promote").close();
    ::kill(postgres_.pid(), SIGUSR1);
    role_ = Role::kPrimary;
  }
}

PgInstance::Role PgInstance::role() const {
  std::lock_guard lk(mu_);
  return role_;
}

bool PgInstance::is_running() const {
  std::lock_guard lk(mu_);
  return postgres_.is_running();
}

bool PgInstance::wait_ready(uint16_t port, int timeout_s) const {
  std::lock_guard lk(mu_);
  auto            deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
  while (std::chrono::steady_clock::now() < deadline) {
    Process p(pg_bin_ / "pg_isready",
              {"-h", "localhost", "-p", std::to_string(port), "-d", "postgres", "-q"});
    p.start();
    if (p.wait() == 0) {
      // A server answers on |port|: it must be this one, not another PostgreSQL that holds the
      // port while ours failed to bind it. postmaster.pid names our postmaster on its first line,
      // and its 8th line is the postmaster's status: "ready" (or "standby" for a hot standby)
      // once it accepts connections. One that fails to bind its port writes the file, then exits
      // without reaching either.
      std::ifstream            in(data_dir_ / "postmaster.pid");
      std::vector<std::string> lines;
      for (std::string line; std::getline(in, line);)
        lines.push_back(line);
      auto const ours =
          !lines.empty() && postgres_.pid() > 0 && lines.at(0) == std::to_string(postgres_.pid());
      auto const ready = lines.size() >= 8 &&
                         (lines.at(7).starts_with("ready") || lines.at(7).starts_with("standby"));
      if (ours && ready)
        return true;
      if (!ours) {
        log(ERROR,
            "A server answers on port {}, but it is not PostgreSQL {} of {}",
            port,
            postgres_.pid(),
            data_dir_);
        return false;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
  return false;
}

// ── backup / restore ──────────────────────────────────────────────────────────

SemaphoreLock PgInstance::backup(std::filesystem::path const& dest_dir) {
  SemaphoreLock lk{backup_sem_};
  if (!lk) {
    lk.acquire();
    return lk;
  }
  std::lock_guard guard(mu_);

  // A pg_basebackup on a primary or a standby alike: the instance keeps running, and the backup has
  // a backup_manifest (its End-LSN names the backup in the store, docs/backup.md).
  std::filesystem::remove_all(dest_dir);
  Process p(pg_bin_ / "pg_basebackup",
            {"-h",
             "127.0.0.1",
             "-p",
             std::to_string(port_),
             "-D",
             dest_dir,
             "--checkpoint=fast",
             "-X",
             "stream"});
  bool    ok = p.start() && p.wait() == 0;
  if (!ok)
    lk.release();
  return lk;
}

std::map<std::string, std::string> PgInstance::controldata(std::filesystem::path const& dir) const {
  struct CapProc : public Process, public std::map<std::string, std::string> {
    using Process::Process;

    void on_stdout_line(std::string_view line) override {
      Process::on_stdout_line(line);
      if (auto pos = line.find(':'); pos != std::string_view::npos) {
        auto key = std::string{line.substr(0, pos)};
        auto val = std::string{line.substr(pos + 1)};

        while (!val.empty() && std::isspace(val.at(0)))
          val.erase(val.begin());

        emplace(key, val);
      }
    }
  };
  CapProc cp(pg_bin_ / "pg_controldata", {dir});
  cp.start();
  cp.wait();

  for (auto k : {
           "Database cluster state",
           "Database system identifier",
       })
    if (cp.find(k) == cp.end())
      return {};

  return cp;
}

SemaphoreLock PgInstance::verify_backup(std::filesystem::path const& backup_dir,
                                        SemaphoreLock                lk) const {
  if (!lk)
    return lk;
  auto server = controldata(data_dir_);
  auto backup = controldata(backup_dir);

  if (server.empty() || backup.empty()) {
    lk.release();
    return lk;
  }
  if (server["Database system identifier"] != backup["Database system identifier"]) {
    lk.release();
    return lk;
  }

  auto cluster_state = backup["Database cluster state"];

  auto run = [](std::filesystem::path bin, std::vector<std::string> args) {
    Process p(std::move(bin), std::move(args));
    return p.start() && p.wait() == 0;
  };

  bool integrity_ok;
  if (cluster_state == "shut down" || cluster_state == "shut down in recovery")
    integrity_ok = run(pg_bin_ / "pg_checksums", {"-D", backup_dir, "--check"});
  else
    integrity_ok = run(pg_bin_ / "pg_verifybackup", {backup_dir});
  if (!integrity_ok)
    lk.release();
  return lk;
}

bool PgInstance::restore(std::filesystem::path const& backup_dir) {
  std::lock_guard lk(mu_);

  // Keep the directory (its inode), replace its contents.
  std::error_code ec;
  if (std::filesystem::exists(data_dir_, ec)) {
    for (auto const& entry : std::filesystem::directory_iterator(data_dir_, ec))
      std::filesystem::remove_all(entry.path(), ec);
    if (ec)
      return false;
  }
  return copy_dir(backup_dir, data_dir_);
}

bool PgInstance::restore_and_start(std::filesystem::path const& backup_dir) {
  std::lock_guard lk(mu_);
  return restore(backup_dir) && postgres_.start();
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
