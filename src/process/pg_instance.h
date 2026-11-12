#pragma once

#include "src/base/logger.h"
#include "src/base/semaphore_lock.h"
#include "src/process/process.h"

// Manages the lifecycle of a single PostgreSQL data directory: initialisation,
// configuration, start/stop/reload/promote, backup, and restore.
class PgInstance {
 public:
  // Maps to the three PostgreSQL shutdown modes (SIGTERM / SIGINT / SIGQUIT).
  enum class StopMode { Smart, Fast, Immediate };

  enum class Role { kPrimary, kStandby };

  // |pg_bin|   – directory containing postgres, initdb, pg_isready, etc.
  // |data_dir| – PGDATA path (need not exist yet; created by init()).
  // |port|     – TCP port passed as -p to postgres; 0 means use PostgreSQL's default.
  // |role|     – starting role; defaults to kStandby.
  PgInstance(std::filesystem::path pg_bin,
             std::filesystem::path data_dir,
             uint16_t              port,
             Role                  role = Role::kStandby);
  ~PgInstance();

  PgInstance(PgInstance const&)            = delete;
  PgInstance& operator=(PgInstance const&) = delete;

  // ── initialisation ────────────────────────────────────────────────────────

  // Run initdb to create a fresh data directory. |initdb_args| are appended
  // verbatim to `initdb -D <data_dir>`.
  bool init(std::vector<std::string> initdb_args = {});

  // ── configuration ─────────────────────────────────────────────────────────

  // Append key = value pairs to postgresql.conf.
  bool write_config(std::map<std::string, std::string> const& settings);

  // Append lines verbatim to pg_hba.conf.
  bool write_hba(std::vector<std::string> const& lines);

  // ── process control ───────────────────────────────────────────────────────

  bool start();

  // Send the shutdown signal for |mode| and wait up to |timeout_ms|.
  // Falls back to SIGKILL if the process has not exited by the deadline.
  void stop(StopMode mode = StopMode::Fast, int timeout_ms = 10000);

  // SIGHUP — reload config files without a full restart.
  void reload();

  // SIGUSR1 — promote a standby to primary (PostgreSQL 12+).
  // Updates role() to kPrimary.
  void promote();

  bool is_running() const;

  // SIGKILL to PostgreSQL and all its processes, right now: the postmaster is stopped (SIGSTOP, so
  // it forks nothing more), each child and its process group is killed (every child calls setsid,
  // so a signal to the postmaster's group doesn't reach them), then the postmaster. Takes no lock
  // and doesn't wait, so it is safe from a raft hook (the server lock held) and while another
  // thread is in this instance (wait_ready, a backup). The node calls it when it loses leadership.
  // stop() or start() reaps the postmaster afterwards.
  void kill();

  // The postmaster's pid, or -1.
  pid_t pid() const;

  // Remove the System V IPC that a killed PostgreSQL left: the shared memory segment that
  // postmaster.pid names (key and id on its 7th line), and the semaphore sets it created (macOS;
  // on Linux PostgreSQL uses POSIX semaphores inside its shared memory). Only if the postmaster of
  // postmaster.pid is gone and nothing is attached to the segment (no backend is alive). A
  // semaphore set is removed only if it is PostgreSQL's own, by PostgreSQL's test: its last
  // semaphore holds PGSemaMagic (537) and was last operated on by that postmaster. stop() calls
  // it when it reaps a postmaster that a signal killed. Returns the number of objects removed.
  int release_ipc();

  // Poll pg_isready on |localhost|:|port| until it returns OK or timeout elapses.
  bool wait_ready(uint16_t port, int timeout_s = 30) const;

  // ── backup / restore ──────────────────────────────────────────────────────

  // Perform a backup to |dest_dir|. At most one backup runs at a time (guarded
  // by a binary semaphore). If a backup is already in progress, blocks until it
  // finishes and returns an owning SemaphoreLock without re-running the backup.
  // On success the returned lock owns the semaphore and evaluates to true.
  // On error the lock is released and evaluates to false.
  //
  // Primary: runs pg_basebackup (instance stays running).
  // Standby: stops the instance and copies data_dir.
  SemaphoreLock backup(std::filesystem::path const& dest_dir);

  // Verify a backup directory. Runs pg_controldata on both data_dir_ and
  // |backup_dir|, then selects the appropriate integrity tool based on the
  // backup type detected from pg_controldata output:
  //   standby backup (shut down state)  → pg_checksums --check
  //   hot backup (in production state)  → pg_verifybackup (PostgreSQL 13+)
  // Takes the lock returned by backup(). Returns the lock as-is on success;
  // releases it and returns a non-owning lock on any verification failure.
  SemaphoreLock verify_backup(std::filesystem::path const& backup_dir, SemaphoreLock lk) const;

  // Copy |backup_dir| into data_dir_. If data_dir_ exists, its contents are removed first but the
  // directory itself is kept: PostgreSQL derives its System V shared memory and semaphore keys
  // from the data directory's inode, so the next postmaster finds and removes what a killed one
  // left. A new directory would leak them until reboot.
  bool restore(std::filesystem::path const& backup_dir);

  // restore() then start().
  bool restore_and_start(std::filesystem::path const& backup_dir);

  // Create |raft_dir|/pg_wal and move the WAL segments out of
  // data_dir/pg_wal into it, leaving pg_wal as an empty directory ready
  // for a FUSE mount. Must be called after init() and before start().
  bool create_raft_wal(std::filesystem::path raft_dir);

  // ── accessors ─────────────────────────────────────────────────────────────

  std::filesystem::path const& data_dir() const { return data_dir_; }
  std::filesystem::path const& pg_bin() const { return pg_bin_; }
  Role                         role() const;

 private:
  static bool copy_dir(std::filesystem::path const& src, std::filesystem::path const& dst);
  static bool append_file(std::filesystem::path const& path, std::string const& content);

  std::map<std::string, std::string> controldata(std::filesystem::path const& dir) const;

  std::filesystem::path pg_bin_;
  std::filesystem::path data_dir_;
  uint16_t              port_;
  Role                  role_;

  mutable std::recursive_mutex mu_;
  std::binary_semaphore        backup_sem_{1};

  struct PostgresProcess : public Process {
    using Process::Process;
    void on_stdout_line(std::string_view line) override { logger::plog("pgres", line); }
    void on_stderr_line(std::string_view line) override { logger::plog("pgres", line); }
  };
  PostgresProcess postgres_;
};
