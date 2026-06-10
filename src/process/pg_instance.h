#pragma once

#include "src/base/logger.h"
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

  // Poll pg_isready on |localhost|:|port| until it returns OK or timeout elapses.
  bool wait_ready(uint16_t port, int timeout_s = 30) const;

  // ── backup / restore ──────────────────────────────────────────────────────

  // Stop the instance (if running), then copy data_dir recursively to
  // |dest_dir|. |dest_dir| must not exist.
  bool backup(std::filesystem::path const& dest_dir, StopMode mode = StopMode::Fast);

  // Copy |backup_dir| recursively to data_dir_ (data_dir_ must not exist).
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

  std::filesystem::path pg_bin_;
  std::filesystem::path data_dir_;
  uint16_t              port_;
  Role                  role_;

  mutable std::recursive_mutex mu_;

  struct PostgresProcess : public Process {
    using Process::Process;
    void on_stdout_line(std::string_view line) override { logger::plog("pgres", line); }
    void on_stderr_line(std::string_view line) override { logger::plog("pgres", line); }
  };
  PostgresProcess postgres_;
};
