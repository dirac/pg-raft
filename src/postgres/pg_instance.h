#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "src/process/process.h"

// Manages the lifecycle of a single PostgreSQL data directory: initialisation,
// configuration, start/stop/reload/promote, backup, and restore.
class PgInstance {
 public:
  // Maps to the three PostgreSQL shutdown modes (SIGTERM / SIGINT / SIGQUIT).
  enum class StopMode { Smart, Fast, Immediate };

  // |pg_bin|    – directory containing postgres, initdb, pg_isready, etc.
  // |data_dir|  – PGDATA path (need not exist yet; created by init()).
  // |extra_args| – extra arguments forwarded to the postgres process.
  PgInstance(std::string pg_bin, std::string data_dir,
             std::vector<std::string> extra_args = {});
  ~PgInstance();

  PgInstance(const PgInstance&) = delete;
  PgInstance& operator=(const PgInstance&) = delete;

  // ── initialisation ────────────────────────────────────────────────────────

  // Run initdb to create a fresh data directory. |initdb_args| are appended
  // verbatim to `initdb -D <data_dir>`.
  bool init(std::vector<std::string> initdb_args = {});

  // ── configuration ─────────────────────────────────────────────────────────

  // Append key = value pairs to postgresql.conf.
  bool write_config(const std::map<std::string, std::string>& settings);

  // Append lines verbatim to pg_hba.conf.
  bool write_hba(const std::vector<std::string>& lines);

  // ── process control ───────────────────────────────────────────────────────

  bool start();

  // Send the shutdown signal for |mode| and wait up to |timeout_ms|.
  // Falls back to SIGKILL if the process has not exited by the deadline.
  void stop(StopMode mode = StopMode::Fast, int timeout_ms = 10000);

  // SIGHUP — reload config files without a full restart.
  void reload();

  // SIGUSR1 — promote a standby to primary (PostgreSQL 12+).
  void promote();

  bool is_running() const;

  // Poll pg_isready on |host|:|port| until it returns OK or timeout elapses.
  bool wait_ready(const std::string& host, uint16_t port,
                  int timeout_s = 30) const;

  // ── backup / restore ──────────────────────────────────────────────────────

  // Stop the instance (if running), then copy data_dir recursively to
  // |dest_dir|. |dest_dir| must not exist.
  bool backup(const std::string& dest_dir, StopMode mode = StopMode::Fast);

  // Copy |backup_dir| recursively to data_dir_ (data_dir_ must not exist).
  bool restore(const std::string& backup_dir);

  // restore() then start().
  bool restore_and_start(const std::string& backup_dir);

  // ── accessors ─────────────────────────────────────────────────────────────

  const std::string& data_dir() const { return data_dir_; }
  const std::string& pg_bin()   const { return pg_bin_; }

 private:
  static bool copy_dir(const std::string& src, const std::string& dst);
  static bool append_file(const std::string& path, const std::string& content);

  std::string pg_bin_;
  std::string data_dir_;
  std::vector<std::string> extra_args_;
  Process postgres_;
};
