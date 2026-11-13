#pragma once

#include "src/base/base.h"
#include "src/base/logger.h"

///
/// The configuration used by the system. The configuration is specified in
/// [toml](https://toml.io/en/)
///
struct Config {
  /// The temp, data and raft directories will reside as subdirs.
  string wdir;

  /// All the postgresql installations. Current and future.
  string install_dir = "/opt/pg_raft/postgresql";

  /// The desired pg_version. If the data directory belongs to a previous version, then the leader
  /// is going to upgrade.
  unsigned pg_major_version = 17;

  struct Server {
    string hostname;

    uint16_t pg_port{0};  ///< postgresql port.
    uint16_t ha_port{0};  ///< Raft Port.
  };
  vector<Server> servers;  ///< Postgres servers

  /// Values in postgresql.conf
  map<string, string> pg_config;

  /// The backup store: a directory (DirBackupStore); an s3:// URL later (docs/backup.md).
  string backup_store;

  /// When a standby takes the next backup (docs/backup.md §4).
  unsigned backup_interval_secs        = 6U * 3600U;
  uint64_t backup_wal_size             = uint64_t{4} << 30U;
  unsigned backup_pending_timeout_secs = 120;

  /// Where the WAL sender's Unix-domain socket goes. Short: macOS limits socket paths to 104
  /// bytes.
  string socket_dir = "/tmp";

  /// Disk syncs on the commit path. All off by default: a commit is durable once a majority has
  /// the WAL in its page cache (written through the mmap), so durability comes from replication;
  /// the kernel writes the pages back on its own. Only a power loss of a majority at once loses
  /// acknowledged commits. Turn them on to measure, or to survive that.
  ///
  /// leader_sync: the leader msyncs PostgreSQL's pg_wal segment and raft's copy before appending
  /// the FlushOp that completes PostgreSQL's fsync.
  bool leader_sync = false;
  /// follower_sync: a follower msyncs the WAL blocks it receives before acknowledging them, and
  /// its standby's own fsync (through FUSE) msyncs too.
  bool follower_sync = false;
  /// log_sync: every commit of the raft log (LMDB) is synced. Off: MDB_NOSYNC, and a background
  /// thread syncs the log within a second.
  bool log_sync = false;

  /// Proxy configuration
  uint16_t proxy_pg_port   = 5432;  // For client connections, that will be forwarded to the leader.
  uint16_t proxy_http_port = 8080;  // Proxy listens to a few admin commands on http port.

  Config() = default;
  Config(string const& filename);
};

/// Parse a toml configuration file.
inline Config config(string const& filename) {
  log(INFO, "Reading config file: {}", filename);
  return Config{filename};
}
