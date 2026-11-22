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
  unsigned pg_major_version = 18;

  struct Server {
    string hostname;

    uint16_t pg_port{0};  ///< postgresql port.
    uint16_t ha_port{0};  ///< Raft Port.
  };
  vector<Server> servers;  ///< Postgres servers

  /// Values in postgresql.conf
  map<string, string> pg_config;

  /// Lines for pg_hba.conf, written by pg_raft_initdb ahead of pg-raft's defaults (which trust
  /// only the loopback and ask other addresses for a scram-sha-256 password), e.g.
  /// "host all all 10.0.0.0/16 trust" for a proxy or clients on other machines.
  vector<string> pg_hba;

  /// The backup store: a directory (DirBackupStore); an s3:// URL later (docs/backup.md).
  string backup_store;

  /// When a standby takes the next backup (docs/backup.md §4).
  unsigned backup_interval_secs        = 6U * 3600U;
  uint64_t backup_wal_size             = uint64_t{4} << 30U;
  unsigned backup_pending_timeout_secs = 120;

  /// Where the WAL sender's Unix-domain socket goes. Short: macOS limits socket paths to 104
  /// bytes.
  string socket_dir = "/tmp";

  /// The disk syncs on the commit path, of the raft log (the ledger, docs/ledger.md), off by
  /// default: a commit is durable once a majority has its ledger records in the page cache, so
  /// durability comes from replication; the ledger is synced in the background within a second.
  /// Only a power loss of a majority at once loses acknowledged commits. Turn them on to measure,
  /// or to survive that. PostgreSQL's own pg_wal is never synced: raft keeps the committed WAL
  /// (the ledger, and raft/wal derived from it), and a standby streams again what its pg_wal lost.
  ///
  /// leader_sync: the leader counts itself in the quorum only once its ledger has the entry on
  /// disk (its fdatasync runs during the network round).
  bool leader_sync = false;
  /// follower_sync: a follower fdatasyncs its ledger before it acknowledges an AppendEntries.
  bool follower_sync = false;
  /// write_op_bytes: on the leader, the WAL written since the last flush is sent with the next
  /// flush (one raft entry per fsync), or as soon as this much of a segment is pending (a WriteOp
  /// of its own), so that a large transaction streams its WAL before its commit.
  uint64_t write_op_bytes = 64 * 1024;
  /// commit_wait: what a commit on the primary waits for the quorum in.
  ///  "fsync"    PostgreSQL's WAL fsync (through FUSE) returns when the flush is committed by raft:
  ///             nothing is visible before a quorum has it. The flushes, and so the raft rounds,
  ///             are serial (WALWriteLock).
  ///  "sync_rep" the fsync returns once the flush is in the leader's ledger, and the commit waits in
  ///             PostgreSQL's synchronous replication for the node (application_name pg_raft), which
  ///             acknowledges the raft-committed WAL: rounds overlap, but a commit whose wait is
  ///             cancelled is visible before a quorum has it. For measurement.
  string commit_wait = "fsync";
  /// wal_capture: how the node sees the WAL its PostgreSQL writes as a primary.
  ///  "fuse"  a FUSE mount over pg_wal (stock PostgreSQL).
  ///  "hook"  PostgreSQL's WAL hooks (pg-raft's patch of XLogWrite: external/patch_files) and the
  ///          pg_raft_wal extension, over a Unix socket: needs that build (install_dir; built by
  ///          external/build-pg-raft-postgresql.sh). pg_wal is a plain directory (a link to pg/pg_wal).
  string wal_capture = "fuse";
  /// io_threads: the node's io_context threads (connections other than raft's leader-to-follower
  /// ones, which read on threads of their own; write completions, timers, the client channel).
  unsigned io_threads = 2;
  /// frame_checksums: xxhash32 of every network frame raft sends (TCP checksums the segments
  /// already; the ledger's records carry a CRC-32C of their own, always).
  bool frame_checksums = false;
  /// ledger_file_size: the size of each ledger file. A power of 2, larger than 32 MiB (a ledger
  /// file outgrows a 16 MiB WAL segment, an InstallSnapshot's two), at most 2 GiB (a file's
  /// written offset is 32 bits): 64 MiB, 128 MiB, ..., 2 GiB.
  uint64_t ledger_file_size = uint64_t{1} << 30U;

  /// Whether |size| is a valid ledger_file_size.
  static bool valid_ledger_file_size(uint64_t size) {
    return std::has_single_bit(size) && size > (uint64_t{32} << 20U) && size <= (uint64_t{2} << 30U);
  }

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
