#pragma once

#include "src/base/base.h"

// The cluster's PostgreSQL backups (docs/backup.md).
//
// Every backup is a pg_basebackup. A backup is named after what a node needs to schedule the next
// one and to bound WAL retention, so that a listing is enough:
//
//   TTTTTTTT-XXXXXXXXXXXXXXXX-SSSSSSSSSS-N
//   timeline  end LSN         start time  node index
//
// The timeline and LSN are fixed-width hex, the time a fixed-width decimal (seconds since the
// epoch) and the node index a decimal, so the names sort as (end LSN, time, node).

/// A complete backup in the store.
struct BackupInfo {
  LogSeqNumber lsn{};   ///< The backup's end LSN: a restore is consistent from here.
  std::time_t  time{};  ///< When the backup was started.
  RaftNodeId   node{};  ///< The index of the node that took it.

  string name() const;

  bool operator==(BackupInfo const&) const = default;
  auto operator<=>(BackupInfo const& that) const {
    if (auto c = std::tuple{lsn.timeline, lsn.lsn} <=> std::tuple{that.lsn.timeline, that.lsn.lsn};
        c != 0)
      return c;
    if (auto c = time <=> that.time; c != 0)
      return c;
    return INT(node) <=> INT(that.node);
  }
};

/// Parse a backup name; nullopt if |name| is not one.
std::optional<BackupInfo> parse_backup_name(std::string_view name);

/// A backup that a node has started and not yet stored or aborted: a lease that the node refreshes
/// while the backup runs, so that a node that dies doesn't block the others (docs/backup.md §4.2).
/// At most one per node.
struct PendingBackup {
  RaftNodeId  node{};
  std::time_t started{};    ///< When the backup was started: the time of its name.
  std::time_t refreshed{};  ///< When the node last refreshed the lease.

  bool operator==(PendingBackup const&) const = default;
};

/// Where the backups are stored. DirBackupStore keeps them in a directory; an S3 store comes later.
class BackupStore {
 public:
  virtual ~BackupStore() = default;

  /// Announce a backup that |node| starts at |time|, before it runs pg_basebackup. Replaces any
  /// lease, and removes any partial copy, that |node| left.
  virtual Result<PendingBackup> begin(RaftNodeId node, std::time_t time) = 0;

  /// Refresh the lease while the backup runs.
  virtual Result<void> refresh(PendingBackup& pending, std::time_t time) = 0;

  /// Store the backup in |dir| (a data directory with its pg_wal) as
  /// (|lsn|, |pending.started|, |pending.node|), then release the lease. Atomic: list() and
  /// latest() never return a backup that is only partly stored.
  virtual Result<BackupInfo> put(PendingBackup const&         pending,
                                 std::filesystem::path const& dir,
                                 LogSeqNumber                 lsn) = 0;

  /// Release the lease of |node| and remove its partial copy, if any: a backup that failed, or
  /// one a restarted node left behind.
  virtual Result<void> abort(RaftNodeId node) = 0;

  /// The leases, live or stale (see pending_blocks()).
  virtual Result<vector<PendingBackup>> pending() const = 0;

  /// The complete backups, in order (oldest first).
  virtual Result<vector<BackupInfo>> list() const = 0;

  /// The latest backup, or nullopt if the store is empty.
  Result<std::optional<BackupInfo>> latest() const;

  /// Copy |backup| into |dest|, which must not exist or be empty.
  virtual Result<void> get(BackupInfo const& backup, std::filesystem::path const& dest) const = 0;

  virtual Result<void> remove(BackupInfo const& backup) = 0;
};

// ── Scheduling (docs/backup.md §4) ──────────────────────────────────────────────────────────────

struct BackupPolicy {
  std::chrono::seconds interval{std::chrono::hours{6}};  ///< backup_interval
  uint64_t             wal_size{4ULL << 30U};            ///< backup_wal_size, in bytes
  std::chrono::seconds pending_timeout{std::chrono::minutes{2}};
};

/// Whether node |index| of |nodes| should start a backup: once interval * (1 + index/nodes) has
/// passed since |latest| started, or wal_size * (1 + index/nodes) of WAL has been written since
/// its end LSN (|wal_end| is RaftState::wal_end). The deadlines are staggered so that the next
/// node covers one that is down. Without a backup there is nothing to measure from (init makes
/// the first one), so it is never due. The caller checks the rest: not the primary, no live lease
/// (pending_blocks()).
bool backup_due(BackupPolicy const&              policy,
                unsigned                         index,
                unsigned                         nodes,
                std::optional<BackupInfo> const& latest,
                std::time_t                      now,
                LogSeqNumber                     wal_end);

/// Whether a lease of another node than |self| is live: refreshed less than pending_timeout ago.
bool pending_blocks(BackupPolicy const&          policy,
                    vector<PendingBackup> const& pending,
                    RaftNodeId                   self,
                    std::time_t                  now);

/// The start of the segment that holds |backup|'s end LSN: raft keeps its entries, and the WAL,
/// from here on (docs/backup.md §5).
LogSeqNumber keep_from(BackupInfo const& backup);

template <>
struct std::formatter<BackupInfo> : public parse_fn {
  auto format(BackupInfo const& b, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "{}", b.name());
  }
};
