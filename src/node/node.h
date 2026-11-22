#pragma once

#include "src/backup/backup_store.h"
#include "src/base/config.h"
#include "src/node/node_wal_tracker.h"
#include "src/node/sync_acker.h"
#include "src/node/wal_hook_server.h"
#include "src/process/pg_instance.h"
#include "src/raft/raft_server.h"
#include "src/wal_fuse/fuse.h"
#include "src/wal_sender/wal_sender_server.h"

// A pg-raft node (docs/node.md): one RaftServer, one PostgreSQL, the FUSE mount over its pg_wal,
// and the WalSenderServer its standby streams from.
//
//   wdir/node<N>/
//     data/        PGDATA (data/pg_wal is the FUSE mount point)
//     pg/pg_wal/   the FUSE backing directory: the WAL PostgreSQL itself writes
//     raft/        the raft log
//     raft/wal/    raft's wal_dir: the replicated WAL, which the WAL sender streams
//
// start() restores the latest backup if the node has no usable data directory, mounts FUSE, and
// starts PostgreSQL as a standby of the local WAL sender, raft, and the status poller. Raft decides
// the role:
//  - became_leader: the transition thread streams the standby everything in raft/wal (up to
//    wal_end), switches the tracker to leader mode, promotes PostgreSQL, waits for its timeline to
//    be committed, then set_serving(true).
//  - stepped_down: in the hook itself, the tracker is fenced and PostgreSQL is killed; the
//    transition thread then reaps it, rebuilds pg/pg_wal from raft/wal (rebuild_pg_wal), and
//    restarts it as a standby. A promotion that finds PostgreSQL killed does that first.

struct NodeOptions {
  Options                   raft;
  std::chrono::milliseconds poll_interval{500};
  std::chrono::seconds      promote_timeout{60};  // per step of a promotion
  std::chrono::seconds      backup_check_interval{60};
  // Tests only: ledger files of this size instead of Config::ledger_file_size (which must be over
  // 32 MiB), so that a test's WAL spans several files and compaction deletes some.
  size_t ledger_file_size_for_test = 0;
};

class Node : public std::enable_shared_from_this<Node> {
 public:
  static shared_ptr<Node> create(Config config, RaftNodeId id, NodeOptions options = {});
  ~Node();

  /// Blocks until PostgreSQL runs (as a standby). The promotion, if this node leads, follows
  /// asynchronously.
  Result<void> start();

  /// Hands the leadership over if it has it, stops PostgreSQL, unmounts FUSE, closes raft.
  void shutdown();

  RaftNodeId             id() const { return id_; }
  uint16_t               pg_port() const { return config_.servers.at(INT(id_)).pg_port; }
  bool                   is_primary() const { return primary_.load(); }
  shared_ptr<RaftServer> raft() const { return raft_; }

  std::filesystem::path const& data_dir() const { return data_dir_; }
  std::filesystem::path const& raft_wal_dir() const { return raft_wal_dir_; }

 private:
  struct Observer;

  Node(Config config, RaftNodeId id, NodeOptions options);

  // Startup.
  Result<void> restore_latest_backup();
  Result<void> write_standby_config(uint16_t wal_sender_port);

  // Demotion (docs/node.md §3.3): reap the killed PostgreSQL, rebuild pg/pg_wal from raft/wal
  // (with FUSE unmounted), and start PostgreSQL again as a standby.
  Result<void> restart_as_standby();

  // pg/pg_wal rebuilt from raft/wal (rebuild_pg_wal), with PostgreSQL stopped and FUSE unmounted.
  Result<void> rebuild();

  // PostgreSQL's pg_wal attached to the WAL capture (wal_capture): "fuse": the FUSE mount over
  // data/pg_wal, backed by pg/pg_wal; "hook": data/pg_wal a link to pg/pg_wal (the extension reports
  // the WAL over the hook server's socket). Detached while PostgreSQL is stopped and pg/pg_wal is
  // rebuilt or restored (mappings of replaced files must not be reused).
  Result<void> attach_wal();
  void         detach_wal();
  bool         hook_mode() const { return config_.wal_capture == "hook"; }

  // node<N>/running: present from start() until a shutdown() that stopped PostgreSQL cleanly. Found
  // at a start, the previous run ended otherwise (a crash, a kill, a power loss, a PostgreSQL that
  // had to be killed), and pg/pg_wal can hold WAL that never committed: it is rebuilt.
  std::filesystem::path running_marker() const { return base_dir_ / "running"; }

  // From raft, with its lock held: must not block.
  void on_applied(RaftState const& state);
  void on_became_leader(RaftTerm term);
  void on_stepped_down(RaftTerm term);
  void on_installed();

  // needs_backup recovery (docs/node.md §4.3): an InstallSnapshot replaced the log, so the
  // standby restores the latest backup (into the same data directory) and restarts. The latest
  // backup's segment must reach the first entry's (no gap); otherwise needs_backup stays set and
  // the backup thread asks again later.
  Result<void> restore_and_restart();

  // Retention (docs/backup.md §5): keep_from from the latest backup, and the unlinked segments of
  // pg/pg_wal before the first entry's segment.
  void retention();

  // The transition thread: acts on the latest role only.
  void transitions();
  void promote(RaftTerm term);
  void demote();
  bool still_leader(RaftTerm term) const;

  // The status poller.
  void poll();

  // Backups after bootstrap (docs/backup.md §4): a standby takes one when its staggered deadline
  // has passed and no other node holds a live lease.
  void backups();
  void maybe_backup();
  bool wait_stopping(std::chrono::milliseconds timeout);

  void advance_senders(LogSeqNumber lsn);

  Config const      config_;
  RaftNodeId const  id_;
  NodeOptions const options_;

  std::filesystem::path const base_dir_;
  std::filesystem::path const data_dir_;
  std::filesystem::path const pg_wal_dir_;    // pg/pg_wal
  std::filesystem::path const raft_dir_;      // raft
  std::filesystem::path const raft_wal_dir_;  // raft/wal

  shared_ptr<BackupStore>     store_;
  shared_ptr<RaftServer>      raft_;
  shared_ptr<NodeWalTracker>  tracker_;
  unique_ptr<Fuse>            fuse_;
  shared_ptr<WalHookServer>   hook_server_;  // wal_capture = "hook"
  unique_ptr<PgInstance>      pg_;
  shared_ptr<WalSenderServer> wal_sender_server_;

  // commit_wait = "sync_rep": with FUSE, the leader's synchronous-standby connection to its
  // PostgreSQL (SyncAcker), from promotion to step-down; on_applied() hands it the committed WAL
  // end. With the hooks, the commits wait in NodeWalTracker::async_commit (no SyncAcker).
  bool                              sync_rep_ = false;
  // A shared_ptr behind a mutex: Apple clang's libc++ has no std::atomic<shared_ptr>.
  class AckerSlot {
   public:
    shared_ptr<SyncAcker> load() {
      auto lk = std::lock_guard{mtx_};
      return ptr_;
    }
    void store(shared_ptr<SyncAcker> p) {
      auto lk = std::lock_guard{mtx_};
      ptr_    = std::move(p);
    }
    shared_ptr<SyncAcker> exchange(shared_ptr<SyncAcker> p) {
      auto lk = std::lock_guard{mtx_};
      std::swap(ptr_, p);
      return p;
    }

   private:
    std::mutex            mtx_;
    shared_ptr<SyncAcker> ptr_;
  };
  AckerSlot acker_;

  std::mutex                  senders_mtx_;
  vector<weak_ptr<WalSender>> senders_;
  LogSeqNumber                committed_{};  // the latest flushed_lsn applied

  // The standby's flushed LSN (StandbyStatusUpdate), for the promotion.
  std::mutex              standby_mtx_;
  std::condition_variable standby_cv_;
  uint64_t                standby_flushed_{0};

  // The role the transition thread acts on.
  std::mutex              role_mtx_;
  std::condition_variable role_cv_;
  std::optional<RaftTerm> leader_term_;  // set by became_leader, cleared by stepped_down
  bool                    role_changed_{false};
  bool                    restore_requested_{false};
  bool                    stopping_{false};

  std::atomic_bool primary_{false};
  std::atomic_bool started_{false};
  std::atomic_bool needs_restart_{false};  // PostgreSQL was killed (stepped_down)

  std::thread transition_thread_;
  std::thread poll_thread_;
  std::thread backup_thread_;
};
