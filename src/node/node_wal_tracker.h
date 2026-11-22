#pragma once

#include "src/base/wal_segment.h"
#include "src/raft/raft_server.h"

// The node's WalTracker (docs/node.md §2): what the FUSE layer over pg_wal reports becomes raft
// entries on the leader, and stays local on a follower.
//
// Leader mode:
//  - async_publish: the range is added to the segment's pending range. Once that reaches
//    write_op_bytes it is sent at once, as a WriteOp (a large transaction streams its WAL before
//    its commit); smaller writes wait for the next flush. An entry's WAL bytes go to exec() from
//    PostgreSQL's segment (pg/pg_wal); its ledger record copies them, and every node writes them
//    into raft/wal when it applies the entry.
//  - async_fsync: one FlushOp that carries what is
//    pending of the segment, the bytes written since its last WriteOp or flush; PostgreSQL's
//    fsync (and its commit) returns when the FlushOp is committed on a quorum, and fails with
//    kNoConsensus if the node is not, or stops being, the leader (PostgreSQL then PANICs, by
//    design). Usually one entry and one raft round per fsync, instead of an entry per write.
//  - async_timeline: a TimelineOp with the history file.
//  Every command carries flushed_lsn: the primary's flush LSN, set by the status poller.
//
// Follower mode: publishes and timelines are ignored (the standby only writes WAL that is already
// committed), and fsync succeeds at once.
//
// pg_wal is never synced (msync): it is a cache of the WAL. Raft keeps the committed WAL durably
// (the ledger, and raft/wal derived from it); a standby whose pg_wal lost pages in a machine crash
// finds an invalid record there and streams the rest from the WAL sender, and a deposed primary's
// pg_wal is rebuilt from raft/wal (rebuild_pg_wal).
//
// Commit waits (async_commit, wal_capture = "hook" with commit_wait = "sync_rep": PostgreSQL's
// sync_rep_wait_hook): a commit at |lsn| completes once an entry of the current leadership that
// carries the WAL up to it (a WriteOp or a FlushOp) is committed; in any mode but leader, and on a
// mode change, the waits fail with kNoConsensus (PostgreSQL then PANICs: nothing this node didn't
// commit becomes visible).
//
// Fenced mode: the node lost leadership and has killed its PostgreSQL; until it restarts as a
// standby (follower mode), every fsync fails with kNoConsensus, so that a backend that outlives
// the kill for a moment can't have a commit acknowledged. Publishes and timelines are ignored.
//
// FUSE calls these one at a time, so the entries are appended in WAL order.
class NodeWalTracker : public WalTracker {
 public:
  enum class Mode { kFollower, kLeader, kFenced };

  /// |write_op_bytes|: the pending writes of a segment are sent as a WriteOp once they reach it
  /// (Config::write_op_bytes).
  /// |wait_commit|: a leader's fsync completes when its FlushOp is committed (commit_wait =
  /// "fsync"); otherwise once it is appended to the leader's ledger, and the commit waits for the
  /// quorum in PostgreSQL's synchronous replication (commit_wait = "sync_rep": async_commit, or
  /// SyncAcker as a synchronous standby).
  explicit NodeWalTracker(shared_ptr<RaftServer> raft,
                          size_t                 write_op_bytes = 64 * 1024,
                          bool                   wait_commit    = true)
      : raft_(std::move(raft)),
        write_op_bytes_(write_op_bytes),
        wait_commit_(wait_commit),
        commits_(make_shared<CommitWaits>()) {}

  void set_mode(Mode mode);
  Mode mode() const { return mode_.load(); }

  /// The primary's flush LSN, from the status poller: the followers' WAL senders stream up to it.
  void set_flushed_lsn(LogSeqNumber lsn);

  void async_publish(shared_ptr<WalSegment> segment, off_t offset, size_t size) override;
  void async_fsync(shared_ptr<WalSegment>            segment,
                   off_t                             offset,
                   size_t                            size,
                   std::function<void(Result<void>)> completion) override;
  void async_timeline(uint32_t timeline, string const& content) override;
  void async_commit(uint64_t lsn, std::function<void(Result<void>)> completion) override;

 private:
  /// The commit waits of the current mode epoch, and how far its committed entries' WAL reaches.
  /// Shared with the entries' handlers (they may run after the tracker is gone, at raft's
  /// shutdown). A mode change bumps |epoch| and fails the waits under |mtx|, so that no entry of an
  /// older leadership completes a newer wait.
  struct CommitWaits {
    std::mutex                                                 mtx;
    uint64_t                                                   epoch         = 0;
    uint64_t                                                   committed_end = 0;
    std::multimap<uint64_t, std::function<void(Result<void>)>> waits;

    /// An entry of |entry_epoch| carrying the WAL up to |end| is committed.
    void committed(uint64_t end, uint64_t entry_epoch);
    /// A mode change: a new epoch, the waits failed.
    void reset();
  };

  /// An entry's handler, for an entry carrying the WAL up to |end|: on commit, completes the waits
  /// it covers.
  std::function<void(Result<RaftIndex>)> on_commit(uint64_t end);

  /// The flush LSN for an entry (the poller's); remembers it as carried.
  LogSeqNumber carry();

  shared_ptr<RaftServer> raft_;
  size_t const           write_op_bytes_;
  bool const             wait_commit_;
  std::atomic<Mode>      mode_{Mode::kFollower};

  // flushed_lsn_: PostgreSQL's flush LSN, polled (not the end of the published bytes: the FUSE
  // walk can end inside a record whose body PostgreSQL hasn't copied yet, docs/node.md §2.2);
  // carried_: the highest one an entry carried. A follower's WAL sender streams up to the
  // flushed_lsn of the entries it has applied.
  mutable std::mutex flushed_mtx_;
  LogSeqNumber       flushed_lsn_{};
  LogSeqNumber       carried_{};

  /// Per segment, the range published since its last WriteOp or flush (leader mode), and the
  /// mode epoch it was published in. Taking a range and appending its entry happen under
  /// pending_mtx_, so that a FlushOp follows the WriteOps of the bytes before it. set_mode() does
  /// not take pending_mtx_ (raft's stepped_down hook calls it with the server lock held, and
  /// exec() takes that lock under pending_mtx_): it bumps epoch_, which voids the older ranges.
  struct Pending {
    off_t    begin;
    off_t    end;
    uint64_t epoch;
  };
  std::mutex                 pending_mtx_;
  map<LogSeqNumber, Pending> pending_;
  std::atomic<uint64_t>      epoch_{0};

  shared_ptr<CommitWaits> const commits_;

  /// Takes the segment's pending range (one of the current epoch), with pending_mtx_ held.
  std::optional<std::pair<off_t, size_t>> take_pending(LogSeqNumber lsn);
};
