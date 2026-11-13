#pragma once

#include "src/base/wal_segment.h"
#include "src/raft/raft_server.h"

// The node's WalTracker (docs/node.md §2): what the FUSE layer over pg_wal reports becomes raft
// entries on the leader, and stays local on a follower.
//
// Leader mode:
//  - async_publish: the bytes go from PostgreSQL's segment (pg/pg_wal) into the same segment in
//    raft's wal_dir (raft/wal), from which the peer connections send them, and their range is
//    added to the segment's pending range. Once that reaches write_op_bytes it is sent at once,
//    as a WriteOp (a large transaction streams its WAL before its commit); smaller writes wait
//    for the next flush.
//  - async_fsync: (with leader_sync, msync both copies,) then one FlushOp that carries what is
//    pending of the segment, the bytes written since its last WriteOp or flush; PostgreSQL's
//    fsync (and its commit) returns when the FlushOp is committed on a quorum, and fails with
//    kNoConsensus if the node is not, or stops being, the leader (PostgreSQL then PANICs, by
//    design). Usually one entry and one raft round per fsync, instead of an entry per write.
//  - async_timeline: a TimelineOp with the history file.
//  Every command carries flushed_lsn: the primary's flush LSN, set by the status poller.
//
// Follower mode: publishes and timelines are ignored (the standby only writes WAL that is already
// committed), and fsync is a local msync.
//
// Fenced mode: the node lost leadership and has killed its PostgreSQL; until it restarts as a
// standby (follower mode), every fsync fails with kNoConsensus, so that a backend that outlives
// the kill for a moment can't have a commit acknowledged. Publishes and timelines are ignored.
//
// FUSE calls these one at a time, so the entries are appended in WAL order.
class NodeWalTracker : public WalTracker {
 public:
  enum class Mode { kFollower, kLeader, kFenced };

  /// |leader_sync|: msync before the FlushOp in leader mode; |follower_sync|: msync PostgreSQL's
  /// fsync in follower mode (Config::leader_sync / follower_sync). |write_op_bytes|: the pending
  /// writes of a segment are sent as a WriteOp once they reach it (Config::write_op_bytes).
  explicit NodeWalTracker(shared_ptr<RaftServer> raft,
                          bool                   leader_sync    = false,
                          bool                   follower_sync  = false,
                          size_t                 write_op_bytes = 64 * 1024)
      : raft_(std::move(raft)),
        leader_sync_(leader_sync),
        follower_sync_(follower_sync),
        write_op_bytes_(write_op_bytes) {}

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

 private:
  /// The flush LSN for an entry (the poller's); remembers it as carried.
  LogSeqNumber carry();

  shared_ptr<RaftServer> raft_;
  bool const             leader_sync_;
  bool const             follower_sync_;
  size_t const           write_op_bytes_;
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

  /// Takes the segment's pending range (one of the current epoch), with pending_mtx_ held.
  std::optional<std::pair<off_t, size_t>> take_pending(LogSeqNumber lsn);
};
