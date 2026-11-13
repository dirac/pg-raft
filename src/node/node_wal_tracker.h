#pragma once

#include "src/base/wal_segment.h"
#include "src/raft/raft_server.h"

// The node's WalTracker (docs/node.md §2): what the FUSE layer over pg_wal reports becomes raft
// entries on the leader, and stays local on a follower.
//
// Leader mode:
//  - async_publish: the bytes go from PostgreSQL's segment (pg/pg_wal) into the same segment in
//    raft's wal_dir (raft/wal), from which the peer connections send them; then a WriteOp.
//  - async_fsync: msync both copies, then a FlushOp; PostgreSQL's fsync (and its commit) returns
//    when the FlushOp is committed on a quorum, and fails with kNoConsensus if the node is not, or
//    stops being, the leader (PostgreSQL then PANICs, by design).
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
  /// fsync in follower mode (Config::leader_sync / follower_sync).
  explicit NodeWalTracker(shared_ptr<RaftServer> raft,
                          bool                   leader_sync   = false,
                          bool                   follower_sync = false)
      : raft_(std::move(raft)), leader_sync_(leader_sync), follower_sync_(follower_sync) {}

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
  LogSeqNumber flushed_lsn() const;

  shared_ptr<RaftServer> raft_;
  bool const             leader_sync_;
  bool const             follower_sync_;
  std::atomic<Mode>      mode_{Mode::kFollower};

  mutable std::mutex flushed_mtx_;
  LogSeqNumber       flushed_lsn_{};
};
