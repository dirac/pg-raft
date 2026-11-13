#include "src/node/node_wal_tracker.h"

#include "src/base/logger.h"

void NodeWalTracker::set_mode(Mode mode) {
  static constexpr std::array kNames = {"follower", "leader", "fenced"};
  if (mode_.exchange(mode) != mode)
    log(INFO, "WAL tracker in {} mode", kNames.at(static_cast<size_t>(mode)));
}

void NodeWalTracker::set_flushed_lsn(LogSeqNumber lsn) {
  auto lk = std::unique_lock{flushed_mtx_};
  if (flushed_lsn_ < lsn)
    flushed_lsn_ = lsn;
}

LogSeqNumber NodeWalTracker::flushed_lsn() const {
  auto lk = std::unique_lock{flushed_mtx_};
  return flushed_lsn_;
}

void NodeWalTracker::async_publish(shared_ptr<WalSegment> segment, off_t offset, size_t size) {
  if (mode() != Mode::kLeader || size == 0)
    return;

  // The bytes first: a peer connection may send them as soon as the entry is in the log.
  auto copy = raft_->segment(segment->lsn());
  if (!copy) {
    elog(copy.error(), "Unable to open {} in raft's wal_dir", segment->lsn());
    return;
  }
  copy.value()->write(segment->mapping() + offset, offset, size);

  raft_->exec(
      WalCommand{
          .flushed_lsn = flushed_lsn(),
          .segment     = segment->lsn(),
          .command     = WriteOp{.offset = offset, .size = size},
      },
      [](Result<RaftIndex> r) {
        if (!r)
          log(DEBUG, "WriteOp not committed: {}", r.error().message());
      });
}

void NodeWalTracker::async_fsync(shared_ptr<WalSegment>            segment,
                                 off_t                             offset,
                                 size_t                            size,
                                 std::function<void(Result<void>)> completion) {
  if (mode() == Mode::kFenced)
    return completion(system_error_code(WalSegment::kNoConsensus));

  // With leader_sync / follower_sync, local durability first, in pg_wal. Without it the bytes are
  // in the page cache (written through the mmap) and a commit is durable by replication.
  bool const leader = mode() == Mode::kLeader;
  if (leader ? leader_sync_ : follower_sync_)
    if (auto r = segment->fsync(offset, size); !r)
      return completion(r);

  if (!leader)
    return completion(ok());

  // The leader's own copy counts towards the quorum.
  if (leader_sync_) {
    auto copy = raft_->segment(segment->lsn());
    if (!copy)
      return completion(copy.error());
    if (auto r = copy.value()->fsync(offset, size); !r)
      return completion(r);
  }

  // The handler runs with the server lock held: it only completes PostgreSQL's fsync.
  raft_->exec(
      WalCommand{
          .flushed_lsn = flushed_lsn(),
          .segment     = segment->lsn(),
          .command     = FlushOp{.offset = offset, .size = size},
      },
      [completion = std::move(completion)](Result<RaftIndex> r) {
        if (r)
          completion(ok());
        else
          completion(system_error_code(WalSegment::kNoConsensus));
      });
}

void NodeWalTracker::async_timeline(uint32_t timeline, string const& content) {
  if (mode() != Mode::kLeader)
    return;

  log(INFO, "Publishing timeline {}", timeline);
  raft_->exec(
      WalCommand{
          .flushed_lsn = flushed_lsn(),
          .command     = TimelineOp{.timeline = timeline, .content = content},
      },
      [timeline](Result<RaftIndex> r) {
        if (!r)
          log(ERROR, "TimelineOp {} not committed: {}", timeline, r.error().message());
      });
}
