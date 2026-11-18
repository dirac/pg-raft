#include "src/node/node_wal_tracker.h"

#include "src/base/logger.h"

void NodeWalTracker::set_mode(Mode mode) {
  static constexpr std::array kNames = {"follower", "leader", "fenced"};
  if (mode_.exchange(mode) != mode) {
    log(INFO, "WAL tracker in {} mode", kNames.at(static_cast<size_t>(mode)));
    // What a previous leadership published and never flushed was never acknowledged: void it.
    epoch_.fetch_add(1);
  }
}

std::optional<std::pair<off_t, size_t>> NodeWalTracker::take_pending(LogSeqNumber lsn) {
  auto pos = pending_.find(lsn);
  if (pos == pending_.end())
    return std::nullopt;
  auto const p = pos->second;
  pending_.erase(pos);
  if (p.epoch != epoch_.load())
    return std::nullopt;
  return std::pair{p.begin, static_cast<size_t>(p.end - p.begin)};
}

void NodeWalTracker::set_flushed_lsn(LogSeqNumber lsn) {
  bool behind = false;
  {
    auto lk = std::unique_lock{flushed_mtx_};
    if (flushed_lsn_ < lsn)
      flushed_lsn_ = lsn;
    behind = carried_ < flushed_lsn_;
  }
  // On the leader, a flush LSN that no entry has carried yet goes out in an entry of its own
  // (no WAL): otherwise, once the writes stop, the followers' standbys would stay behind the last
  // flush until the next one. PostgreSQL's flush LSN is past an fsync that has committed.
  if (behind && mode() == Mode::kLeader)
    raft_->exec(WalCommand{.flushed_lsn = carry()}, [](Result<RaftIndex> r) {
      if (!r)
        log(DEBUG, "flushed_lsn entry not committed: {}", r.error().message());
    });
}

LogSeqNumber NodeWalTracker::carry() {
  auto lk = std::unique_lock{flushed_mtx_};
  if (carried_ < flushed_lsn_)
    carried_ = flushed_lsn_;
  return flushed_lsn_;
}

void NodeWalTracker::async_publish(shared_ptr<WalSegment> segment, off_t offset, size_t size) {
  if (mode() != Mode::kLeader || size == 0)
    return;

  // Pending: sent with the segment's next flush, or as a WriteOp once there is enough of it.
  auto const end   = offset + static_cast<off_t>(size);
  auto const epoch = epoch_.load();
  auto       lk    = std::unique_lock{pending_mtx_};
  auto [pos, added] =
      pending_.try_emplace(segment->lsn(), Pending{.begin = offset, .end = end, .epoch = epoch});
  if (!added) {
    auto& p = pos->second;
    if (p.epoch != epoch)
      p = Pending{.begin = offset, .end = end, .epoch = epoch};
    p.begin = std::min(p.begin, offset);
    p.end   = std::max(p.end, end);
  }
  if (static_cast<size_t>(pos->second.end - pos->second.begin) < write_op_bytes_)
    return;

  auto const [from, bytes] = take_pending(segment->lsn()).value_or(std::pair{off_t{0}, size_t{0}});
  if (bytes == 0)
    return;
  // The bytes from PostgreSQL's segment: the entry's record copies them.
  raft_->exec(
      WalCommand{
          .flushed_lsn = carry(),
          .segment     = segment->lsn(),
          .command     = WriteOp{.offset = from, .size = bytes},
      },
      std::span<uint8_t const>(segment->mapping() + from, bytes),
      [](Result<RaftIndex> r) {
        if (!r)
          log(DEBUG, "WriteOp not committed: {}", r.error().message());
      });
}

void NodeWalTracker::async_fsync(shared_ptr<WalSegment>            segment,
                                 off_t                             offset,
                                 size_t /*size*/,
                                 std::function<void(Result<void>)> completion) {
  if (mode() == Mode::kFenced)
    return completion(system_error_code(WalSegment::kNoConsensus));

  // pg_wal is a cache of the WAL (raft keeps it durably): no msync. A standby's fsync is done.
  if (mode() != Mode::kLeader)
    return completion(ok());

  // The writes since the segment's last WriteOp or flush travel with this flush (wal_bytes());
  // nothing new: an empty FlushOp, still the barrier. Taken and appended under pending_mtx_, so
  // that no WriteOp of earlier bytes lands after it.
  auto lk    = std::unique_lock{pending_mtx_};
  auto flush = FlushOp{.offset = offset, .size = 0};
  if (auto p = take_pending(segment->lsn()); p)
    flush = FlushOp{.offset = p->first, .size = p->second};

  // The handler runs with the server lock held: it only completes PostgreSQL's fsync.
  raft_->exec(
      WalCommand{
          .flushed_lsn = carry(),
          .segment     = segment->lsn(),
          .command     = flush,
      },
      std::span<uint8_t const>(segment->mapping() + flush.offset, flush.size),
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
          .flushed_lsn = carry(),
          .command     = TimelineOp{.timeline = timeline, .content = content},
      },
      [timeline](Result<RaftIndex> r) {
        if (!r)
          log(ERROR, "TimelineOp {} not committed: {}", timeline, r.error().message());
      });
}
