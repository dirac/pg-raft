#include "src/node/node_wal_tracker.h"

#include "src/base/logger.h"

void NodeWalTracker::set_mode(Mode mode) {
  static constexpr std::array kNames = {"follower", "leader", "fenced"};
  if (mode_.exchange(mode) != mode) {
    log(INFO, "WAL tracker in {} mode", kNames.at(static_cast<size_t>(mode)));
    // What a previous leadership published and never flushed was never acknowledged: void it.
    epoch_.fetch_add(1);
    // Its commits too: they fail (PostgreSQL PANICs, never acknowledging them).
    commits_->reset();
  }
}

void NodeWalTracker::CommitWaits::committed(uint64_t end, uint64_t entry_epoch) {
  vector<std::function<void(Result<void>)>> done;
  {
    auto lk = std::unique_lock{mtx};
    if (entry_epoch != epoch || end <= committed_end)
      return;
    committed_end   = end;
    auto const last = waits.upper_bound(end);
    for (auto pos = waits.begin(); pos != last; ++pos)
      done.push_back(std::move(pos->second));
    waits.erase(waits.begin(), last);
  }
  for (auto& f : done)
    f(ok());
}

void NodeWalTracker::CommitWaits::reset() {
  decltype(waits) failed;
  {
    auto lk = std::unique_lock{mtx};
    ++epoch;
    committed_end = 0;
    failed.swap(waits);
  }
  for (auto& [lsn, f] : failed)
    f(system_error_code(WalSegment::kNoConsensus));
}

std::function<void(Result<RaftIndex>)> NodeWalTracker::on_commit(uint64_t end) {
  auto epoch = uint64_t{0};
  {
    auto lk = std::unique_lock{commits_->mtx};
    epoch   = commits_->epoch;
  }
  return [commits = commits_, end, epoch](Result<RaftIndex> r) {
    if (r)
      commits->committed(end, epoch);
  };
}

void NodeWalTracker::async_commit(uint64_t lsn, std::function<void(Result<void>)> completion) {
  {
    auto lk = std::unique_lock{commits_->mtx};
    // Only a leader's PostgreSQL commits; the mode is set before reset() takes the lock, so a wait
    // added in leader mode is failed by the next change.
    if (mode() == Mode::kLeader && lsn > commits_->committed_end) {
      commits_->waits.emplace(lsn, std::move(completion));
      return;
    }
  }
  if (mode() == Mode::kLeader)
    return completion(ok());
  completion(system_error_code(WalSegment::kNoConsensus));
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
      [committed = on_commit(segment->lsn().lsn + static_cast<uint64_t>(from) + bytes)](
          Result<RaftIndex> r) {
        if (!r)
          log(DEBUG, "WriteOp not committed: {}", r.error().message());
        committed(r);
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
  // The WAL up to the flush's end is in this entry or the ones before it (with no bytes, its offset
  // is the end of what was flushed: the hook passes the end, FUSE the start of what it hasn't
  // synced), for the commit waits.
  auto committed = on_commit(segment->lsn().lsn + static_cast<uint64_t>(flush.offset) + flush.size);

  if (!wait_commit_) {
    // Appended (exec() appends before it returns): the fsync completes now, the commit waits in
    // PostgreSQL's synchronous replication. Not leader (the handler runs at once): it fails.
    auto failed = make_shared<std::atomic<bool>>(false);
    raft_->exec(
        WalCommand{
            .flushed_lsn = carry(),
            .segment     = segment->lsn(),
            .command     = flush,
        },
        std::span<uint8_t const>(segment->mapping() + flush.offset, flush.size),
        [failed, committed](Result<RaftIndex> r) {
          if (!r)
            failed->store(true);
          committed(r);
        });
    lk.unlock();
    return completion(failed->load() ? Result<void>{system_error_code(WalSegment::kNoConsensus)}
                                     : ok());
  }

  // The handler runs with the server lock held: it only completes PostgreSQL's fsync.
  raft_->exec(
      WalCommand{
          .flushed_lsn = carry(),
          .segment     = segment->lsn(),
          .command     = flush,
      },
      std::span<uint8_t const>(segment->mapping() + flush.offset, flush.size),
      [completion = std::move(completion), committed](Result<RaftIndex> r) {
        committed(r);
        if (r)
          completion(ok());
        else
          completion(system_error_code(WalSegment::kNoConsensus));
      });
}

void NodeWalTracker::async_timeline(uint32_t timeline, string const& content) {
  if (mode() != Mode::kLeader)
    return;
  // A timeline that isn't past the committed one belongs to another primary: publishing it would
  // replace that history (the node then steps down: Node::promote()).
  if (auto const committed = raft_->applied_state().timeline; timeline <= committed) {
    log(ERROR, "Not publishing timeline {}: timeline {} is committed already", timeline, committed);
    return;
  }

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
