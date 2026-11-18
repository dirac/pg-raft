#pragma once

#include "src/raft/entry.h"

/// The state machine. The replicated state is the WAL itself (the segment files, written as the
/// entries are received); what is applied is the position:
///
///  - log_idx: the last entry applied.
///  - timestamp: the time of the last entry applied.
///  - flushed_lsn: the highest flushed_lsn of the entries applied, the committed WAL that a
///    standby may replay (WalSender::advance).
///  - needs_backup: the log was replaced by an InstallSnapshot, the WAL before its segments is
///    missing: the node needs a PostgreSQL backup before its standby can replay.
///  - wal_end: the highest end (segment + offset + size) of the WAL bytes applied (WriteOps, and
///    FlushOps, which carry the writes since the previous flush: wal_bytes()), the end of the
///    WAL in wal_dir. A new leader advances its WalSender to it before promoting: flushed_lsn is
///    polled and stale, while wal_end covers every byte a previous primary acknowledged. It can
///    end inside an incomplete record (the FUSE layer does not check CRCs), which PostgreSQL
///    discards when it promotes. It is a maximum: a rewritten part of a page is sent again over
///    earlier offsets.
///  - first_idx, first_segment: the node's own (each node compacts on its own schedule, so they
///    are not compared across nodes): the first entry of the log, and the segment of the first
///    entry that has one. Every entry in the log has its WAL in wal_dir, so the segments wholly
///    before first_segment (by address) can be deleted (RaftServer's WAL deletion thread).
///
/// It is saved in the meta file (docs/ledger.md §8) on compaction and on needs_backup changes.
struct RaftState {
  RaftIndex    log_idx{0};
  std::time_t  timestamp{0};
  LogSeqNumber flushed_lsn{};
  bool         needs_backup{false};
  LogSeqNumber wal_end{};
  RaftIndex    first_idx{0};
  LogSeqNumber first_segment{};

  bool operator==(RaftState const&) const = default;

  /// Apply the entry at idx.
  void apply(RaftIndex idx, Entry const& e) {
    assert(idx == log_idx + 1U);
    log_idx   = idx;
    timestamp = e.time;
    if (flushed_lsn < e.cmd.flushed_lsn)
      flushed_lsn = e.cmd.flushed_lsn;

    if (auto const bytes = wal_bytes(e.cmd); bytes) {
      auto const end = LogSeqNumber{
          .timeline = e.cmd.segment->timeline,
          .lsn      = e.cmd.segment->lsn + static_cast<uint64_t>(bytes->first) + bytes->second,
      };
      if (wal_end < end)
        wal_end = end;
    }
  }

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar& log_idx;
    ar& timestamp;
    ar& flushed_lsn;
    ar& needs_backup;
    ar& wal_end;
    ar& first_idx;
    ar& first_segment;
  }
};

template <>
struct std::formatter<RaftState> : public parse_fn {
  auto format(RaftState const& s, std::format_context& ctx) const {
    return std::format_to(ctx.out(),
                          "RaftState {{ log_idx = {} timestamp = {} flushed_lsn = {} "
                          "needs_backup = {} wal_end = {} first_idx = {} first_segment = {} }}",
                          s.log_idx,
                          s.timestamp,
                          s.flushed_lsn,
                          s.needs_backup,
                          s.wal_end,
                          s.first_idx,
                          s.first_segment);
  }
};
