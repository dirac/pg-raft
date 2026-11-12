#pragma once

#include "src/raft/raft_types.h"

/// The state machine. The replicated state is the WAL itself (the segment files, written as the
/// entries are received); what is applied is the position:
///
///  - log_idx: the last entry applied.
///  - timestamp: the time of the last entry applied.
///  - flushed_lsn: the highest flushed_lsn of the entries applied, the committed WAL that a
///    standby may replay (WalSender::advance).
///  - needs_backup: the log was replaced by an InstallSnapshot, the WAL before its segments is
///    missing: the node needs a PostgreSQL backup before its standby can replay.
///  - wal_end: the highest end (segment + offset + size) of the WriteOps applied, the end of the
///    WAL in wal_dir. A new leader advances its WalSender to it before promoting: flushed_lsn is
///    polled and stale, while wal_end covers every byte a previous primary acknowledged. It can
///    end inside an incomplete record (the FUSE layer does not check CRCs), which PostgreSQL
///    discards when it promotes. It is a maximum: a rewritten page is published again as a
///    WriteOp over earlier offsets.
///
/// It is saved with the log on compaction (RaftLog::save_snapshot).
struct RaftState {
  RaftIndex    log_idx{0};
  std::time_t  timestamp{0};
  LogSeqNumber flushed_lsn{};
  bool         needs_backup{false};
  LogSeqNumber wal_end{};

  bool operator==(RaftState const&) const = default;

  /// Apply the entry at idx.
  void apply(RaftIndex idx, Entry const& e) {
    assert(idx == log_idx + 1U);
    log_idx   = idx;
    timestamp = e.time;
    if (flushed_lsn < e.cmd.flushed_lsn)
      flushed_lsn = e.cmd.flushed_lsn;

    if (auto const* w = std::get_if<WriteOp>(&e.cmd.command); w && e.cmd.segment) {
      auto const end = LogSeqNumber{
          .timeline = e.cmd.segment->timeline,
          .lsn      = e.cmd.segment->lsn + static_cast<uint64_t>(w->offset) + w->size,
      };
      if (wal_end < end)
        wal_end = end;
    }
  }

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & log_idx;
    ar & timestamp;
    ar & flushed_lsn;
    ar & needs_backup;
    ar & wal_end;
  }
};

template <>
struct std::formatter<RaftState> : public parse_fn {
  auto format(RaftState const& s, std::format_context& ctx) const {
    return std::format_to(ctx.out(),
                          "RaftState {{ log_idx = {} timestamp = {} flushed_lsn = {} "
                          "needs_backup = {} wal_end = {} }}",
                          s.log_idx,
                          s.timestamp,
                          s.flushed_lsn,
                          s.needs_backup,
                          s.wal_end);
  }
};
