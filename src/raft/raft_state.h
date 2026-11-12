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
///
/// It is saved with the log on compaction (RaftLog::save_snapshot).
struct RaftState {
  RaftIndex    log_idx{0};
  std::time_t  timestamp{0};
  LogSeqNumber flushed_lsn{};
  bool         needs_backup{false};

  bool operator==(RaftState const&) const = default;

  /// Apply the entry at idx.
  void apply(RaftIndex idx, Entry const& e) {
    assert(idx == log_idx + 1U);
    log_idx   = idx;
    timestamp = e.time;
    if (flushed_lsn < e.cmd.flushed_lsn)
      flushed_lsn = e.cmd.flushed_lsn;
  }

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & log_idx;
    ar & timestamp;
    ar & flushed_lsn;
    ar & needs_backup;
  }
};

template <>
struct std::formatter<RaftState> : public parse_fn {
  auto format(RaftState const& s, std::format_context& ctx) const {
    return std::format_to(ctx.out(),
                          "RaftState {{ log_idx = {} timestamp = {} flushed_lsn = {} "
                          "needs_backup = {} }}",
                          s.log_idx,
                          s.timestamp,
                          s.flushed_lsn,
                          s.needs_backup);
  }
};
