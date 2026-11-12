#pragma once

#include "src/base/base.h"

// Ported from sparrowdb's Config::ZqConfig (session and lock options dropped, snapshot_entries
// added, no leader_compaction_lag: a pg-raft leader compacts its own log).
struct Options {
  unsigned election_timeout_msecs   = 200;
  unsigned reconnect_interval_msecs = 2000;  // Attempt reconnect after 2 seconds.
  unsigned connection_timeout_secs  = 10;    // A peer connection silent for this long is closed.
  unsigned max_entries      = 0x1U << 13U;   // Max entries to send in an append entries request.
  unsigned tick_seconds     = 2;             // Every two seconds, add a tick entry to the log.
  unsigned snapshot_entries = 0x1U << 13U;   // Latest entries sent in an InstallSnapshot, 8K.
  unsigned keep_old_logs    = 0x1U << 14U;   // Keep older logs upto when compacting.
  unsigned compaction_lag   = 0x1U << 16U;   // Compact the log after applying this many, 65k.
  bool     log_req_response = false;
  // Sync the log on every commit, so that an entry is on the disk of a majority before it is
  // committed. Otherwise the log is synced in the background, within a second.
  bool sync_commits = false;
  // Testing only: nothing is synced to the disk (MDB_NOSYNC), so a machine crash loses the term,
  // the vote and the log. A process crash loses nothing.
  bool no_sync = false;
};
