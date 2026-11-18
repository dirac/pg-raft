#pragma once

#include "src/base/base.h"

// The options of the raft servers.
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
  // Each ledger file's size (docs/ledger.md); smaller in the tests.
  size_t ledger_file_size = size_t{1} << 30U;
  // The most bytes of records in an AppendEntries (at least one record is sent).
  size_t max_request_bytes = size_t{8} << 20U;
  // Sync the ledger before an entry counts as held by this node (the leader: its sync runs during
  // the round; a follower: before it answers), so that an entry is on the disk of a majority before
  // it is committed. Otherwise the ledger is synced in the background, within a second.
  bool sync_commits = false;
  // How often the WAL written into wal_dir on apply is synced and the applied state saved (a
  // checkpoint, docs/ledger.md §9.2): after a crash the entries after it are applied again.
  unsigned checkpoint_secs = 10;
  // Testing only: nothing is synced to the disk, so a machine crash loses the term, the vote and
  // the log. A process crash loses nothing.
  bool no_sync = false;
};
