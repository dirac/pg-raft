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
  // Sync the log on every commit, so that an entry is on the disk of a majority before it is
  // committed. Otherwise the log is synced in the background, within a second.
  bool sync_commits = false;
  // A follower msyncs the WAL blocks of an AppendEntries before acknowledging it. Off by default:
  // a commit is durable by replication (a majority has the bytes in its page cache, written
  // through the mmap), not by the disks; the kernel writes the pages back on its own. Only a power
  // loss of a majority at once loses acknowledged commits. (Config::follower_sync.)
  bool follower_sync = false;
  // Testing only: nothing is synced to the disk (MDB_NOSYNC), so a machine crash loses the term,
  // the vote and the log. A process crash loses nothing.
  bool no_sync = false;
};
