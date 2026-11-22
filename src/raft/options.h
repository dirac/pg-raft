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
  // The ledger's syncs on the commit path (docs/ledger.md §6.2); without them it is synced in the
  // background, within a second, and a commit is durable by replication.
  // leader_sync: the leader counts itself in the quorum only once its ledger has the entry on disk
  // (its fdatasync runs during the network round).
  bool leader_sync = false;
  // follower_sync: a follower fdatasyncs its ledger before it acknowledges an AppendEntries.
  bool follower_sync = false;
  // How often the WAL written into wal_dir on apply is synced and the applied state saved (a
  // checkpoint, docs/ledger.md §9.2): after a crash the entries after it are applied again.
  unsigned checkpoint_secs = 10;
  // Both ends of each leader-to-follower connection read on a thread of their own (Connection's
  // reader thread): the leader its follower's replies, the follower the leader's requests. The
  // other connections stay on the io_context.
  bool reader_threads = true;
  // A follower whose PostgreSQL standby has replayed less than the applied wal_end minus this many
  // bytes (or isn't running) doesn't start an election: it would win (raft compares the logs, not
  // PostgreSQL's replay) and then fail to promote in time, while a caught-up node could serve. Its
  // standby catches up without a leader (it streams committed WAL from wal_dir). After
  // lagging_election_wait_msecs without a leader it starts one anyway. 0: off (the raft tests have
  // no PostgreSQL; the node sets it).
  uint64_t max_standby_lag              = 0;
  unsigned lagging_election_wait_msecs  = 30000;
  // Compaction without a keep_from (set_wal_keep_from(): the latest backup's segment) keeps
  // keep_old_logs entries. The node turns it off: until it has set keep_from, compacting could drop
  // the WAL that the latest backup needs to catch up, and a node restoring it would be stuck until
  // the next backup.
  bool compact_without_keep_from = true;
  // The local PostgreSQL's redo point when the server starts (set_wal_gc_floor() later): the WAL
  // deletion thread runs at once, and must not delete what a restarting standby replays from.
  uint64_t wal_gc_floor = 0;
  // Testing only: nothing is synced to the disk, so a machine crash loses the term, the vote and
  // the log. A process crash loses nothing.
  bool no_sync = false;
};
