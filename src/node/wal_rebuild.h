#pragma once

#include "src/base/base.h"

// Demotion (docs/node.md §3.3): before a deposed primary restarts as a standby, its pg_wal must
// hold only committed WAL, or the standby would replay WAL that the cluster never committed.
// raft/wal holds the replicated WAL, so pg_wal is rebuilt from it:
//
//  - a segment or history file not in raft/wal is deleted (the deposed primary's uncommitted WAL,
//    or a timeline its failed promotion started);
//  - a segment on a later timeline than |end| is deleted, one on an earlier timeline gets raft's
//    copy;
//  - on |end|'s timeline, a segment wholly before |end| gets raft's copy, the segment holding |end|
//    gets raft's copy with everything from |end| on zeroed (raft/wal may hold uncommitted bytes
//    past it: the leader copies bytes in before they commit), and a later segment is deleted;
//  - unlinked (.X), .partial and xlogtemp.* files are deleted; directories (archive_status,
//    summaries) and other files are kept.
//
// |end| is the end of the committed WAL: the larger of RaftState::wal_end and the end of the
// backup the node restored, rounded up to its segment's end (its WAL is committed by definition
// and is in raft/wal too; pg_backup_stop ends that segment with an XLOG_SWITCH record past the
// backup's End-LSN, which must not be zeroed).
// PostgreSQL must not be running, and pg_wal must not be mounted (WalFuseOps caches mmaps by LSN).
Result<void> rebuild_pg_wal(std::filesystem::path const& pg_wal,
                            std::filesystem::path const& raft_wal,
                            LogSeqNumber                 end);
