#pragma once

#include "src/base/base.h"

// Demotion (docs/node.md §3.3), and a start after an unclean end: before PostgreSQL starts as a
// standby, its pg_wal must hold only committed WAL, or the standby would replay WAL that the
// cluster never committed. raft/wal holds committed WAL only (each node writes an entry's WAL bytes
// there when it applies the entry), so pg_wal is rebuilt from it:
//
//  - a segment or history file in raft/wal gets raft's copy; the segment holding its timeline's
//    start point gets the bytes before that point from the ancestor timelines that own them
//    (raft's copies of their segments, else PostgreSQL's own copy): PostgreSQL copies them in
//    through an xlogtemp file on promotion, which FUSE doesn't replicate;
//  - a segment or history file not in raft/wal is deleted (the deposed primary's uncommitted WAL,
//    or a timeline its failed promotion started), except a segment older than every segment
//    raft/wal holds, on a timeline raft knows: history raft has compacted away (a restored
//    backup's WAL, which its standby replays from the backup's redo point);
//  - unlinked (.X), .partial and xlogtemp.* files are deleted; directories (archive_status,
//    summaries) and other files are kept.
//
// Nothing is cut at an "end": the applied state a node starts with is its last checkpoint's, behind
// what raft/wal holds, and cutting there dropped committed segments of a newer timeline (the
// standby then replayed the old timeline's partial last record and PANICked).
// PostgreSQL must not be running, and pg_wal must not be mounted (WalFuseOps caches mmaps by LSN).
Result<void> rebuild_pg_wal(std::filesystem::path const& pg_wal,
                            std::filesystem::path const& raft_wal);
