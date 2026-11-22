#pragma once

#include "src/base/wal_segment.h"

// The node's end of the pg_raft_wal extension (src/pg_raft_wal, wal_capture = "hook"): PostgreSQL's
// WAL hooks (the pg-raft patch of XLogWrite) become WalTracker calls, as the FUSE layer's do
// (wal_capture = "fuse"), on a Unix socket instead of a FUSE mount over pg_wal.
//
// Each request is 24 bytes (type, timeline, segment number, offset, size; native byte order), each
// answer one byte (0: done):
//  - 'W': the WAL at [offset, offset + size) of the segment is in PostgreSQL's file
//         (pg_wal/<segment>): WalTracker::async_publish, answered at once.
//  - 'F': the segment's WAL up to |offset| must be durable: WalTracker::async_fsync, answered when it
//         completes (on the leader, when raft commits it); 1 if it fails.
//  - 'T': a new timeline's history file, |size| bytes of it following: WalTracker::async_timeline.
//  - 'C': a commit waits for the WAL up to the LSN in the segment number's place (sync_rep_wait_hook,
//         commit_wait = "sync_rep"): WalTracker::async_commit, answered when it completes (when raft
//         commits the WAL up to there); 1 if it fails.
//
// PostgreSQL sends the write and flush requests with WALWriteLock held, waiting for each answer: they
// come one at a time, in WAL order, even over the connections of different processes. Commit waits
// come from many processes at once, each on its own connection.
//
// The segments are mapped from |pg_wal| (WalSegment); reset_segments() drops the mappings, before
// PostgreSQL's segments are replaced (a rebuild or a restore of pg_wal).
class WalHookServer : public std::enable_shared_from_this<WalHookServer> {
 public:
  static constexpr size_t kRequestSize = 24;

  WalHookServer(std::filesystem::path socket, std::filesystem::path pg_wal, WalTracker& tracker);
  ~WalHookServer();

  /// Binds the socket (replacing a stale socket file) and accepts.
  Result<void> start();

  /// Stops accepting, closes the connections, removes the socket file.
  void stop();

  /// Drops the segment mappings (the files are about to be replaced).
  void reset_segments();

  std::filesystem::path const& socket_path() const { return socket_; }

 private:
  struct Conn;
  void accept();
  Result<shared_ptr<WalSegment>> segment(LogSeqNumber lsn);

  std::filesystem::path const socket_;
  std::filesystem::path const pg_wal_;
  WalTracker&                 tracker_;

  std::mutex                             mtx_;  // the segments and the cache
  shared_ptr<WalSegmentCache>            cache_;
  map<LogSeqNumber, shared_ptr<WalSegment>> segments_;

  asio::local::stream_protocol::acceptor acceptor_;
  std::mutex                             conns_mtx_;
  vector<weak_ptr<Conn>>                 conns_;
};
