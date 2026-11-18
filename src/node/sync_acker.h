#pragma once

#include "src/base/base.h"

// The node as its PostgreSQL's synchronous standby (Config::commit_wait = "sync_rep"): a physical
// replication client, application_name pg_raft (synchronous_standby_names = 'pg_raft'), that
// reports the raft-committed WAL end as its flush position. A commit then waits in PostgreSQL's
// SyncRepWaitForLSN, outside WALWriteLock, so the raft rounds of successive flushes overlap.
//
// The WAL it receives is read and discarded: the FUSE layer still captures the WAL for raft. It
// reports min(committed, received end), so it never acknowledges WAL its walsender hasn't sent.
//
// Not the default: a commit whose wait is cancelled (pg_cancel_backend, a terminate, a fast
// shutdown) becomes visible before a quorum has it (PostgreSQL's synchronous replication).
//
// Thread-safe: ack() may be called from any thread (raft's applied hook, with its lock held: it
// only writes a few bytes to the socket).
class SyncAcker {
 public:
  explicit SyncAcker(uint16_t pg_port) : port_(pg_port) {}
  ~SyncAcker() { stop(); }

  SyncAcker(SyncAcker const&)            = delete;
  SyncAcker& operator=(SyncAcker const&) = delete;

  /// Connects over loopback TCP (pg_hba: host replication 127.0.0.1 trust, initdb's default),
  /// starts replication at the server's current flush position, and reads on a thread of its own.
  Result<void> start();

  /// The raft-committed WAL end: acknowledged as written, flushed and applied.
  void ack(uint64_t committed_lsn);

  /// Closes the connection and joins the reader. Safe to call more than once.
  void stop();

  bool running() const { return running_.load(); }

 private:
  void read_loop();
  // Sends the status update for min(committed_, received_); with |force|, even if not new.
  void send_status(bool force, bool reply_requested = false);

  uint16_t const port_;
  int            fd_ = -1;
  std::thread    reader_;

  std::mutex            write_mtx_;  // the socket's writes (ack() and the reader's replies)
  std::atomic<uint64_t> committed_{0};
  std::atomic<uint64_t> received_{0};
  uint64_t              sent_{0};  // under write_mtx_
  std::atomic<bool>     running_{false};
};
