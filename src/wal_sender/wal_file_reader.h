#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>

#include "src/wal_sender/wal_sender.h"

// Reads WAL segments from a PostgreSQL pg_wal directory and feeds them to a
// callback as WalSegment values.
//
// run() blocks, delivering segments starting at start_lsn, following the
// primary's WAL stream until stop() is called.  When the current segment is
// not yet fully written it polls every poll_ms milliseconds.
class WalFileReader {
 public:
  using SegmentCallback = std::function<void(WalSegment)>;

  // |wal_dir|          – path to the pg_wal directory.
  // |timeline|         – primary's current timeline ID.
  // |wal_segment_size| – must match primary's wal_segment_size (bytes).
  WalFileReader(std::string wal_dir, uint32_t timeline,
                uint32_t wal_segment_size = 16 * 1024 * 1024,
                int poll_ms = 100);

  // Read WAL from |start_lsn| and invoke |cb| for every chunk. Blocks until
  // stop() is called.
  void run(uint64_t start_lsn, SegmentCallback cb);

  // Signal run() to return. Thread-safe.
  void stop();

 private:
  std::string segment_filename(uint64_t lsn) const;

  std::string wal_dir_;
  uint32_t timeline_;
  uint32_t wal_segment_size_;
  int poll_ms_;
  std::atomic<bool> stopped_{false};
};
