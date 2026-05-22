#pragma once

#include <string>
#include <vector>

// Initializes a PostgreSQL data directory for use with WalFuseManager.
//
// run() performs three steps in order:
//   1. Runs initdb to create |data_dir|.
//   2. Creates |pg_raft_wal_dir|.
//   3. Moves all contents of |data_dir|/pg_wal into |pg_raft_wal_dir|,
//      leaving the pg_wal directory itself in place as the future FUSE
//      mount point.
//
// After run() succeeds, WalFuseManager can be constructed and started:
// it will mount a passthrough FUSE at pg_wal (backed by pg_raft_wal_dir)
// and then launch postgres.
//
// pg_raft_wal is always located at |data_dir|/pg_raft_wal (a sibling of pg_wal).
class PgInitializer {
 public:
  // |pg_bin_dir|  – directory containing the postgres binaries (initdb, etc.)
  // |data_dir|    – PGDATA path to initialize.
  // |initdb_args| – extra arguments forwarded verbatim to initdb.
  PgInitializer(std::string pg_bin_dir, std::string data_dir,
                std::vector<std::string> initdb_args = {});

  // Execute the three-step initialization. Returns false on any failure.
  bool run();

 private:
  std::string pg_bin_dir_;
  std::string data_dir_;
  std::vector<std::string> initdb_args_;
};
