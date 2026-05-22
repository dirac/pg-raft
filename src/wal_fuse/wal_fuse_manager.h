#pragma once

#include <string>
#include <vector>

#include "src/process/process.h"
#include "src/wal_fuse/wal_fuse.h"

// Mounts a passthrough FUSE filesystem over the pg_wal directory in a
// PostgreSQL data directory, then starts PostgreSQL.
//
// On start():
//   1. Creates |pg_raft_wal_dir| if absent.
//   2. Mounts WalFuse at |data_dir|/pg_wal, redirecting all I/O to
//      |pg_raft_wal_dir|.
//   3. Waits until the mount is live, then starts postgres.
//
// On stop():
//   1. Stops postgres (SIGTERM → wait).
//   2. Unmounts the FUSE filesystem.
class WalFuseManager {
 public:
  // |pg_bin|     – path to the postgres binary.
  // |data_dir|   – PostgreSQL data directory (PGDATA). pg_raft_wal is
  //                expected at |data_dir|/pg_raft_wal (created by PgInitializer).
  // |extra_args| – additional arguments forwarded to postgres.
  WalFuseManager(std::string pg_bin, std::string data_dir,
                 std::vector<std::string> extra_args = {});
  ~WalFuseManager();

  WalFuseManager(const WalFuseManager&) = delete;
  WalFuseManager& operator=(const WalFuseManager&) = delete;

  bool start();
  void stop();

 private:
  bool wait_for_mount(int timeout_ms);

  std::string pg_bin_;
  std::string data_dir_;
  std::vector<std::string> extra_args_;

  // Declared after the string/vector members so their values are ready when
  // these are constructed in the initializer list.
  WalFuse fuse_;
  Process postgres_;
};
