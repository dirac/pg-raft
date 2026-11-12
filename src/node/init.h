#pragma once

#include "src/backup/backup_store.h"
#include "src/base/config.h"

// pg_raft_initdb: creates a cluster's first backup (docs/node.md §4.1). It doesn't start a raft
// server: every node then starts by restoring the latest backup from the store.

/// The PostgreSQL binaries: install_dir/<pg_major_version>/bin.
std::filesystem::path pg_bin(Config const& config);

/// The end LSN of a pg_basebackup in |backup_dir|: the End-LSN (and Timeline) of the last
/// WAL-Ranges entry of its backup_manifest. A restore is consistent from there.
Result<LogSeqNumber> backup_end_lsn(std::filesystem::path const& backup_dir);

/// initdb (with config.pg_config and the pg-raft defaults) into a scratch directory under
/// config.wdir, start PostgreSQL on |port| on its own (no FUSE), take a pg_basebackup, verify it,
/// and store it in |store| as node 0's. Refuses a store that already holds a backup, so that a
/// second cluster isn't created in the same store by mistake. The scratch directory is removed.
Result<BackupInfo> init_cluster(Config const& config, BackupStore& store, uint16_t port);
