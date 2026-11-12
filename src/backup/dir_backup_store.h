#pragma once

#include "src/backup/backup_store.h"

// A BackupStore in a directory (docs/backup.md §3):
//
//   root/
//     00000001-0000000003000028-1762934400-0/       a complete backup
//     .pending-3                                    node 3's lease: start time inside, mtime =
//                                                   last refresh
//     .tmp-00000002-0000000006000000-1762977600-3/  a put() copying, or one that crashed
//
// put() copies into .tmp-<name>, fsyncs it and renames it to <name>: the rename is the commit, and
// names starting with '.' are never listed as backups. A partial copy is removed by the next
// begin() or abort() of the same node.
class DirBackupStore : public BackupStore {
 public:
  explicit DirBackupStore(std::filesystem::path root);

  Result<PendingBackup> begin(RaftNodeId node, std::time_t time) override;
  Result<void>          refresh(PendingBackup& pending, std::time_t time) override;
  Result<BackupInfo>    put(PendingBackup const&         pending,
                            std::filesystem::path const& dir,
                            LogSeqNumber                 lsn) override;
  Result<void>          abort(RaftNodeId node) override;

  Result<vector<PendingBackup>> pending() const override;
  Result<vector<BackupInfo>>    list() const override;

  Result<void> get(BackupInfo const& backup, std::filesystem::path const& dest) const override;
  Result<void> remove(BackupInfo const& backup) override;

  std::filesystem::path const& root() const { return root_; }

 private:
  std::filesystem::path lease(RaftNodeId node) const;

  /// Remove the partial copies of |node|.
  Result<void> remove_tmp(RaftNodeId node);

  std::filesystem::path root_;
};
