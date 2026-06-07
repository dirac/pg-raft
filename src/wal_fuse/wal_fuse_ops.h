#pragma once

#include "src/wal_fuse/fuse_ops.h"
#include "src/wal_fuse/wal_segment.h"

class WalFuseOps : public FuseOps {
 public:
  WalFuseOps(WalTracker& tracker, std::filesystem::path backing_dir)
      : FuseOps(std::move(backing_dir)), tracker_(tracker) {}

  int op_open(char const* path, struct fuse_file_info* fi) override;
  int op_read(char const*            path,
              char*                  buff,
              size_t                 size,
              off_t                  offset,
              struct fuse_file_info* fi) override;
  int op_write(char const*            path,
               char const*            buff,
               size_t                 size,
               off_t                  offset,
               struct fuse_file_info* fi) override;
  int op_fsync(char const* path, int datasync, struct fuse_file_info* fi) override;
  int op_flush(char const* path, struct fuse_file_info* fi) override;
  int op_release(char const* path, struct fuse_file_info* fi) override;

 private:
  WalTracker&                                                                      tracker_;
  map<std::tuple<int, LogSeqNumber>, std::tuple<unsigned, shared_ptr<WalSegment>>> segments_;
};
