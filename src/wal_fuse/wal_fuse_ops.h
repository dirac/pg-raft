#pragma once

#include "src/wal_fuse/fuse_ops.h"
#include "src/wal_fuse/wal_segment.h"

class WalFuseOps : public FuseOps {
 public:
  WalFuseOps(WalTracker& tracker, std::filesystem::path backing_dir)
      : FuseOps(std::move(backing_dir)), tracker_(tracker) {}

  int op_open(char const*, struct fuse_file_info*) override;
  int op_read(char const*, char*, size_t, off_t, struct fuse_file_info*) override;
  int op_write(char const*, char const*, size_t, off_t, struct fuse_file_info*) override;
  int op_fsync(char const*, int, struct fuse_file_info*) override;
  int op_flush(char const* path, struct fuse_file_info* fi) override;
  int op_release(char const*, struct fuse_file_info*) override;

 private:
  WalTracker&                                                     tracker_;
  map<LogSeqNumber, std::tuple<unsigned, shared_ptr<WalSegment>>> segments_;
};
