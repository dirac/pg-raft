#pragma once

#include "src/wal_fuse/fuse_ops.h"
#include "src/wal_fuse/wal_file.h"

class WalFuseOps : public FuseOps {
  int op_open(char const*, struct fuse_file_info*) final;
  int op_read(char const*, char*, size_t, off_t, struct fuse_file_info*) override;
  int op_write(char const*, char const*, size_t, off_t, struct fuse_file_info*) override;
  int op_fsync(char const*, int, struct fuse_file_info*) override;
  int op_flush(char const* path, struct fuse_file_info* fi) override;
  int op_release(char const*, struct fuse_file_info*) final ;

 private:
  map<std::tuple<uint32_t, uint64_t>, std::tuple<unsigned, shared_ptr<WalSegment>>> files_;
};
