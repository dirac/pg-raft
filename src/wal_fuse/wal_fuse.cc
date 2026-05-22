#include "src/wal_fuse/wal_fuse.h"

#include <fuse/fuse_opt.h>

WalFuse::~WalFuse() {
  if (mounted_.load()) stop();
}

bool WalFuse::start() {
  struct fuse_args args = FUSE_ARGS_INIT(0, nullptr);
  fuse_opt_add_arg(&args, "wal_fuse");
  // noappledouble: suppress ._ resource-fork files in pg_wal.
  fuse_opt_add_arg(&args, "-onoappledouble");

  chan_ = fuse_mount(mount_point_.c_str(), &args);
  if (!chan_) {
    fuse_opt_free_args(&args);
    return false;
  }

  fuse_ = fuse_new(chan_, &args, &ops_, sizeof(ops_),
                   const_cast<char*>(backing_dir_.c_str()));
  fuse_opt_free_args(&args);
  if (!fuse_) {
    fuse_unmount(mount_point_.c_str(), chan_);
    chan_ = nullptr;
    return false;
  }

  mounted_.store(true);
  loop_thread_ = std::thread(&WalFuse::run_loop, this);
  return true;
}

void WalFuse::stop() {
  if (!mounted_.load()) return;
  // Unmounting causes the blocking fuse_loop() in the loop thread to return.
  fuse_unmount(mount_point_.c_str(), chan_);
  if (loop_thread_.joinable()) loop_thread_.join();
  fuse_destroy(fuse_);
  fuse_ = nullptr;
  chan_ = nullptr;
  mounted_.store(false);
}

void WalFuse::run_loop() {
  fuse_loop(fuse_);
}

