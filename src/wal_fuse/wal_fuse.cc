#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif
#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif
#include <fuse/fuse.h>
#include <fuse/fuse_opt.h>

#include "src/wal_fuse/wal_fuse.h"
#include "src/wal_fuse/wal_fuse_ops.h"

template <typename Ops>
WalFuse<Ops>::WalFuse(std::string mount_point, std::string backing_dir)
    : mount_point_(std::move(mount_point)),
      backing_dir_(std::move(backing_dir)) {}

template <typename Ops>
WalFuse<Ops>::~WalFuse() {
  if (mounted_.load()) stop();
}

template <typename Ops>
bool WalFuse<Ops>::start() {
  struct fuse_args args = FUSE_ARGS_INIT(0, nullptr);
  fuse_opt_add_arg(&args, "wal_fuse");
  // noappledouble: suppress ._ resource-fork files in pg_wal.
  fuse_opt_add_arg(&args, "-onoappledouble");

  chan_ = fuse_mount(mount_point_.c_str(), &args);
  if (!chan_) {
    fuse_opt_free_args(&args);
    return false;
  }

  static const struct fuse_operations kOps = Ops::make_ops();
  fuse_ = fuse_new(chan_, &args, &kOps, sizeof(kOps),
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

template <typename Ops>
void WalFuse<Ops>::stop() {
  if (!mounted_.load()) return;
  // Unmounting causes the blocking fuse_loop() in the loop thread to return.
  fuse_unmount(mount_point_.c_str(), chan_);
  if (loop_thread_.joinable()) loop_thread_.join();
  fuse_destroy(fuse_);
  fuse_ = nullptr;
  chan_ = nullptr;
  mounted_.store(false);
}

template <typename Ops>
void WalFuse<Ops>::run_loop() {
  fuse_loop(fuse_);
}

template class WalFuse<WalFuseOpBase>;
