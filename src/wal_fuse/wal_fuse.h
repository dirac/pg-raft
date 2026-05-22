#pragma once

#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif
#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif
#include <fuse/fuse.h>

#include <atomic>
#include <string>
#include <thread>

// Passthrough FUSE filesystem that mounts at |mount_point| and redirects all
// file operations to |backing_dir|.
//
// Ops must provide a static make_ops() returning struct fuse_operations.
// Pass Ops{} as the third constructor argument — it is used only to deduce
// the template parameter and call Ops::make_ops() at construction time.
class WalFuse {
 public:
  template <typename Ops>
  WalFuse(std::string mount_point, std::string backing_dir, Ops)
      : mount_point_(std::move(mount_point)),
        backing_dir_(std::move(backing_dir)),
        ops_(Ops::make_ops()) {}

  ~WalFuse();
  WalFuse(const WalFuse&) = delete;
  WalFuse& operator=(const WalFuse&) = delete;

  bool start();
  void stop();
  bool is_mounted() const { return mounted_.load(); }

 private:
  void run_loop();

  std::string mount_point_;
  std::string backing_dir_;
  struct fuse_operations ops_;

  struct fuse* fuse_ = nullptr;
  struct fuse_chan* chan_ = nullptr;

  std::thread loop_thread_;
  std::atomic<bool> mounted_{false};
};
