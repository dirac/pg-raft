#pragma once

#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif
#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif
#include <fuse/fuse.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <filesystem>

#include "src/wal_fuse/wal_fuse_ops.h"

// Passthrough FUSE filesystem that mounts at |mount_point|.
//
// The |ops| instance handles all file operations and owns the backing_dir path.
// It is passed as fuse_get_context()->private_data so that virtual method
// dispatch works inside the static FUSE callbacks.
class WalFuse {
 public:
  WalFuse(std::filesystem::path mount_point, std::unique_ptr<WalFuseOpBase> ops);
  ~WalFuse();

  WalFuse(WalFuse const&)            = delete;
  WalFuse& operator=(WalFuse const&) = delete;

  bool start(int timeout_ms = 5000);
  void stop();
  bool is_mounted() const { return mounted_.load(); }

 private:
  void run_loop();

  std::filesystem::path          mount_point_;
  std::unique_ptr<WalFuseOpBase> ops_instance_;
  struct fuse_operations         ops_;

  struct fuse*      fuse_ = nullptr;
  struct fuse_chan* chan_ = nullptr;

  std::thread       loop_thread_;
  std::atomic<bool> mounted_{false};
};
