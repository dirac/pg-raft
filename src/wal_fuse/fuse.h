#pragma once

#include "src/wal_fuse/fuse_ops.h"

// Passthrough FUSE filesystem that mounts at |mount_point|.
//
// The |ops| instance handles all file operations and owns the backing_dir path.
// It is passed as fuse_get_context()->private_data so that virtual method
// dispatch works inside the static FUSE callbacks.
class Fuse {
 public:
  Fuse(std::filesystem::path mount_point, std::unique_ptr<FuseOps> ops);
  ~Fuse();

  Fuse(Fuse const&)            = delete;
  Fuse& operator=(Fuse const&) = delete;

  bool start(int timeout_ms = 5000);
  void stop();
  bool is_mounted() const { return mounted_.load(); }

  FuseOps const& ops() const { return *ops_instance_; }

 private:
  void run_loop();

  std::filesystem::path    mount_point_;
  std::unique_ptr<FuseOps> ops_instance_;
  struct fuse_operations   ops_;

  struct fuse*      fuse_ = nullptr;
  struct fuse_chan* chan_ = nullptr;

  std::thread       loop_thread_;
  std::atomic<bool> mounted_{false};
};
