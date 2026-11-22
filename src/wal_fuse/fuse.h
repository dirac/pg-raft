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

  // Before start(): run libfuse's multithreaded loop (fuse_loop_mt), one request per thread, instead
  // of the single-threaded fuse_loop (the default), which handles one request at a time.
  void set_multithreaded(bool mt) { multithreaded_ = mt; }

  bool start(int timeout_ms = 5000);
  void stop();
  bool is_mounted() const { return mounted_.load(); }

  /// Removes the mount a killed process left at |mount_point| (its FUSE daemon gone: a dead mount,
  /// on which everything fails with ENXIO or ENOTCONN), so that the directory can be used, or
  /// mounted again; waits up to 5 s for it to go. start() does it too.
  static void remove_stale_mount(std::filesystem::path const& mount_point);

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
  std::atomic<bool> loop_done_{false};  // the loop thread's fuse_loop() returned
  bool              multithreaded_ = false;
};
