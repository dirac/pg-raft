#pragma once

#include <atomic>
#include <string>
#include <thread>

struct fuse;
struct fuse_chan;

// Passthrough FUSE filesystem that mounts at |mount_point| and redirects all
// file operations to |backing_dir|.
//
// Ops must provide a static make_ops() returning a fuse_operations struct.
//
// Call start() to mount and launch the FUSE event loop in a background thread.
// Call stop() to unmount and join the thread.
template <typename Ops>
class WalFuse {
 public:
  WalFuse(std::string mount_point, std::string backing_dir);
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

  struct fuse* fuse_ = nullptr;
  struct fuse_chan* chan_ = nullptr;

  std::thread loop_thread_;
  std::atomic<bool> mounted_{false};
};

// Suppress implicit instantiation in all other translation units.
class WalFuseOpBase;
extern template class WalFuse<WalFuseOpBase>;
