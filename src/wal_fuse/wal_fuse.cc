#include "src/wal_fuse/wal_fuse.h"

WalFuse::WalFuse(std::filesystem::path mount_point, std::unique_ptr<WalFuseOps> ops)
    : mount_point_(std::move(mount_point)),
      ops_instance_(std::move(ops)),
      ops_(WalFuseOps::make_ops()) {}

WalFuse::~WalFuse() {
  if (mounted_.load())
    stop();
}

bool WalFuse::start(int timeout_ms) {
  struct fuse_args args = FUSE_ARGS_INIT(0, nullptr);
  fuse_opt_add_arg(&args, "wal_fuse");
  // noappledouble: suppress ._ resource-fork files in pg_wal.
  fuse_opt_add_arg(&args, "-onoappledouble");

  chan_ = fuse_mount(mount_point_.c_str(), &args);
  if (!chan_) {
    fuse_opt_free_args(&args);
    return false;
  }

  fuse_ = fuse_new(chan_, &args, &ops_, sizeof(ops_), ops_instance_.get());
  fuse_opt_free_args(&args);
  if (!fuse_) {
    fuse_unmount(mount_point_.c_str(), chan_);
    chan_ = nullptr;
    return false;
  }
  loop_thread_ = std::thread(&WalFuse::run_loop, this);

  // Wait for the mounting.
  // A FUSE mount is live when the mountpoint's st_dev differs from its
  // parent directory's st_dev (the kernel replaced the dir with the FUSE fs).

  auto parent_dir = mount_point_.parent_path();

  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    struct stat st_mp{}, st_parent{};
    if (stat(mount_point_.c_str(), &st_mp) == 0 && stat(parent_dir.c_str(), &st_parent) == 0 &&
        st_mp.st_dev != st_parent.st_dev) {
      mounted_.store(true);
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return false;
}

void WalFuse::stop() {
  if (!mounted_.load())
    return;
  // Unmounting causes the blocking fuse_loop() in the loop thread to return.
  fuse_unmount(mount_point_.c_str(), chan_);
  if (loop_thread_.joinable())
    loop_thread_.join();
  fuse_destroy(fuse_);
  fuse_ = nullptr;
  chan_ = nullptr;
  mounted_.store(false);
}

void WalFuse::run_loop() {
  fuse_loop_mt(fuse_);
}
