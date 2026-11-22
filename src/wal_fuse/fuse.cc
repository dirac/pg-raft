#include "src/wal_fuse/fuse.h"

#include <sys/mount.h>
#include <sys/stat.h>

#include <cstdlib>
#include <cstring>

#include "src/base/logger.h"

Fuse::Fuse(std::filesystem::path mount_point, std::unique_ptr<FuseOps> ops)
    : mount_point_(std::move(mount_point)),
      ops_instance_(std::move(ops)),
      ops_(FuseOps::make_ops()) {}

Fuse::~Fuse() {
  if (mounted_.load())
    stop();
}

namespace {

/// Whether |mp| is still a mount (its device differs from its parent's), or a dead one (a FUSE
/// daemon that was killed leaves it in place: stat fails with ENOTCONN or ENXIO).
bool mounted_or_dead(std::filesystem::path const& mp) {
  struct stat st_mp{}, st_parent{};
  if (stat(mp.c_str(), &st_mp) != 0)
    return errno == ENOTCONN || errno == ENXIO || errno == EIO;
  return stat(mp.parent_path().c_str(), &st_parent) == 0 && st_mp.st_dev != st_parent.st_dev;
}

}  // namespace

void Fuse::remove_stale_mount(std::filesystem::path const& mp) {
  if (!mounted_or_dead(mp))
    return;
  log(WARN, "{} is still mounted (a previous run's FUSE): unmounting it", mp);
#ifdef __APPLE__
  if (::unmount(mp.c_str(), MNT_FORCE) != 0)
    log(WARN, "unmount {}: {}", mp, strerror(errno));
#else
  // Unprivileged: fusermount (lazy, so that it goes even if something still holds it).
  auto cmd = std::format("fusermount -u -z '{}' 2>/dev/null", mp.string());
  if (std::system(cmd.c_str()) != 0)
    log(WARN, "fusermount -u -z {} failed", mp);
#endif
  for (int i = 0; i < 100 && mounted_or_dead(mp); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

bool Fuse::start(int timeout_ms) {
  remove_stale_mount(mount_point_);

  struct fuse_args args = FUSE_ARGS_INIT(0, nullptr);
  fuse_opt_add_arg(&args, "fuse");
#ifdef __APPLE__
  // noappledouble: suppress ._ resource-fork files in pg_wal (macFUSE only).
  fuse_opt_add_arg(&args, "-onoappledouble");
#endif

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
  loop_done_.store(false);
  loop_thread_ = std::thread(&Fuse::run_loop, this);

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

void Fuse::stop() {
  if (!mounted_.load())
    return;
  // Unmounting causes the blocking fuse_loop() in the loop thread to return. A plain unmount fails
  // (EBUSY) while something still has a file open on the volume (a process that outlived
  // PostgreSQL, an indexer): the loop then never returns and the join hangs. After 5 s, force it.
  fuse_unmount(mount_point_.c_str(), chan_);
  for (int i = 0; i < 100 && !loop_done_.load(); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  if (!loop_done_.load()) {
    log(WARN, "{}: the unmount did not finish (busy?): forcing it", mount_point_);
#ifdef __APPLE__
    if (::unmount(mount_point_.c_str(), MNT_FORCE) != 0)
      log(WARN, "unmount {}: {}", mount_point_, strerror(errno));
#else
    auto cmd = std::format("fusermount -u -z '{}' 2>/dev/null", mount_point_.string());
    if (std::system(cmd.c_str()) != 0)
      log(WARN, "fusermount -u -z {} failed", mount_point_);
#endif
  }
  if (loop_thread_.joinable())
    loop_thread_.join();
  fuse_destroy(fuse_);
  fuse_ = nullptr;
  chan_ = nullptr;
  mounted_.store(false);
}

void Fuse::run_loop() {
  if (multithreaded_)
    fuse_loop_mt(fuse_);
  else
    fuse_loop(fuse_);
  loop_done_.store(true);
}
