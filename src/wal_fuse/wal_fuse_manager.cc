#include "src/wal_fuse/wal_fuse_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <thread>

static std::vector<std::string> build_pg_args(
    const std::string& data_dir, const std::vector<std::string>& extra) {
  std::vector<std::string> args = {"-D", data_dir};
  args.insert(args.end(), extra.begin(), extra.end());
  return args;
}

WalFuseManager::WalFuseManager(std::string pg_bin, std::string data_dir,
                                std::vector<std::string> extra_args)
    : pg_bin_(std::move(pg_bin)),
      data_dir_(std::move(data_dir)),
      extra_args_(std::move(extra_args)),
      fuse_(data_dir_ + "/pg_wal", data_dir_ + "/pg_raft_wal"),
      postgres_(pg_bin_, build_pg_args(data_dir_, extra_args_)) {}

WalFuseManager::~WalFuseManager() {
  stop();
}

bool WalFuseManager::start() {

  if (!fuse_.start()) return false;

  if (!wait_for_mount(5000)) {
    fuse_.stop();
    return false;
  }

  if (!postgres_.start()) {
    fuse_.stop();
    return false;
  }

  return true;
}

void WalFuseManager::stop() {
  if (postgres_.is_running()) postgres_.stop();
  if (fuse_.is_mounted()) fuse_.stop();
}

bool WalFuseManager::wait_for_mount(int timeout_ms) {
  // A FUSE mount is live when the mountpoint's st_dev differs from its
  // parent directory's st_dev (the kernel replaced the dir with the FUSE fs).
  const std::string mount_point = data_dir_ + "/pg_wal";

  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    struct stat st_mp{}, st_parent{};
    if (stat(mount_point.c_str(), &st_mp) == 0 &&
        stat(data_dir_.c_str(), &st_parent) == 0 &&
        st_mp.st_dev != st_parent.st_dev) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return false;
}
