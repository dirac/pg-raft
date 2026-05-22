#define FUSE_USE_VERSION 26
#define _FILE_OFFSET_BITS 64
#include <fuse/fuse.h>
#include <fuse/fuse_opt.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/xattr.h>
#include <unistd.h>

#include <string>

#include "src/wal_fuse/wal_fuse.h"

// ── helpers ───────────────────────────────────────────────────────────────────

static const char* get_backing_dir() {
  return static_cast<const char*>(fuse_get_context()->private_data);
}

static std::string real_path(const char* path) {
  return std::string(get_backing_dir()) + path;
}

// ── FUSE operations ───────────────────────────────────────────────────────────

static int op_getattr(const char* path, struct stat* st) {
  return lstat(real_path(path).c_str(), st) == -1 ? -errno : 0;
}

static int op_fgetattr(const char* path, struct stat* st,
                       struct fuse_file_info* fi) {
  (void)path;
  return fstat(static_cast<int>(fi->fh), st) == -1 ? -errno : 0;
}

static int op_access(const char* path, int mask) {
  return access(real_path(path).c_str(), mask) == -1 ? -errno : 0;
}

static int op_readlink(const char* path, char* buf, size_t size) {
  ssize_t res = readlink(real_path(path).c_str(), buf, size - 1);
  if (res == -1) return -errno;
  buf[res] = '\0';
  return 0;
}

static int op_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                      off_t offset, struct fuse_file_info* fi) {
  (void)offset;
  (void)fi;
  DIR* dp = opendir(real_path(path).c_str());
  if (!dp) return -errno;

  struct dirent* de;
  while ((de = readdir(dp)) != nullptr) {
    struct stat st{};
    st.st_ino = de->d_ino;
    st.st_mode = static_cast<mode_t>(static_cast<unsigned>(de->d_type) << 12u);
    if (filler(buf, de->d_name, &st, 0)) break;
  }
  closedir(dp);
  return 0;
}

static int op_mkdir(const char* path, mode_t mode) {
  return mkdir(real_path(path).c_str(), mode) == -1 ? -errno : 0;
}

static int op_unlink(const char* path) {
  return unlink(real_path(path).c_str()) == -1 ? -errno : 0;
}

static int op_rmdir(const char* path) {
  return rmdir(real_path(path).c_str()) == -1 ? -errno : 0;
}

static int op_symlink(const char* target, const char* path) {
  return symlink(target, real_path(path).c_str()) == -1 ? -errno : 0;
}

static int op_rename(const char* from, const char* to) {
  return rename(real_path(from).c_str(), real_path(to).c_str()) == -1
             ? -errno
             : 0;
}

static int op_link(const char* from, const char* to) {
  return link(real_path(from).c_str(), real_path(to).c_str()) == -1 ? -errno
                                                                     : 0;
}

static int op_chmod(const char* path, mode_t mode) {
  return chmod(real_path(path).c_str(), mode) == -1 ? -errno : 0;
}

static int op_chown(const char* path, uid_t uid, gid_t gid) {
  return lchown(real_path(path).c_str(), uid, gid) == -1 ? -errno : 0;
}

static int op_truncate(const char* path, off_t size) {
  return truncate(real_path(path).c_str(), size) == -1 ? -errno : 0;
}

static int op_ftruncate(const char* path, off_t size,
                        struct fuse_file_info* fi) {
  (void)path;
  return ftruncate(static_cast<int>(fi->fh), size) == -1 ? -errno : 0;
}

static int op_utimens(const char* path, const struct timespec ts[2]) {
  return utimensat(AT_FDCWD, real_path(path).c_str(), ts,
                   AT_SYMLINK_NOFOLLOW) == -1
             ? -errno
             : 0;
}

static int op_create(const char* path, mode_t mode, struct fuse_file_info* fi) {
  int fd = open(real_path(path).c_str(), fi->flags, mode);
  if (fd == -1) return -errno;
  fi->fh = static_cast<uint64_t>(fd);
  return 0;
}

static int op_open(const char* path, struct fuse_file_info* fi) {
  int fd = open(real_path(path).c_str(), fi->flags);
  if (fd == -1) return -errno;
  fi->fh = static_cast<uint64_t>(fd);
  return 0;
}

static int op_read(const char* path, char* buf, size_t size, off_t offset,
                   struct fuse_file_info* fi) {
  (void)path;
  ssize_t res = pread(static_cast<int>(fi->fh), buf, size, offset);
  return res == -1 ? -errno : static_cast<int>(res);
}

static int op_write(const char* path, const char* buf, size_t size,
                    off_t offset, struct fuse_file_info* fi) {
  (void)path;
  ssize_t res = pwrite(static_cast<int>(fi->fh), buf, size, offset);
  return res == -1 ? -errno : static_cast<int>(res);
}

static int op_statfs(const char* path, struct statvfs* stbuf) {
  return statvfs(real_path(path).c_str(), stbuf) == -1 ? -errno : 0;
}

static int op_flush(const char* path, struct fuse_file_info* fi) {
  (void)path;
  (void)fi;
  return 0;
}

static int op_release(const char* path, struct fuse_file_info* fi) {
  (void)path;
  close(static_cast<int>(fi->fh));
  return 0;
}

static int op_fsync(const char* path, int datasync, struct fuse_file_info* fi) {
  (void)path;
  (void)datasync;
  return fsync(static_cast<int>(fi->fh)) == -1 ? -errno : 0;
}

// macOS xattr signatures include a `position` parameter for resource forks.
static int op_setxattr(const char* path, const char* name, const char* value,
                       size_t size, int options, uint32_t position) {
  int res =
      setxattr(real_path(path).c_str(), name, value, size, position, options);
  return res == -1 ? -errno : 0;
}

static int op_getxattr(const char* path, const char* name, char* value,
                       size_t size, uint32_t position) {
  ssize_t res = getxattr(real_path(path).c_str(), name, value, size, position,
                         XATTR_NOFOLLOW);
  return res == -1 ? -errno : static_cast<int>(res);
}

static int op_listxattr(const char* path, char* list, size_t size) {
  ssize_t res = listxattr(real_path(path).c_str(), list, size, XATTR_NOFOLLOW);
  return res == -1 ? -errno : static_cast<int>(res);
}

static int op_removexattr(const char* path, const char* name) {
  return removexattr(real_path(path).c_str(), name, XATTR_NOFOLLOW) == -1
             ? -errno
             : 0;
}

// clang-format off
static const struct fuse_operations kOps = {
  .getattr     = op_getattr,
  .readlink    = op_readlink,
  .readdir     = op_readdir,
  .mkdir       = op_mkdir,
  .unlink      = op_unlink,
  .rmdir       = op_rmdir,
  .symlink     = op_symlink,
  .rename      = op_rename,
  .link        = op_link,
  .chmod       = op_chmod,
  .chown       = op_chown,
  .truncate    = op_truncate,
  .open        = op_open,
  .read        = op_read,
  .write       = op_write,
  .statfs      = op_statfs,
  .flush       = op_flush,
  .release     = op_release,
  .fsync       = op_fsync,
  .setxattr    = op_setxattr,
  .getxattr    = op_getxattr,
  .listxattr   = op_listxattr,
  .removexattr = op_removexattr,
  .access      = op_access,
  .create      = op_create,
  .ftruncate   = op_ftruncate,
  .fgetattr    = op_fgetattr,
  .utimens     = op_utimens,
};
// clang-format on

// ── WalFuse ───────────────────────────────────────────────────────────────────

WalFuse::WalFuse(std::string mount_point, std::string backing_dir)
    : mount_point_(std::move(mount_point)),
      backing_dir_(std::move(backing_dir)) {}

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
