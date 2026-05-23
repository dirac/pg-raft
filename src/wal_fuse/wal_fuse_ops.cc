#include "src/wal_fuse/wal_fuse_ops.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/xattr.h>
#include <unistd.h>

#include <string>

#include "src/base/logger.h"

// ── helpers ───────────────────────────────────────────────────────────────────

const char* WalFuseOpBase::backing_dir() {
  return static_cast<const char*>(fuse_get_context()->private_data);
}

std::string WalFuseOpBase::real_path(const char* path) {
  return std::string(backing_dir()) + path;
}

// ── operations ────────────────────────────────────────────────────────────────

int WalFuseOpBase::op_getattr(const char* path, struct stat* st) {
  logger::debug("getattr {}", path);
  return lstat(real_path(path).c_str(), st) == -1 ? -errno : 0;
}

int WalFuseOpBase::op_fgetattr(const char* path, struct stat* st,
                                struct fuse_file_info* fi) {
  logger::debug("fgetattr {} fd={}", path, fi->fh);
  return fstat(static_cast<int>(fi->fh), st) == -1 ? -errno : 0;
}

int WalFuseOpBase::op_access(const char* path, int mask) {
  logger::debug("access {} mask={}", path, mask);
  return access(real_path(path).c_str(), mask) == -1 ? -errno : 0;
}

int WalFuseOpBase::op_readlink(const char* path, char* buf, size_t size) {
  logger::debug("readlink {}", path);
  ssize_t res = readlink(real_path(path).c_str(), buf, size - 1);
  if (res == -1) return -errno;
  buf[res] = '\0';
  return 0;
}

int WalFuseOpBase::op_readdir(const char* path, void* buf,
                               fuse_fill_dir_t filler, off_t offset,
                               struct fuse_file_info* fi) {
  logger::debug("readdir {}", path);
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

int WalFuseOpBase::op_mkdir(const char* path, mode_t mode) {
  logger::debug("mkdir {} mode={:o}", path, mode);
  return mkdir(real_path(path).c_str(), mode) == -1 ? -errno : 0;
}

int WalFuseOpBase::op_unlink(const char* path) {
  logger::debug("unlink {}", path);
  return unlink(real_path(path).c_str()) == -1 ? -errno : 0;
}

int WalFuseOpBase::op_rmdir(const char* path) {
  logger::debug("rmdir {}", path);
  return rmdir(real_path(path).c_str()) == -1 ? -errno : 0;
}

int WalFuseOpBase::op_symlink(const char* target, const char* path) {
  logger::debug("symlink {} -> {}", path, target);
  return symlink(target, real_path(path).c_str()) == -1 ? -errno : 0;
}

int WalFuseOpBase::op_rename(const char* from, const char* to) {
  logger::debug("rename {} -> {}", from, to);
  return rename(real_path(from).c_str(), real_path(to).c_str()) == -1
             ? -errno
             : 0;
}

int WalFuseOpBase::op_link(const char* from, const char* to) {
  logger::debug("link {} -> {}", from, to);
  return link(real_path(from).c_str(), real_path(to).c_str()) == -1 ? -errno
                                                                     : 0;
}

int WalFuseOpBase::op_chmod(const char* path, mode_t mode) {
  logger::debug("chmod {} mode={:o}", path, mode);
  return chmod(real_path(path).c_str(), mode) == -1 ? -errno : 0;
}

int WalFuseOpBase::op_chown(const char* path, uid_t uid, gid_t gid) {
  logger::debug("chown {} uid={} gid={}", path, uid, gid);
  return lchown(real_path(path).c_str(), uid, gid) == -1 ? -errno : 0;
}

int WalFuseOpBase::op_truncate(const char* path, off_t size) {
  logger::debug("truncate {} size={}", path, size);
  return truncate(real_path(path).c_str(), size) == -1 ? -errno : 0;
}

int WalFuseOpBase::op_ftruncate(const char* path, off_t size,
                                 struct fuse_file_info* fi) {
  logger::debug("ftruncate {} size={} fd={}", path, size, fi->fh);
  return ftruncate(static_cast<int>(fi->fh), size) == -1 ? -errno : 0;
}

int WalFuseOpBase::op_utimens(const char* path, const struct timespec ts[2]) {
  logger::debug("utimens {}", path);
  return utimensat(AT_FDCWD, real_path(path).c_str(), ts,
                   AT_SYMLINK_NOFOLLOW) == -1
             ? -errno
             : 0;
}

int WalFuseOpBase::op_create(const char* path, mode_t mode,
                              struct fuse_file_info* fi) {
  logger::debug("create {} mode={:o}", path, mode);
  int fd = open(real_path(path).c_str(), fi->flags, mode);
  if (fd == -1) return -errno;
  fi->fh = static_cast<uint64_t>(fd);
  return 0;
}

int WalFuseOpBase::op_open(const char* path, struct fuse_file_info* fi) {
  logger::debug("open {}", path);
  int fd = open(real_path(path).c_str(), fi->flags);
  if (fd == -1) return -errno;
  fi->fh = static_cast<uint64_t>(fd);
  return 0;
}

int WalFuseOpBase::op_read(const char* path, char* buf, size_t size,
                            off_t offset, struct fuse_file_info* fi) {
  logger::debug("read {} size={} off={}", path, size, offset);
  ssize_t res = pread(static_cast<int>(fi->fh), buf, size, offset);
  return res == -1 ? -errno : static_cast<int>(res);
}

int WalFuseOpBase::op_write(const char* path, const char* buf, size_t size,
                             off_t offset, struct fuse_file_info* fi) {
  logger::debug("write {} size={} off={}", path, size, offset);
  ssize_t res = pwrite(static_cast<int>(fi->fh), buf, size, offset);
  return res == -1 ? -errno : static_cast<int>(res);
}

int WalFuseOpBase::op_statfs(const char* path, struct statvfs* stbuf) {
  logger::debug("statfs {}", path);
  return statvfs(real_path(path).c_str(), stbuf) == -1 ? -errno : 0;
}

int WalFuseOpBase::op_flush(const char* path, struct fuse_file_info* fi) {
  logger::debug("flush {} fd={}", path, fi->fh);
  return 0;
}

int WalFuseOpBase::op_release(const char* path, struct fuse_file_info* fi) {
  logger::debug("release {} fd={}", path, fi->fh);
  close(static_cast<int>(fi->fh));
  return 0;
}

int WalFuseOpBase::op_fsync(const char* path, int datasync,
                             struct fuse_file_info* fi) {
  logger::debug("fsync {} datasync={} fd={}", path, datasync, fi->fh);
  return fsync(static_cast<int>(fi->fh)) == -1 ? -errno : 0;
}

int WalFuseOpBase::op_setxattr(const char* path, const char* name,
                                const char* value, size_t size, int options,
                                uint32_t position) {
  logger::debug("setxattr {} {}", path, name);
  return setxattr(real_path(path).c_str(), name, value, size, position,
                  options) == -1
             ? -errno
             : 0;
}

int WalFuseOpBase::op_getxattr(const char* path, const char* name, char* value,
                                size_t size, uint32_t position) {
  logger::debug("getxattr {} {}", path, name);
  ssize_t res = getxattr(real_path(path).c_str(), name, value, size, position,
                         XATTR_NOFOLLOW);
  return res == -1 ? -errno : static_cast<int>(res);
}

int WalFuseOpBase::op_listxattr(const char* path, char* list, size_t size) {
  logger::debug("listxattr {}", path);
  ssize_t res = listxattr(real_path(path).c_str(), list, size, XATTR_NOFOLLOW);
  return res == -1 ? -errno : static_cast<int>(res);
}

int WalFuseOpBase::op_removexattr(const char* path, const char* name) {
  logger::debug("removexattr {} {}", path, name);
  return removexattr(real_path(path).c_str(), name, XATTR_NOFOLLOW) == -1
             ? -errno
             : 0;
}

// ── make_ops ─────────────────────────────────────────────────────────────────

// clang-format off
struct fuse_operations WalFuseOpBase::make_ops() {
  struct fuse_operations ops{};
  ops.getattr     = op_getattr;
  ops.readlink    = op_readlink;
  ops.mkdir       = op_mkdir;
  ops.unlink      = op_unlink;
  ops.rmdir       = op_rmdir;
  ops.symlink     = op_symlink;
  ops.rename      = op_rename;
  ops.link        = op_link;
  ops.chmod       = op_chmod;
  ops.chown       = op_chown;
  ops.truncate    = op_truncate;
  ops.open        = op_open;
  ops.read        = op_read;
  ops.write       = op_write;
  ops.statfs      = op_statfs;
  ops.flush       = op_flush;
  ops.release     = op_release;
  ops.fsync       = op_fsync;
  ops.setxattr    = op_setxattr;
  ops.getxattr    = op_getxattr;
  ops.listxattr   = op_listxattr;
  ops.removexattr = op_removexattr;
  ops.access      = op_access;
  ops.create      = op_create;
  ops.ftruncate   = op_ftruncate;
  ops.fgetattr    = op_fgetattr;
  ops.utimens     = op_utimens;
  ops.readdir     = op_readdir;
  return ops;
}
// clang-format on
