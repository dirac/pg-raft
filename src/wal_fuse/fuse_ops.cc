#include "src/wal_fuse/fuse_ops.h"

#include "src/base/logger.h"

// ── helpers ───────────────────────────────────────────────────────────────────

static FuseOps* ctx() {
  return static_cast<FuseOps*>(fuse_get_context()->private_data);
}

FuseOps::FuseOps(std::filesystem::path backing_dir) : backing_dir_(std::move(backing_dir)) {}

std::filesystem::path FuseOps::real_path(char const* path) const {
  return backing_dir_ / (*path == '/' ? path + 1 : path);
}

// ── operations ────────────────────────────────────────────────────────────────

int FuseOps::op_getattr(char const* path, struct stat* st) {
  log(DEBUG, "getattr {}", path);
  n_getattr.fetch_add(1, std::memory_order_relaxed);

  return lstat(real_path(path).c_str(), st) == -1 ? -errno : 0;
}

int FuseOps::op_fgetattr(char const* path, struct stat* st, struct fuse_file_info* fi) {
  log(DEBUG, "fgetattr {} fd={}", path, fi->fh);
  n_fgetattr.fetch_add(1, std::memory_order_relaxed);

  return fstat(static_cast<int>(fi->fh), st) == -1 ? -errno : 0;
}

int FuseOps::op_access(char const* path, int mask) {
  log(DEBUG, "access {} mask={}", path, mask);
  n_access.fetch_add(1, std::memory_order_relaxed);

  return access(real_path(path).c_str(), mask) == -1 ? -errno : 0;
}

int FuseOps::op_readlink(char const* path, char* buf, size_t size) {
  log(DEBUG, "readlink {}", path);
  n_readlink.fetch_add(1, std::memory_order_relaxed);

  ssize_t res = readlink(real_path(path).c_str(), buf, size - 1);
  if (res == -1)
    return -errno;
  buf[res] = '\0';
  return 0;
}

namespace {
struct DirInfo {
  DIR* const     dp;
  struct dirent* de;
  off_t          offset;
};
}  // namespace

int FuseOps::op_opendir(char const* path, struct fuse_file_info* fi) {
  log(DEBUG, "opendir {}", path);
  n_opendir.fetch_add(1, std::memory_order_relaxed);

  DIR* dp = opendir(real_path(path).c_str());
  if (!dp)
    return -errno;

  auto* fh = new DirInfo{.dp = dp, .de = nullptr, .offset = 0};
  fi->fh   = reinterpret_cast<uint64_t>(fh);
  return 0;
}

int FuseOps::op_releasedir(char const* path, struct fuse_file_info* fi) {
  log(DEBUG, "releasedir {}", path);
  n_releasedir.fetch_add(1, std::memory_order_relaxed);

  auto* fh = reinterpret_cast<DirInfo*>(fi->fh);

  ::closedir(fh->dp);
  delete fh;

  return 0;
}

int FuseOps::op_readdir(char const*            path,
                        void*                  buf,
                        fuse_fill_dir_t        filler,
                        off_t                  offset,
                        struct fuse_file_info* fi) {
  log(DEBUG, "readdir {} {}", path, offset);
  n_readdir.fetch_add(1, std::memory_order_relaxed);

  auto* fh = reinterpret_cast<DirInfo*>(fi->fh);

  if (fh->de && fh->offset == offset) {
    struct stat st{};
    st.st_ino  = fh->de->d_ino;
    st.st_mode = static_cast<mode_t>(static_cast<unsigned>(fh->de->d_type) << 12u);
    if (filler(buf, fh->de->d_name, &st, ++offset)) {
      return 0;
    }
  }

  struct dirent* de;
  while ((de = readdir(fh->dp)) != nullptr) {
    fh->de     = de;
    fh->offset = offset;

    struct stat st{};
    st.st_ino  = de->d_ino;
    st.st_mode = static_cast<mode_t>(static_cast<unsigned>(de->d_type) << 12u);
    if (filler(buf, de->d_name, &st, ++offset))
      return 0;
  }
  return 0;
}

int FuseOps::op_mkdir(char const* path, mode_t mode) {
  log(DEBUG, "mkdir {} mode={:o}", path, mode);
  n_mkdir.fetch_add(1, std::memory_order_relaxed);

  return mkdir(real_path(path).c_str(), mode) == -1 ? -errno : 0;
}

int FuseOps::op_unlink(char const* path) {
  log(DEBUG, "unlink {}", path);
  n_unlink.fetch_add(1, std::memory_order_relaxed);

  return unlink(real_path(path).c_str()) == -1 ? -errno : 0;
}

int FuseOps::op_rmdir(char const* path) {
  log(DEBUG, "rmdir {}", path);
  n_rmdir.fetch_add(1, std::memory_order_relaxed);

  return rmdir(real_path(path).c_str()) == -1 ? -errno : 0;
}

int FuseOps::op_symlink(char const* target, char const* path) {
  log(DEBUG, "symlink {} -> {}", path, target);
  n_symlink.fetch_add(1, std::memory_order_relaxed);

  return symlink(target, real_path(path).c_str()) == -1 ? -errno : 0;
}

int FuseOps::op_rename(char const* from, char const* to) {
  log(DEBUG, "rename {} -> {}", from, to);
  n_rename.fetch_add(1, std::memory_order_relaxed);

  return rename(real_path(from).c_str(), real_path(to).c_str()) == -1 ? -errno : 0;
}

int FuseOps::op_link(char const* from, char const* to) {
  log(DEBUG, "link {} -> {}", from, to);
  n_link.fetch_add(1, std::memory_order_relaxed);

  return link(real_path(from).c_str(), real_path(to).c_str()) == -1 ? -errno : 0;
}

int FuseOps::op_chmod(char const* path, mode_t mode) {
  log(DEBUG, "chmod {} mode={:o}", path, mode);
  n_chmod.fetch_add(1, std::memory_order_relaxed);

  return chmod(real_path(path).c_str(), mode) == -1 ? -errno : 0;
}

int FuseOps::op_chown(char const* path, uid_t uid, gid_t gid) {
  log(DEBUG, "chown {} uid={} gid={}", path, uid, gid);
  n_chown.fetch_add(1, std::memory_order_relaxed);

  return lchown(real_path(path).c_str(), uid, gid) == -1 ? -errno : 0;
}

int FuseOps::op_truncate(char const* path, off_t size) {
  log(DEBUG, "truncate {} size={}", path, size);
  n_truncate.fetch_add(1, std::memory_order_relaxed);

  return truncate(real_path(path).c_str(), size) == -1 ? -errno : 0;
}

int FuseOps::op_ftruncate(char const* path, off_t size, struct fuse_file_info* fi) {
  log(DEBUG, "ftruncate {} size={} fd={}", path, size, fi->fh);
  n_ftruncate.fetch_add(1, std::memory_order_relaxed);

  return ftruncate(static_cast<int>(fi->fh), size) == -1 ? -errno : 0;
}

int FuseOps::op_utimens(char const* path, const struct timespec ts[2]) {
  log(DEBUG, "utimens {}", path);
  n_utimens.fetch_add(1, std::memory_order_relaxed);

  return utimensat(AT_FDCWD, real_path(path).c_str(), ts, AT_SYMLINK_NOFOLLOW) == -1 ? -errno : 0;
}

int FuseOps::op_create(char const* path, mode_t mode, struct fuse_file_info* fi) {
  log(DEBUG, "create {} mode={:o}", path, mode);
  n_create.fetch_add(1, std::memory_order_relaxed);

  int fd = open(real_path(path).c_str(), fi->flags, mode);
  if (fd == -1)
    return -errno;
  fi->fh = static_cast<uint64_t>(fd);
  return 0;
}

int FuseOps::op_open(char const* path, struct fuse_file_info* fi) {
  log(DEBUG, "open {}", path);
  n_open.fetch_add(1, std::memory_order_relaxed);

  int fd = open(real_path(path).c_str(), fi->flags);
  if (fd == -1)
    return -errno;

  fi->fh = static_cast<uint64_t>(fd);

  TEST_CONCURRENCY;
  return 0;
}

int FuseOps::op_read(char const*            path,
                     char*                  buf,
                     size_t                 size,
                     off_t                  offset,
                     struct fuse_file_info* fi) {
  TEST_CONCURRENCY;

  log(DEBUG, "read {} size={} off={}", path, size, offset);
  n_read.fetch_add(1, std::memory_order_relaxed);

  ssize_t res = pread(static_cast<int>(fi->fh), buf, size, offset);
  return res == -1 ? -errno : static_cast<int>(res);
}

int FuseOps::op_write(char const*            path,
                      char const*            buf,
                      size_t                 size,
                      off_t                  offset,
                      struct fuse_file_info* fi) {
  TEST_CONCURRENCY;

  log(DEBUG, "write {} size={} off={}", path, size, offset);
  n_write.fetch_add(1, std::memory_order_relaxed);

  ssize_t res = pwrite(static_cast<int>(fi->fh), buf, size, offset);
  return res == -1 ? -errno : static_cast<int>(res);
}

int FuseOps::op_statfs(char const* path, struct statvfs* stbuf) {
  log(DEBUG, "statfs {}", path);
  n_statfs.fetch_add(1, std::memory_order_relaxed);

  return statvfs(real_path(path).c_str(), stbuf) == -1 ? -errno : 0;
}

int FuseOps::op_flush(char const* path, struct fuse_file_info* fi) {
  TEST_CONCURRENCY;

  log(DEBUG, "flush {} fd={}", path, fi->fh);
  n_flush.fetch_add(1, std::memory_order_relaxed);

  return 0;
}

int FuseOps::op_release(char const* path, struct fuse_file_info* fi) {
  TEST_CONCURRENCY;

  log(DEBUG, "release {} fd={}", path, fi->fh);
  n_release.fetch_add(1, std::memory_order_relaxed);

  close(static_cast<int>(fi->fh));
  return 0;
}

int FuseOps::op_fsync(char const* path, int datasync, struct fuse_file_info* fi) {
  TEST_CONCURRENCY;

  log(DEBUG, "fsync {} datasync={} fd={}", path, datasync, fi->fh);
  n_fsync.fetch_add(1, std::memory_order_relaxed);

  return fsync(static_cast<int>(fi->fh)) == -1 ? -errno : 0;
}

int FuseOps::op_setxattr(char const* path,
                         char const* name,
                         char const* value,
                         size_t      size,
                         int         options,
                         uint32_t    position) {
  log(DEBUG, "setxattr {} {}", path, name);
  n_setxattr.fetch_add(1, std::memory_order_relaxed);

  return setxattr(real_path(path).c_str(), name, value, size, position, options) == -1 ? -errno : 0;
}

int FuseOps::op_getxattr(char const* path,
                         char const* name,
                         char*       value,
                         size_t      size,
                         uint32_t    position) {
  log(DEBUG, "getxattr {} {}", path, name);
  n_getxattr.fetch_add(1, std::memory_order_relaxed);

  ssize_t res = getxattr(real_path(path).c_str(), name, value, size, position, XATTR_NOFOLLOW);
  return res == -1 ? -errno : static_cast<int>(res);
}

int FuseOps::op_listxattr(char const* path, char* list, size_t size) {
  log(DEBUG, "listxattr {}", path);
  n_listxattr.fetch_add(1, std::memory_order_relaxed);

  ssize_t res = listxattr(real_path(path).c_str(), list, size, XATTR_NOFOLLOW);
  return res == -1 ? -errno : static_cast<int>(res);
}

int FuseOps::op_removexattr(char const* path, char const* name) {
  log(DEBUG, "removexattr {} {}", path, name);
  n_removexattr.fetch_add(1, std::memory_order_relaxed);

  return removexattr(real_path(path).c_str(), name, XATTR_NOFOLLOW) == -1 ? -errno : 0;
}

// ── make_ops ─────────────────────────────────────────────────────────────────
//
// Each entry is a captureless lambda that casts private_data to FuseOps*
// and calls the virtual method, enabling subclass dispatch.

static void* fuse_init(struct fuse_conn_info* conn) {
  // 128 KB is the absolute max buffer payload allowed by libfuse v2
  conn->max_write = 128 * 1024;
  return ctx();
}

// clang-format off
struct fuse_operations FuseOps::make_ops() {
  struct fuse_operations ops{};
  ops.init        = fuse_init,
  ops.getattr     = [](const char* p, struct stat* s)                                                { return ctx()->op_getattr(p, s); };
  ops.fgetattr    = [](const char* p, struct stat* s, struct fuse_file_info* f)                      { return ctx()->op_fgetattr(p, s, f); };
  ops.access      = [](const char* p, int m)                                                         { return ctx()->op_access(p, m); };
  ops.readlink    = [](const char* p, char* b, size_t n)                                             { return ctx()->op_readlink(p, b, n); };
  ops.readdir     = [](const char* p, void* b, fuse_fill_dir_t f, off_t o, struct fuse_file_info* i) { return ctx()->op_readdir(p, b, f, o, i); };
  ops.opendir     = [](const char* p, struct fuse_file_info* i)                                      { return ctx()->op_opendir(p, i); };
  ops.releasedir  = [](const char* p, struct fuse_file_info* i)                                      { return ctx()->op_releasedir(p, i); };
  ops.mkdir       = [](const char* p, mode_t m)                                                      { return ctx()->op_mkdir(p, m); };
  ops.unlink      = [](const char* p)                                                                { return ctx()->op_unlink(p); };
  ops.rmdir       = [](const char* p)                                                                { return ctx()->op_rmdir(p); };
  ops.symlink     = [](const char* t, const char* p)                                                 { return ctx()->op_symlink(t, p); };
  ops.rename      = [](const char* a, const char* b)                                                 { return ctx()->op_rename(a, b); };
  ops.link        = [](const char* a, const char* b)                                                 { return ctx()->op_link(a, b); };
  ops.chmod       = [](const char* p, mode_t m)                                                      { return ctx()->op_chmod(p, m); };
  ops.chown       = [](const char* p, uid_t u, gid_t g)                                              { return ctx()->op_chown(p, u, g); };
  ops.truncate    = [](const char* p, off_t o)                                                       { return ctx()->op_truncate(p, o); };
  ops.ftruncate   = [](const char* p, off_t o, struct fuse_file_info* f)                             { return ctx()->op_ftruncate(p, o, f); };
  ops.utimens     = [](const char* p, const struct timespec t[2])                                    { return ctx()->op_utimens(p, t); };
  ops.create      = [](const char* p, mode_t m, struct fuse_file_info* f)                            { return ctx()->op_create(p, m, f); };
  ops.open        = [](const char* p, struct fuse_file_info* f)                                      { return ctx()->op_open(p, f); };
  ops.read        = [](const char* p, char* b, size_t n, off_t o, struct fuse_file_info* f)          { return ctx()->op_read(p, b, n, o, f); };
  ops.write       = [](const char* p, const char* b, size_t n, off_t o, struct fuse_file_info* f)    { return ctx()->op_write(p, b, n, o, f); };
  ops.statfs      = [](const char* p, struct statvfs* s)                                             { return ctx()->op_statfs(p, s); };
  ops.flush       = [](const char* p, struct fuse_file_info* f)                                      { return ctx()->op_flush(p, f); };
  ops.release     = [](const char* p, struct fuse_file_info* f)                                      { return ctx()->op_release(p, f); };
  ops.fsync       = [](const char* p, int d, struct fuse_file_info* f)                               { return ctx()->op_fsync(p, d, f); };
  ops.setxattr    = [](const char* p, const char* n, const char* v, size_t s, int o, uint32_t pos)   { return ctx()->op_setxattr(p, n, v, s, o, pos); };
  ops.getxattr    = [](const char* p, const char* n, char* v, size_t s, uint32_t pos)                { return ctx()->op_getxattr(p, n, v, s, pos); };
  ops.listxattr   = [](const char* p, char* l, size_t s)                                             { return ctx()->op_listxattr(p, l, s); };
  ops.removexattr = [](const char* p, const char* n)                                                 { return ctx()->op_removexattr(p, n); };
  return ops;
}
// clang-format on
