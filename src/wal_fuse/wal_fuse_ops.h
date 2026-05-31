#pragma once

#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif
#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif
#include <fuse/fuse.h>

#include <filesystem>
#include <string>

// Passthrough FUSE operations backed by a directory on the real filesystem.
//
// Construct with a backing_dir path. Override virtual op_* methods in a
// subclass to intercept specific operations; all others pass through to the
// backing directory unchanged.
//
// Pass as the ops argument to WalFuse. The WalFuse instance stores this object
// and passes it as fuse_get_context()->private_data so that the static
// trampolines in make_ops() can dispatch to the correct virtual method.
class WalFuseOps {
 public:
  explicit WalFuseOps(std::filesystem::path backing_dir);
  virtual ~WalFuseOps() = default;

  WalFuseOps(WalFuseOps const&)            = delete;
  WalFuseOps& operator=(WalFuseOps const&) = delete;

  // Returns a fuse_operations table whose entries are static trampolines.
  // Each trampoline casts fuse_get_context()->private_data to WalFuseOps*
  // and calls the corresponding virtual method.
  static struct fuse_operations make_ops();

  virtual int op_getattr(char const*, struct stat*);
  virtual int op_fgetattr(char const*, struct stat*, struct fuse_file_info*);
  virtual int op_access(char const*, int);
  virtual int op_readlink(char const*, char*, size_t);
  virtual int op_opendir(char const* path, struct fuse_file_info* fi);
  virtual int op_readdir(char const*, void*, fuse_fill_dir_t, off_t, struct fuse_file_info*);
  virtual int op_releasedir(char const* path, struct fuse_file_info* fi);
  virtual int op_mkdir(char const*, mode_t);
  virtual int op_unlink(char const*);
  virtual int op_rmdir(char const*);
  virtual int op_symlink(char const*, char const*);
  virtual int op_rename(char const*, char const*);
  virtual int op_link(char const*, char const*);
  virtual int op_chmod(char const*, mode_t);
  virtual int op_chown(char const*, uid_t, gid_t);
  virtual int op_truncate(char const*, off_t);
  virtual int op_ftruncate(char const*, off_t, struct fuse_file_info*);
  virtual int op_utimens(char const*, const struct timespec[2]);
  virtual int op_create(char const*, mode_t, struct fuse_file_info*);
  virtual int op_open(char const*, struct fuse_file_info*);
  virtual int op_read(char const*, char*, size_t, off_t, struct fuse_file_info*);
  virtual int op_write(char const*, char const*, size_t, off_t, struct fuse_file_info*);
  virtual int op_statfs(char const*, struct statvfs*);
  virtual int op_flush(char const*, struct fuse_file_info*);
  virtual int op_release(char const*, struct fuse_file_info*);
  virtual int op_fsync(char const*, int, struct fuse_file_info*);
  virtual int op_setxattr(char const*, char const*, char const*, size_t, int, uint32_t);
  virtual int op_getxattr(char const*, char const*, char*, size_t, uint32_t);
  virtual int op_listxattr(char const*, char*, size_t);
  virtual int op_removexattr(char const*, char const*);

 protected:
  std::filesystem::path const& backing_dir() const { return backing_dir_; }
  std::filesystem::path        real_path(char const* path) const;

 private:
  std::filesystem::path backing_dir_;
};
