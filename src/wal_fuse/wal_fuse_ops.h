#pragma once

#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif
#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif
#include <fuse/fuse.h>

#include <string>

// Passthrough FUSE operations: every call is forwarded to the backing
// directory stored in fuse_get_context()->private_data.
//
// Pass as the Ops template argument to WalFuse<Ops>.
class WalFuseOpBase {
 public:
  // Build and return the fuse_operations table for this policy.
  static struct fuse_operations make_ops();

 private:
  static const char* backing_dir();
  static std::string real_path(const char* path);

  static int op_getattr(const char*, struct stat*);
  static int op_fgetattr(const char*, struct stat*, struct fuse_file_info*);
  static int op_access(const char*, int);
  static int op_readlink(const char*, char*, size_t);
  static int op_readdir(const char*, void*, fuse_fill_dir_t, off_t,
                        struct fuse_file_info*);
  static int op_mkdir(const char*, mode_t);
  static int op_unlink(const char*);
  static int op_rmdir(const char*);
  static int op_symlink(const char*, const char*);
  static int op_rename(const char*, const char*);
  static int op_link(const char*, const char*);
  static int op_chmod(const char*, mode_t);
  static int op_chown(const char*, uid_t, gid_t);
  static int op_truncate(const char*, off_t);
  static int op_ftruncate(const char*, off_t, struct fuse_file_info*);
  static int op_utimens(const char*, const struct timespec[2]);
  static int op_create(const char*, mode_t, struct fuse_file_info*);
  static int op_open(const char*, struct fuse_file_info*);
  static int op_read(const char*, char*, size_t, off_t, struct fuse_file_info*);
  static int op_write(const char*, const char*, size_t, off_t,
                      struct fuse_file_info*);
  static int op_statfs(const char*, struct statvfs*);
  static int op_flush(const char*, struct fuse_file_info*);
  static int op_release(const char*, struct fuse_file_info*);
  static int op_fsync(const char*, int, struct fuse_file_info*);
  static int op_setxattr(const char*, const char*, const char*, size_t, int,
                         uint32_t);
  static int op_getxattr(const char*, const char*, char*, size_t, uint32_t);
  static int op_listxattr(const char*, char*, size_t);
  static int op_removexattr(const char*, const char*);
};
