#pragma once

#include "src/base/base.h"

// Passthrough FUSE operations backed by a directory on the real filesystem.
//
// Construct with a backing_dir path. Override virtual op_* methods in a
// subclass to intercept specific operations; all others pass through to the
// backing directory unchanged.
//
// Pass as the ops argument to Fuse. The Fuse instance stores this object
// and passes it as fuse_get_context()->private_data so that the static
// trampolines in make_ops() can dispatch to the correct virtual method.
class FuseOps {
 public:
  explicit FuseOps(std::filesystem::path backing_dir);
  virtual ~FuseOps() = default;

  FuseOps(FuseOps const&)            = delete;
  FuseOps& operator=(FuseOps const&) = delete;

  // Per-op invocation counters (relaxed — for observation only).
  std::atomic<uint64_t> n_getattr{0};
  std::atomic<uint64_t> n_fgetattr{0};
  std::atomic<uint64_t> n_access{0};
  std::atomic<uint64_t> n_readlink{0};
  std::atomic<uint64_t> n_opendir{0};
  std::atomic<uint64_t> n_readdir{0};
  std::atomic<uint64_t> n_releasedir{0};
  std::atomic<uint64_t> n_mkdir{0};
  std::atomic<uint64_t> n_unlink{0};
  std::atomic<uint64_t> n_rmdir{0};
  std::atomic<uint64_t> n_symlink{0};
  std::atomic<uint64_t> n_rename{0};
  std::atomic<uint64_t> n_link{0};
  std::atomic<uint64_t> n_chmod{0};
  std::atomic<uint64_t> n_chown{0};
  std::atomic<uint64_t> n_truncate{0};
  std::atomic<uint64_t> n_ftruncate{0};
  std::atomic<uint64_t> n_utimens{0};
  std::atomic<uint64_t> n_create{0};
  std::atomic<uint64_t> n_open{0};
  std::atomic<uint64_t> n_read{0};
  std::atomic<uint64_t> n_write{0};
  std::atomic<uint64_t> n_statfs{0};
  std::atomic<uint64_t> n_flush{0};
  std::atomic<uint64_t> n_release{0};
  std::atomic<uint64_t> n_fsync{0};
  std::atomic<uint64_t> n_setxattr{0};
  std::atomic<uint64_t> n_getxattr{0};
  std::atomic<uint64_t> n_listxattr{0};
  std::atomic<uint64_t> n_removexattr{0};

  // Returns a fuse_operations table whose entries are static trampolines.
  // Each trampoline casts fuse_get_context()->private_data to FuseOps*
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

  /// Validate that op on the same file is not concurrently active.
#if 0
#define TEST_CONCURRENCY Active a{&active_, fi->fh};

  std::array<std::atomic_flag, 1024> active_;
  struct Active {
    Active(std::array<std::atomic_flag, 1024>* flags, uint64_t fd) : flags_(flags), fd_(fd) {
      if (fd_ >= flags_->size())
        return;

      if (flags_->at(fd_).test_and_set())
        throw format("Concurrent access {}", fd_);

      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ~Active() {
      if (fd_ >= flags_->size())
        return;
      flags_->at(fd_).clear();
    }
    std::array<std::atomic_flag, 1024>* flags_;
    uint64_t                            fd_;
  };
#else
#define TEST_CONCURRENCY
#endif
};
