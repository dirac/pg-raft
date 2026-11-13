#include "src/wal_fuse/wal_fuse_ops.h"

#include "src/base/logger.h"

namespace {

// A WAL file handle: fi->fh holds a heap-allocated reference to the segment's shared
// WalFuseSegment, tagged so that it is told apart from a passthrough file descriptor.
using Handle = shared_ptr<WalFuseSegment>;

void set_wal_segment(struct fuse_file_info* fi, Handle* segment) {
  uint64_t fh = reinterpret_cast<uint64_t>(segment);
  assert((fh & 0x1) == 0x0);

  if (fh & (u8(0x1) << 63))
    fh |= u8(0x1);
  else
    fh |= (u8(0x1) << 63);
  fi->fh        = fh;
  fi->direct_io = 1;
}

Handle* get_handle(struct fuse_file_info* fi) {
  uint64_t fh = fi->fh;

  if (!(fh & (u8(0x1) << 63)))
    return nullptr;

  if (fh & u8(0x1))
    fh = fh ^ u8(0x1);
  else
    fh = fh ^ (u8(0x1) << 63);

  return reinterpret_cast<Handle*>(fh);
}

WalFuseSegment* get_wal_segment(struct fuse_file_info* fi) {
  auto* h = get_handle(fi);
  return h ? h->get() : nullptr;
}
}  // namespace

Result<shared_ptr<WalFuseSegment>> WalFuseOps::wal_segment(LogSeqNumber lsn) {
  auto lk = std::unique_lock{segments_mtx_};
  if (auto pos = segments_.find(lsn); pos != segments_.end())
    if (auto seg = pos->second.lock(); seg)
      return seg;

  auto map = std::make_shared<WalSegment>(cache_, backing_dir(), lsn);
  if (auto r = map->open(); !r)
    return r.error();
  auto seg       = std::make_shared<WalFuseSegment>(tracker_, std::move(map));
  segments_[lsn] = seg;

  // Forget the segments no handle has open any more.
  std::erase_if(segments_, [](auto const& kv) { return kv.second.expired(); });
  return seg;
}

int WalFuseOps::op_open(char const* path, struct fuse_file_info* fi) {
  if (auto lsn = log_seq_number(path); lsn) {
    log(DEBUG, "open {} flags={} lsn={}", path, fi->flags, *lsn);
    n_open.fetch_add(1, std::memory_order_relaxed);

    auto seg = wal_segment(*lsn);
    if (!seg)
      return -seg.error().value();

    set_wal_segment(fi, new Handle(std::move(seg.value())));
    return 0;
  } else
    return FuseOps::op_open(path, fi);
}

int WalFuseOps::op_release(char const* path, struct fuse_file_info* fi) {
  if (auto handle = std::unique_ptr<Handle>{get_handle(fi)}; handle) {
    log(DEBUG, "release {}, lsn={}", path, (*handle)->lsn());
    n_release.fetch_add(1, std::memory_order_relaxed);

    fi->fh = 0;
    return 0;
  } else
    return FuseOps::op_release(path, fi);
}

int WalFuseOps::op_write(char const*            path,
                         char const*            buf,
                         size_t                 size,
                         off_t                  offset,
                         struct fuse_file_info* fi) {
  if (auto* segment = get_wal_segment(fi); segment) {
    log(DEBUG,
        "write {} size={} off={}, lsn={} buf={}",
        path,
        size,
        offset,
        segment->lsn(),
        reinterpret_cast<void const*>(buf));
    n_write.fetch_add(1, std::memory_order_relaxed);

    segment->write(reinterpret_cast<unsigned char const*>(buf), size, offset);
    return static_cast<int>(size);
  } else
    return FuseOps::op_write(path, buf, size, offset, fi);
}

int WalFuseOps::op_flush(char const* path, struct fuse_file_info* fi) {
  if (auto* segment = get_wal_segment(fi); segment) {
    log(INFO, "flush {}, lsn={}", path, segment->lsn());
    n_flush.fetch_add(1, std::memory_order_relaxed);

    return 0;
  } else
    return FuseOps::op_flush(path, fi);
}

int WalFuseOps::op_fsync(char const* path, int datasync, struct fuse_file_info* fi) {
  if (auto* segment = get_wal_segment(fi); segment) {
    log(DEBUG, "fsync {}, lsn={}", path, segment->lsn());
    n_fsync.fetch_add(1, std::memory_order_relaxed);

    return segment->fsync();
  } else
    return FuseOps::op_fsync(path, datasync, fi);
}

int WalFuseOps::op_read(char const*            path,
                        char*                  buf,
                        size_t                 size,
                        off_t                  offset,
                        struct fuse_file_info* fi) {
  if (auto* segment = get_wal_segment(fi); segment) {
    log(DEBUG, "read {} size={} off={}, lsn={}", path, size, offset, segment->lsn());
    n_read.fetch_add(1, std::memory_order_relaxed);

    segment->read(reinterpret_cast<unsigned char*>(buf), size, offset);
    return static_cast<int>(size);
  } else
    return FuseOps::op_read(path, buf, size, offset, fi);
}

int WalFuseOps::op_unlink(char const* path) {
  if (auto lsn = log_seq_number(path); lsn) {
    log(INFO, "unlink {}", path);
    n_unlink.fetch_add(1, std::memory_order_relaxed);

    char to[64];
    to[0] = '/';
    to[1] = '.';
    strncpy(to + 2, *path == '/' ? path + 1 : path, sizeof(to) - 3);

    return rename(real_path(path).c_str(), real_path(to).c_str()) == -1 ? -errno : 0;
  } else
    return FuseOps::op_unlink(path);
}

int WalFuseOps::op_rename(char const* from, char const* to) {
  if (auto r = FuseOps::op_rename(from, to); r != 0)
    return r;

  if (auto timeline = history_timeline(to); timeline) {
    auto content = read_history_file(backing_dir(), *timeline);
    if (!content)
      return -content.error().value();

    log(INFO, "Timeline {} installed ({} bytes of history)", *timeline, content->size());
    tracker_.async_timeline(*timeline, *content);
  }
  return 0;
}
