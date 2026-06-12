#include "src/wal_fuse/wal_fuse_ops.h"

#include "src/base/logger.h"

namespace {

void set_wal_segment(struct fuse_file_info* fi, WalFuseSegment* segment) {
  uint64_t fh = reinterpret_cast<uint64_t>(segment);
  assert((fh & 0x1) == 0x0);

  if (fh & (u8(0x1) << 63))
    fh |= u8(0x1);
  else
    fh |= (u8(0x1) << 63);
  fi->fh        = fh;
  fi->direct_io = 1;
}

WalFuseSegment* get_wal_segment(struct fuse_file_info* fi) {
  uint64_t fh = fi->fh;

  if (!(fh & (u8(0x1) << 63)))
    return nullptr;

  if (fh & u8(0x1))
    fh = fh ^ u8(0x1);
  else
    fh = fh ^ (u8(0x1) << 63);

  return reinterpret_cast<WalFuseSegment*>(fh);
}
}  // namespace

int WalFuseOps::op_open(char const* path, struct fuse_file_info* fi) {
  if (auto lsn = log_seq_number(path); lsn) {
    log(DEBUG, "open {} flags={} lsn={}", path, fi->flags, *lsn);
    n_open.fetch_add(1, std::memory_order_relaxed);

    auto seg = std::make_shared<WalSegment>(cache_, backing_dir(), *lsn);
    if (auto r = seg->open(); !r)
      return -r.error().value();

    auto fuse_seg = std::make_unique<WalFuseSegment>(tracker_, std::move(seg));
    set_wal_segment(fi, fuse_seg.release());
    return 0;
  } else
    return FuseOps::op_open(path, fi);
}

int WalFuseOps::op_release(char const* path, struct fuse_file_info* fi) {
  if (auto segment = std::unique_ptr<WalFuseSegment>{get_wal_segment(fi)}; segment) {
    log(DEBUG, "release {}, lsn={}", path, segment->lsn());
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
