#include "src/wal_fuse/wal_fuse_ops.h"

namespace {

uint32_t hex_to_int(std::string_view hex_sv) {
  uint32_t          value = 0;
  std::stringstream ss;
  ss << std::hex << hex_sv;
  ss >> value;
  return value;
}

std::optional<LogSeqNumber> timeline_and_lsn(char const* path) {
  if (*path == '/')
    ++path;

  for (auto i = 0; i < 24; ++i) {
    char c = path[i];
    if (c >= '0' && c <= '9')
      continue;
    if (c >= 'A' && c <= 'F')
      continue;
    if (c >= 'a' && c <= 'f')
      continue;

    return std::nullopt;
  }
  if (path[25] != '\0')
    return std::nullopt;

  // Zero-allocation slicing using std::string_view
  auto timeline_sv = std::string_view{path, 8};
  auto logid_sv    = std::string_view{path + 8, 8};
  auto logseg_sv   = std::string_view{path + 16, 8};

  // Convert hex segments to integers
  uint32_t timeline = hex_to_int(timeline_sv);
  uint32_t logid    = hex_to_int(logid_sv);
  uint32_t logseg   = hex_to_int(logseg_sv);

  // Reconstruct the 64-bit LSN value
  // Internal formula: LSN = (logid * 0x100000000) + (logseg * WAL_SEGMENT_SIZE)
  uint64_t lsn =
      (static_cast<uint64_t>(logid) << 32) + (static_cast<uint64_t>(logseg) * kWalSegmentSize);

  return LogSeqNumber{.timeline = timeline, .lsn = lsn};
}

void set_wal_file(struct fuse_file_info* fi, WalFile* file) {
  uint64_t fh = reinterpret_cast<uint64_t>(file);
  assert((fh & 0x1) == 0x0);

  if (fh & (0x1 << 63))
    fh &= 0x1;
  else
    fh &= (0x1 << 63);
  fi->fh = fh;
}

WalFile* get_wal_file(struct fuse_file_info* fi) {
  uint64_t fh = fi->fh;

  if (!(fh & (0x1 << 63)))
    return nullptr;

  if (fh & 0x1)
    fh = fh ^ 0x1;
  else
    fh = fh ^ (0x1 << 63);

  return reinterpret_cast<struct fuse_file_info*>(fh);
}
}  // namespace

int WalFuseOps::op_open(char const* path, struct fuse_file_info* fi) {
  if (auto lsn = timeline_and_lsn(path); lsn) {
    n_open.fetch_add(1, std::memory_order_relaxed);
    log(DEBUG, "open {}, lsn={}", path, *lsn);

    auto pos = files_.find(*lsn);
    if (pos == files_.end()) {
      int fd = ::open(real_path(path).c_str(), fi->flags);
      if (fd == -1)
        return -errno;

      void* map = ::mmap(nullptr, kWalSegmentSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      if (map == MAP_FAILED) {
        int err = errno;
        ::close(fd);
        return -err;
      }

      pos = files_.emplace(*lsn, std::make_tuple(0, make_shared<WalFile>(tracker_, *lsn, fd, map)))
                .first;
    }
    ++std::get<0>(pos->second);
    set_wal_file(fi, std::get<1>(pos->second).get());
  } else
    return FuseOps::op_open(path, fi);
}

int WalFuseOps::op_release(char const* path, struct fuse_file_info* fi) {
  if (auto* file = get_wal_file(fi); file) {
    n_release.fetch_add(1, std::memory_order_relaxed);
    log(DEBUG, "release {}, lsn={}", path, file->lsn());

    if (auto pos = files_.find(file->lsn()); pos != files_.end()) {
      if (--std::get<0>(pos->second) == 0)
        files_.erase(pos);
    }
  } else
    return FuseOps::op_release(path, fi);
}

int WalFuseOps::op_write(char const*            path,
                         char const*            buf,
                         size_t                 size,
                         off_t                  offset,
                         struct fuse_file_info* fi) {
  if (auto* file = get_wal_file(fi); file) {
    n_write.fetch_add(1, std::memory_order_relaxed);
    log(DEBUG, "write {} size={} off={}, lsn={}", path, size, offset, file->lsn());

    file->write(buf, size, offset);
    return 0;
  } else
    return FuseOps::op_write(path, fi);
}

int WalFuseOps::op_read(char const*            path,
                        char*                  buf,
                        size_t                 size,
                        off_t                  offset,
                        struct fuse_file_info* fi) {
  if (auto* file = get_wal_file(fi); file) {
  }
}
