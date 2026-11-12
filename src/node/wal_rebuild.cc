#include "src/node/wal_rebuild.h"

#include <fcntl.h>
#include <unistd.h>

#include "src/base/logger.h"
#include "src/base/wal_segment.h"

namespace fs = std::filesystem;

namespace {

Result<void> copy_from_raft(fs::path const& from, fs::path const& to) {
  std::error_code ec;
  fs::copy_file(from, to, fs::copy_options::overwrite_existing, ec);
  if (ec)
    return elog(ec, "Unable to copy {} to {}", from, to);
  return ok();
}

/// Zero |path| from |offset| to its end.
Result<void> zero_from(fs::path const& path, off_t offset) {
  int fd = ::open(path.c_str(), O_RDWR);
  if (fd == -1)
    return elog(system_error_code(errno), "Unable to open {}", path);
  auto size  = static_cast<off_t>(kWalSegmentSize);
  auto zeros = std::vector<char>(1U << 16U, 0);
  for (off_t off = offset; off < size;) {
    auto n = ::pwrite(
        fd, zeros.data(), static_cast<size_t>(std::min<off_t>(size - off, std::ssize(zeros))), off);
    if (n <= 0) {
      auto e = errno;
      ::close(fd);
      return elog(system_error_code(e), "Unable to zero {}", path);
    }
    off += n;
  }
  ::fsync(fd);
  ::close(fd);
  return ok();
}

}  // namespace

Result<void> rebuild_pg_wal(fs::path const& pg_wal, fs::path const& raft_wal, LogSeqNumber end) {
  std::error_code ec;
  auto            remove = [&](fs::path const& p, char const* why) -> Result<void> {
    log(INFO, "Rebuilding pg_wal: removing {} ({})", p.filename(), why);
    fs::remove(p, ec);
    if (ec)
      return elog(ec, "Unable to remove {}", p);
    return ok();
  };

  for (auto const& entry : fs::directory_iterator(pg_wal, ec)) {
    auto const path = entry.path();
    auto const name = path.filename().string();
    if (!entry.is_regular_file())
      continue;
    auto const in_raft = fs::exists(raft_wal / name);

    if (auto seg = log_seq_number(name.c_str()); seg) {
      auto const   seg_end = seg->lsn + kWalSegmentSize;
      Result<void> r       = ok();
      if (!in_raft)
        r = remove(path, "not in raft/wal");
      else if (seg->timeline > end.timeline)
        r = remove(path, "a later timeline than the committed WAL");
      else if (seg->timeline < end.timeline || seg_end <= end.lsn)
        r = copy_from_raft(raft_wal / name, path);
      else if (seg->lsn <= end.lsn) {
        r = copy_from_raft(raft_wal / name, path);
        if (r)
          r = zero_from(path, static_cast<off_t>(end.lsn - seg->lsn));
      } else
        r = remove(path, "past the committed WAL");
      if (!r)
        return r;
    } else if (history_timeline(name)) {
      if (auto r = in_raft ? copy_from_raft(raft_wal / name, path)
                           : remove(path, "a timeline that was not committed");
          !r)
        return r;
    } else if ((name.starts_with('.') && log_seq_number(name.c_str() + 1)) ||
               name.ends_with(".partial") || name.starts_with("xlogtemp.")) {
      if (auto r = remove(path, "not WAL a standby replays"); !r)
        return r;
    }
  }
  if (ec)
    return elog(ec, "Unable to list {}", pg_wal);
  return ok();
}
