#include "src/node/wal_rebuild.h"

#include <fcntl.h>
#include <unistd.h>

#include <sstream>

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

/// The timelines that |tli| descends from, with the WAL range each owns (its history file, in
/// |raft_wal|, else |pg_wal|): {tli, begin, end}, the last being |tli| itself from its start point
/// on. Nullopt for timeline 1 or without a readable history file.
struct TimelineRange {
  uint32_t tli;
  uint64_t begin;
  uint64_t end;
};
std::optional<vector<TimelineRange>> timeline_ranges(fs::path const& raft_wal,
                                                     fs::path const& pg_wal,
                                                     uint32_t        tli) {
  auto content = read_history_file(raft_wal, tli);
  if (!content)
    content = read_history_file(pg_wal, tli);
  if (!content)
    return std::nullopt;
  vector<TimelineRange> ranges;
  uint64_t              begin = 0;
  std::istringstream    in(*content);
  string                line;
  while (std::getline(in, line)) {
    unsigned parent = 0, hi = 0, lo = 0;
    if (line.empty() || line.front() == '#' ||
        sscanf(line.c_str(), "%u %X/%X", &parent, &hi, &lo) != 3)
      continue;
    auto const end = (uint64_t{hi} << 32U) | lo;
    ranges.push_back({static_cast<uint32_t>(parent), begin, end});
    begin = end;
  }
  if (ranges.empty())
    return std::nullopt;
  ranges.push_back({tli, begin, UINT64_MAX});
  return ranges;
}

/// |n| bytes from the start of |path|; nullopt if it can't be read whole.
std::optional<Bytes> read_prefix(fs::path const& path, size_t n) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd == -1)
    return std::nullopt;
  Bytes  b(n);
  size_t done = 0;
  while (done < n) {
    auto r = ::pread(fd, b.data() + done, n - done, static_cast<off_t>(done));
    if (r <= 0)
      break;
    done += static_cast<size_t>(r);
  }
  ::close(fd);
  if (done != n)
    return std::nullopt;
  return b;
}

Result<void> write_prefix(fs::path const& path, Bytes const& b) {
  int fd = ::open(path.c_str(), O_RDWR);
  if (fd == -1)
    return elog(system_error_code(errno), "Unable to open {}", path);
  size_t done = 0;
  while (done < b.size()) {
    auto r = ::pwrite(fd, b.data() + done, b.size() - done, static_cast<off_t>(done));
    if (r <= 0) {
      auto e = errno;
      ::close(fd);
      return elog(system_error_code(e), "Unable to write {}", path);
    }
    done += static_cast<size_t>(r);
  }
  ::fsync(fd);
  ::close(fd);
  return ok();
}

}  // namespace

Result<void> rebuild_pg_wal(fs::path const& pg_wal, fs::path const& raft_wal) {
  std::error_code ec;
  auto            remove = [&](fs::path const& p, char const* why) -> Result<void> {
    log(INFO, "Rebuilding pg_wal: removing {} ({})", p.filename(), why);
    fs::remove(p, ec);
    if (ec)
      return elog(ec, "Unable to remove {}", p);
    return ok();
  };

  // The oldest segment raft/wal holds: pg_wal's segments before it, on a timeline raft knows (a
  // history file there, or timeline 1), are history raft has compacted away (committed: a deposed
  // primary's uncommitted WAL is always past what raft holds). A restored backup's WAL is such.
  std::optional<uint64_t> raft_first;
  for (auto const& entry : fs::directory_iterator(raft_wal, ec))
    if (auto seg = log_seq_number(entry.path().filename().string().c_str()))
      if (!raft_first || seg->lsn < *raft_first)
        raft_first = seg->lsn;
  auto const compacted = [&](LogSeqNumber const& seg) {
    return raft_first && seg.lsn + kWalSegmentSize <= *raft_first &&
           (seg.timeline == 1 || fs::exists(raft_wal / history_filename(seg.timeline)));
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

      // The segment holding its timeline's start point: on promotion PostgreSQL copies the
      // parent's bytes before that point into it through an xlogtemp file, which FUSE doesn't
      // replicate, so raft's copy has zeros there (the WAL sender serves them from the parent
      // timeline). Raft's copy gets them back: each part from raft's copy of the segment of the
      // ancestor timeline that owns it (committed WAL), else from this file as PostgreSQL wrote it
      // (raft may have compacted the ancestor's).
      std::optional<Bytes> prefix;
      if (in_raft)
        if (auto ranges = timeline_ranges(raft_wal, pg_wal, seg->timeline);
            ranges && ranges->back().begin > seg->lsn && ranges->back().begin < seg_end) {
          // Several timelines can start within one segment (promotions in quick succession):
          // each part of the prefix comes from the timeline that owns it.
          auto const n       = static_cast<size_t>(ranges->back().begin - seg->lsn);
          auto       base    = read_prefix(path, n);  // PostgreSQL's own copy, if any
          bool       missing = false;
          prefix             = base.value_or(Bytes(n, 0));
          for (auto const& r : *ranges) {
            if (r.tli == seg->timeline || r.end <= seg->lsn || r.begin >= seg->lsn + n)
              continue;
            auto const from = std::max(r.begin, seg->lsn) - seg->lsn;
            auto const to   = std::min(r.end, seg->lsn + n) - seg->lsn;
            // raft's copy of the owner's segment (committed WAL) over PostgreSQL's.
            if (auto b = read_prefix(raft_wal / filename({.timeline = r.tli, .lsn = seg->lsn}),
                                     static_cast<size_t>(to)))
              std::copy(b->begin() + static_cast<std::ptrdiff_t>(from),
                        b->begin() + static_cast<std::ptrdiff_t>(to),
                        prefix->begin() + static_cast<std::ptrdiff_t>(from));
            else
              missing = true;
          }
          if (missing && !base) {
            log(WARN, "Rebuilding pg_wal: no copy of the bytes of {} before its timeline's start", name);
            prefix.reset();
          }
        }

      if (!in_raft && compacted(*seg)) {
        // kept: before everything raft/wal holds
      } else if (!in_raft) {
        r = remove(path, "not in raft/wal");
      } else {
        r = copy_from_raft(raft_wal / name, path);
        if (r && prefix)
          r = write_prefix(path, *prefix);
      }
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
