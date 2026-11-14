#include "src/base/wal_segment.h"

#include <fcntl.h>
#include <unistd.h>

#include <charconv>
#include <fstream>

WalSegment::WalSegment(shared_ptr<WalSegmentCache>  cache,
                       std::filesystem::path const& wal_dir,
                       LogSeqNumber                 lsn)
    : file_(wal_dir / filename(lsn)), lsn_(lsn), cache_(std::move(cache)) {}

WalSegment::~WalSegment() {
  if (map_) {
    map_->last_epoch.store(epoch_msec(), std::memory_order_relaxed);
    map_->ref.fetch_sub(1, std::memory_order_relaxed);
  }
  gc();
}

std::mutex& WalSegment::map_mtx() {
  return cache_->mtx;
}

map<LogSeqNumber, WalSegment::Mapping>& WalSegment::mappings() {
  return cache_->mappings;
}

void WalSegment::gc(std::unique_lock<std::mutex> lk) {
  auto& maps = mappings();

  std::erase_if(maps, [](auto const& pair) { return pair.second.erase(5000); });
  if (maps.size() > 5) [[unlikely]]
    std::erase_if(maps, [](auto const& pair) { return pair.second.erase(0); });

  lk.unlock();
}

Result<void> WalSegment::open() {
  if (map_)
    return ok();

  auto  lk   = std::unique_lock{map_mtx()};
  auto& maps = mappings();

  auto pos = maps.find(lsn_);
  if (pos == maps.end()) {
    if (!std::filesystem::exists(file_)) {
      // Add a '.' to the filename.
      string name{"."};
      name += file_.filename();
      std::erase(name, '"');

      file_ = file_.parent_path() / name;
    }

    int fd = ::open(file_.c_str(), O_RDWR);
    if (fd == -1)
      return elog(system_error_code(errno), "Unable to open file {}", file_);

    struct stat s;
    fstat(fd, &s);
    log(INFO, "Opened file={} stat={}", file_, s);

    void* map = ::mmap(nullptr, kWalSegmentSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
      auto ec = elog(system_error_code(errno), "Unable to mmap file {}", file_);
      ::close(fd);
      return ec;
    }

    pos = maps.emplace(lsn_, Mapping{reinterpret_cast<unsigned char*>(map), fd}).first;
  }
  map_ = &(pos->second);
  map_->last_epoch.store(epoch_msec(), std::memory_order_relaxed);
  map_->ref.fetch_add(1, std::memory_order_relaxed);

  gc(std::move(lk));
  return ok();
}

WalSegment::Mapping::Mapping(Mapping&& that) noexcept
    : map(that.map), fd(that.fd), ref(that.ref.load()), last_epoch(that.last_epoch.load()) {
  that.map = nullptr;
  that.fd  = -1;
  that.ref = 0;
}

WalSegment::Mapping& WalSegment::Mapping::operator=(Mapping&& that) noexcept {
  std::swap(map, that.map);
  std::swap(fd, that.fd);

  auto r = ref.load();
  ref.store(that.ref.load());
  that.ref.store(r);

  return *this;
}

WalSegment::Mapping::~Mapping() {
  if (!map)
    return;

  ::munmap(map, kWalSegmentSize);
  ::close(fd);
}

bool WalSegment::Mapping::erase(unsigned t) const {
  return map == nullptr || (ref.load(std::memory_order_relaxed) == 0 &&
                            (epoch_msec() - last_epoch.load(std::memory_order_relaxed)) >= t);
}

Result<void> WalSegment::fsync(off_t offset, size_t size) {
  off_t p_offset = (offset / getpagesize()) * getpagesize();

  size += u8(offset - p_offset);

  size_t p_size = (size / static_cast<size_t>(getpagesize())) * static_cast<size_t>(getpagesize());
  if (p_size < size)
    p_size += static_cast<size_t>(getpagesize());

  auto* buff = mapping() + p_offset;

  // Nothing to sync (WalFuseSegment::fsync asks even for an empty range); msync of 0 bytes fails
  // with EINVAL on macOS.
  if (p_size == 0)
    return ok();

  if (::msync(buff, p_size, MS_SYNC))
    return elog(
        system_error_code(errno), "msync({}, {}) failed", reinterpret_cast<void*>(buff), p_size);
  return ok();
}

// ── History files ───────────────────────────────────────────────────────────────────────────────

std::optional<uint32_t> history_timeline(std::string_view path) {
  if (path.starts_with('/'))
    path.remove_prefix(1);
  constexpr std::string_view kSuffix = ".history";
  if (path.size() != 8 + kSuffix.size() || !path.ends_with(kSuffix))
    return std::nullopt;
  for (char c : path.substr(0, 8))
    if (!((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F')))
      return std::nullopt;

  uint32_t timeline = 0;
  std::from_chars(path.data(), path.data() + 8, timeline, 16);
  return timeline;
}

Result<void> write_history_file(std::filesystem::path const& dir,
                                uint32_t                     timeline,
                                string const&                content) {
  auto path = dir / history_filename(timeline);
  auto tmp  = dir / ("." + history_filename(timeline) + ".tmp");

  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, file_mode());
  if (fd == -1)
    return elog(system_error_code(errno), "Unable to create {}", tmp);
  auto n   = ::write(fd, content.data(), content.size());
  auto err = errno;
  if (n == static_cast<ssize_t>(content.size()) && ::fsync(fd) != 0) {
    err = errno;
    n   = -1;
  }
  ::close(fd);
  if (n != static_cast<ssize_t>(content.size()))
    return elog(system_error_code(err), "Unable to write {}", tmp);

  if (::rename(tmp.c_str(), path.c_str()) != 0)
    return elog(system_error_code(errno), "Unable to rename {} to {}", tmp, path);

  if (int dfd = ::open(dir.c_str(), O_RDONLY); dfd != -1) {
    ::fsync(dfd);
    ::close(dfd);
  }
  return ok();
}

Result<string> read_history_file(std::filesystem::path const& dir, uint32_t timeline) {
  auto          path = dir / history_filename(timeline);
  std::ifstream in(path, std::ios::binary);
  if (!in)
    return system_error_code(ENOENT);
  return string{std::istreambuf_iterator<char>(in), {}};
}

std::optional<uint32_t> latest_history_timeline(std::filesystem::path const& dir) {
  std::optional<uint32_t> latest;
  std::error_code         ec;
  for (auto const& entry : std::filesystem::directory_iterator(dir, ec))
    if (auto t = history_timeline(entry.path().filename().string()); t && (!latest || *t > *latest))
      latest = t;
  return latest;
}
