#include "src/base/wal_segment.h"

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

  if (::msync(buff, p_size, MS_SYNC))
    return elog(
        system_error_code(errno), "msync({}, {}) failed", reinterpret_cast<void*>(buff), p_size);
  return ok();
}
