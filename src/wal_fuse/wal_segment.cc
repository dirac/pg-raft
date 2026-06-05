#include "src/wal_fuse/wal_segment.h"

namespace {
uint16_t xlp_magic(char const* p) {
  return static_cast<uint16_t>(p[0]) | (static_cast<uint16_t>(p[1]) << 8);
}

uint32_t xlp_rem_len(char const* p) {
  p += 16;
  return (static_cast<uint32_t>(p[0]) << 0) | (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

bool page_start(off_t const offset) {
  return offset % kWalBlockSize == 0;
}
uint32_t round_up(uint32_t len) {
  return ((len + 7) / 8) * 8;
}

/// Total length of XLogRecord
uint32_t xl_tot_len(char const* record) {
  auto* p = record;
  return (static_cast<uint32_t>(p[0]) << 0) | (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

uint32_t block_space(off_t offset) {
  return kWalBlockSize - (offset % kWalBlockSize);
}

uint32_t diff(char const* s, char const* e) {
  return u4(reinterpret_cast<uint64_t>(e) - reinterpret_cast<uint64_t>(s));
}

}  // namespace

std::tuple<off_t, size_t> WalSegment::WalState::update(char const* ptr, size_t size, off_t off) {
  assert(!(curr_ < off));
  assert(!((off + size) <= curr_));

  // The bytes before curr_ have already been processed. Increment ptr to the unprocessed part (and
  // decrement size).
  ptr += (curr_ - off);
  size -= (curr_ - off);
  off = curr_;

  size_t hsize = 0;
  off_t  next  = curr_;

  if (page_start(curr_)) {
    if (xlp_magic(ptr) == 0)
      return {curr_, 0U};

    /// Start of the file, initialize rsize_ from the header.
    if (curr_ == 0)
      rsize_ = round_up(xlp_rem_len(ptr));

    hsize = curr_ == 0 ? 40 : 24;

    next = curr_ + hsize;

    size -= hsize;
    ptr += hsize;
    off += hsize;
  }

  // If rsize is not zero, we have to consume that many bytes, otherwise
  // it is start of a XLogReader, therefore read the size. If there are no more data to read then
  // it will return a block of zero size.
  if (!rsize_)
    rsize_ = round_up(xl_tot_len(ptr));

  auto sz = std::min({size, static_cast<size_t>(block_space(next)), rsize_});

  rsize_ -= sz;
  next += sz;

  return {std::exchange(curr_, next), sz + hsize};
}

void WalSegment::write(char const* buf, size_t size, off_t off) {
  if (!next_) [[unlikely]] {
    size_t bytes = 0;
    do {
      std::tie(next_, bytes) = state_.update(map_, kWalSegmentSize, 0);
    } while (bytes);

    // page aligned.
    synced_ = ((next_ / getpagesize()) * getpagesize());
  }

  // Already processed or it is empty (zero elements).
  if (off + size < next_ || next_ < off) {
    assert(std::memcmp(map_ + off, buf, size) == 0);
    return;
  }

  assert(std::memcmp(map_ + off, buf, (next_ - off)) == 0);

  // forward the buff for already processed bytes.
  buf  = buf + (next_ - off);
  size = size - (next_ - off);
  off  = next_;

  // bytes with data.
  size_t bytes = 0;
  auto   begin = next_;

  do {
    std::tie(next_, bytes) = state_.update(buf, size, off);
  } while (bytes);

  std::memcpy(map_ + begin, buf, (next_ - begin));
  tracker_.async_publish(shared_from_this(), begin, (next_ - begin));
}

int WalSegment::fsync() {
  // page aligned size.
  auto size = (((next_ - synced_) + (getpagesize() - 1)) / getpagesize()) * getpagesize();

  auto promise = std::promise<int>{};
  auto future  = promise.get_future();

  tracker_.async_fsync(std::move(promise), shared_from_this(), map_ + synced_, size);

  if (auto r = future.get(); r)
    return r;

  // Keep one page of overlap so the next fsync re-covers the last page,
  // which may still be receiving writes.
  synced_ += size - getpagesize();
  return 0;
}

uint32_t WalSegment::offset(char const* p) const {
  return diff(map_, p);
}

bool WalSegment::page_start(char const* p) const {
  return (offset(p) % kWalBlockSize) == 0;
}

bool WalSegment::first_page(char const* p) const {
  return offset(p) < kWalBlockSize;
}

uint32_t WalSegment::remaining_space(char const* p) const {
  assert(page_start(p) == false);
  return kWalBlockSize - (offset(p) % kWalBlockSize);
}

uint16_t WalSegment::xlp_magic(unsigned page) const {
  auto* p = map_ + (page * kWalBlockSize);
  return static_cast<uint16_t>(p[0]) | (static_cast<uint16_t>(p[1]) << 8);
}

/// Flags (first page, continuation, etc.)
uint16_t WalSegment::xlp_info(unsigned page) const {
  auto* p = map_ + (page * kWalBlockSize) + 2;
  return static_cast<uint16_t>(p[0]) | (static_cast<uint16_t>(p[1]) << 8);
}

/// Remaining bytes of a spanning record
uint32_t WalSegment::xlp_rem_len(unsigned page) const {
  auto* p = map_ + (page * kWalBlockSize) + 16;
  return (static_cast<uint32_t>(p[0]) << 0) | (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

/// LSN of this page's start
uint64_t WalSegment::xlp_pageaddr(unsigned page) const {
  auto* p = map_ + (page * kWalBlockSize) + 8;
  return (static_cast<uint64_t>(p[0]) << 0) | (static_cast<uint64_t>(p[1]) << 8) |
         (static_cast<uint64_t>(p[2]) << 16) | (static_cast<uint64_t>(p[3]) << 24) |
         (static_cast<uint64_t>(p[4]) << 32) | (static_cast<uint64_t>(p[5]) << 40) |
         (static_cast<uint64_t>(p[5]) << 48) | (static_cast<uint64_t>(p[7]) << 56);
}

void WalTracker::async_publish(shared_ptr<WalSegment>, off_t, size_t) {}

void WalTracker::async_fsync(std::promise<int> promise,
                             shared_ptr<WalSegment>,
                             char const* buff,
                             size_t      size);

{
  if (::msync(buff, size, MSYNC))
    promise.set_value(-errno);
  else
    promise.set_value(0);
}
