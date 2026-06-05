#include "src/wal_fuse/wal_segment.h"

namespace {
uint16_t seg_xlp_magic(char const* p) {
  return static_cast<uint16_t>(static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8));
}

uint32_t seg_xlp_rem_len(char const* p) {
  p += 16;
  return (static_cast<uint32_t>(p[0]) << 0) | (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

bool seg_page_start(off_t const offset) {
  return offset % kWalBlockSize == 0;
}

/// Total length of XLogRecord
uint32_t seg_xl_tot_len(char const* record) {
  auto* p = record;
  return (static_cast<uint32_t>(p[0]) << 0) | (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

uint32_t seg_block_space(off_t offset) {
  return u4(kWalBlockSize - (offset % kWalBlockSize));
}

uint32_t diff(char const* s, char const* e) {
  return u4(reinterpret_cast<uint64_t>(e) - reinterpret_cast<uint64_t>(s));
}
template <uint32_t n = 8>
uint32_t round_up(uint32_t len) {
  return ((len + n - 1) / n) * n;
}

}  // namespace

std::tuple<off_t, size_t> WalSegment::WalState::update(char const* ptr, size_t size, off_t off) {
  assert(!(curr_ < off));
  assert(!((off + static_cast<off_t>(size)) <= curr_));

  // The bytes before curr_ have already been processed. Increment ptr to the unprocessed part (and
  // decrement size).
  ptr += (curr_ - off);
  size -= u8(curr_ - off);
  off = curr_;

  off_t hsize = 0;
  off_t next  = curr_;

  if (seg_page_start(curr_)) {
    if (seg_xlp_magic(ptr) == 0)
      return {curr_, 0U};

    /// Start of the file, initialize rsize_ from the header.
    if (curr_ == 0)
      rsize_ = round_up(seg_xlp_rem_len(ptr));

    hsize = curr_ == 0 ? 40 : 24;

    next = curr_ + hsize;

    size -= u8(hsize);
    ptr += hsize;
    off += hsize;
  }

  // If rsize is not zero, we have to consume that many bytes, otherwise
  // it is start of a XLogReader, therefore read the size. If there are no more data to read then
  // it will return a block of zero size.
  if (!rsize_)
    rsize_ = round_up(seg_xl_tot_len(ptr));

  auto sz = std::min({size, static_cast<size_t>(seg_block_space(next)), rsize_});

  rsize_ -= sz;
  next += sz;

  return {std::exchange(curr_, next), sz + u8(hsize)};
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
  if (off + i4(size) < next_ || next_ < off) {
    assert(std::memcmp(map_ + off, buf, size) == 0);
    return;
  }

  assert(std::memcmp(map_ + off, buf, u8(next_ - off)) == 0);

  // forward the buff for already processed bytes.
  assert(next_ > off);

  buf  = buf + (next_ - off);
  size = size - u8(next_ - off);
  off  = next_;

  // bytes with data.
  size_t bytes = 0;
  auto   begin = next_;

  do {
    std::tie(next_, bytes) = state_.update(buf, size, off);
  } while (bytes);

  std::memcpy(map_ + begin, buf, u8(next_ - begin));
  tracker_.async_publish(shared_from_this(), begin, u8(next_ - begin));
}

int WalSegment::fsync() {
  // page aligned size.
  auto size = (((next_ - synced_) + (getpagesize() - 1)) / getpagesize()) * getpagesize();

  auto promise = std::promise<int>{};
  auto future  = promise.get_future();

  tracker_.async_fsync(std::move(promise), shared_from_this(), map_ + synced_, u8(size));

  if (auto r = future.get(); r)
    return r;

  // Keep one page of overlap so the next fsync re-covers the last page,
  // which may still be receiving writes.
  synced_ += size - getpagesize();
  return 0;
}

void WalSegment::read(char* buf, size_t size, off_t off) {
  std::memcpy(buf, map_ + off, size);
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
  return u2(static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8));
}

/// Flags (first page, continuation, etc.)
uint16_t WalSegment::xlp_info(unsigned page) const {
  auto* p = map_ + (page * kWalBlockSize) + 2;
  return u2(static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8));
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
         (static_cast<uint64_t>(p[6]) << 48) | (static_cast<uint64_t>(p[7]) << 56);
}

void WalTracker::async_publish(shared_ptr<WalSegment>, off_t, size_t) {}

void WalTracker::async_fsync(std::promise<int> promise,
                             shared_ptr<WalSegment>,
                             char*  buff,
                             size_t size) {
  if (::msync(buff, size, MS_SYNC))
    promise.set_value(-errno);
  else
    promise.set_value(0);
}
