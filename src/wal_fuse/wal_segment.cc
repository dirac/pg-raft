#include "src/wal_fuse/wal_segment.h"

#include "src/base/logger.h"

namespace {
uint16_t xlp_magic(unsigned char const* p) {
  return static_cast<uint16_t>(static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8));
}

uint32_t xlp_rem_len(unsigned char const* p) {
  p += 16;
  return (static_cast<uint32_t>(p[0]) << 0) | (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

bool page_start(off_t const offset) {
  return offset % kWalBlockSize == 0;
}

/// Total length of XLogRecord
uint32_t xl_tot_len(unsigned char const* record) {
  auto const* p = record;
  auto const  r = (static_cast<uint32_t>(p[0]) << 0) | (static_cast<uint32_t>(p[1]) << 8) |
                 (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  return r;
}

uint32_t block_space(off_t offset) {
  return u4(kWalBlockSize - (offset % kWalBlockSize));
}

template <uint32_t n = 8>
uint32_t round_up(uint32_t len) {
  return ((len + n - 1) / n) * n;
}

}  // namespace

WalSegment::~WalSegment() {
  log(INFO, "The file {} has data upto {}", lsn(), next_);
  ::munmap(map_, kWalSegmentSize);
  ::close(fd_);
}

std::tuple<off_t, size_t> WalSegment::WalState::update(unsigned char const* ptr,
                                                       size_t               size,
                                                       off_t                off) {
  assert(off <= curr_);
  assert(curr_ <= (off + static_cast<off_t>(size)));

  if (curr_ == (off + static_cast<off_t>(size)))
    return {curr_, 0U};

  // The bytes before curr_ have already been processed. Increment ptr to the unprocessed part (and
  // decrement size).
  ptr += (curr_ - off);
  size -= u8(curr_ - off);
  off = curr_;

  off_t hsize = 0;
  off_t next  = curr_;

  if (page_start(curr_)) {
    if (xlp_magic(ptr) == 0)
      return {curr_, 0U};

    /// Start of the file, initialize rsize_ from the header.
    if (curr_ == 0)
      rsize_ = round_up(xlp_rem_len(ptr));
    assert(rsize_ == round_up(xlp_rem_len(ptr)));
    log(DEBUG, "xlp_rem_len = {} at {}", rsize_, next);

    hsize = curr_ == 0 ? 40 : 24;

    next = curr_ + hsize;

    size -= u8(hsize);
    ptr += hsize;
    off += hsize;
  }

  // If rsize is not zero, we have to consume that many bytes, otherwise
  // it is start of a XLogReader, therefore read the size. If there are no more data to read then
  // it will return a block of zero size.
  if (!rsize_) {
    rsize_ = round_up(xl_tot_len(ptr));
    log(DEBUG, "xl_tot_len = {} at {}", rsize_, next);
  }

  auto sz = std::min({size, static_cast<size_t>(block_space(next)), rsize_});

  rsize_ -= sz;
  next += sz;

  log(DEBUG, "consuming {} at {}, rem = {}", sz + u8(hsize), next, rsize_);

  return {std::exchange(curr_, next), sz + u8(hsize)};
}

void breakpoint();

bool modified(unsigned char const* m, unsigned char const* b, size_t size, off_t off) {
  if (std::memcmp(m, b, size) == 0)
    return false;

  struct fuse_context *ctx = fuse_get_context();
  pid_t process_id = ctx->pid;

  off_t sz = static_cast<off_t>(size);

  bool print = false;
  for (off_t i = 0; i < sz; ++i, m++, b++) {
    print = print || ((sz - i) < 10) || m[5] != b[5];
    if (print) {
      sz = std::min(sz, i + 100);
      log(ERROR, "[{}] off={} {:x} vs {:x}", process_id, off + i, *m, *b);
    }
  }
  breakpoint();
  return true;
}

void WalSegment::write(unsigned char const* buf, size_t size, off_t off) {
  if (!next_) [[unlikely]] {
    size_t bytes = 0;
    do {
      std::tie(next_, bytes) = state_.update(map_, kWalSegmentSize, 0);
    } while (bytes);

    log(INFO, "The file {} has data upto {}", lsn(), next_);

    // page aligned.
    synced_ = ((next_ / getpagesize()) * getpagesize());
  }

  // Already processed
  if ((off + i4(size)) < next_) {
    assert(!modified(map_ + off, buf, size, off));
    return;
  }

  // No wal records present, it is empty (zero elements).
  if (next_ < off) {
    // assert(!modified(map_ + off, buf, size, off));
    return;
  }
  assert(!modified(map_ + off, buf, u8(next_ - off), off));

  // forward the buff for already processed bytes.
  assert(next_ >= off);

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
  if (!(prot_ & PROT_WRITE))
    return 0;

  // page aligned size.
  auto size = (((next_ - synced_) + (getpagesize() - 1)) / getpagesize()) * getpagesize();
  if (size == 0)
    return 0;

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

void WalSegment::read(unsigned char* buf, size_t size, off_t off) {
  std::memcpy(buf, map_ + off, size);
}

void WalTracker::async_publish(shared_ptr<WalSegment>, off_t, size_t) {}

void WalTracker::async_fsync(std::promise<int> promise,
                             shared_ptr<WalSegment>,
                             unsigned char* buff,
                             size_t         size) {
  if (::msync(buff, size, MS_SYNC)) {
    auto err = errno;
    log(ERROR,
        "msync({}, {}) failed, error={} ({})",
        reinterpret_cast<void*>(buff),
        size,
        strerror(err),
        err);
    promise.set_value(-err);
  } else
    promise.set_value(0);
}

void breakpoint()
{
  log(ERROR, "Break");
}
