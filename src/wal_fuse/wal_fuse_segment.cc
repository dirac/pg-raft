#include "src/wal_fuse/wal_fuse_segment.h"

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

WalFuseSegment::~WalFuseSegment() {
  log(INFO, "The file {} has data upto {}", lsn(), next_);
}

std::tuple<off_t, size_t, bool> WalFuseSegment::WalState::update(unsigned char const* ptr,
                                                                 size_t               size,
                                                                 off_t                off) {
  bool rec_start = false;

  assert(off <= curr_);
  assert(curr_ <= (off + static_cast<off_t>(size)));

  if (curr_ == (off + static_cast<off_t>(size)))
    return {curr_, 0U, rec_start};

  // The bytes before curr_ have already been processed. Increment ptr to the unprocessed part (and
  // decrement size).
  ptr += (curr_ - off);
  size -= u8(curr_ - off);
  off = curr_;

  off_t hsize = 0;
  off_t next  = curr_;

  if (page_start(curr_)) {
    if (xlp_magic(ptr) == 0)
      return {curr_, 0U, rec_start};

    rsize_ = round_up(xlp_rem_len(ptr));

    log(DEBUG, "xlp_rem_len = {} at {}", rsize_, next);

    hsize = curr_ == 0 ? 40 : 24;
    next  = curr_ + hsize;

    size -= u8(hsize);
    ptr += hsize;
    off += hsize;

    if (!rsize_)
      return {std::exchange(curr_, next), u8(hsize), rec_start};
  }

  // If rsize is not zero, we have to consume that many bytes, otherwise
  // it is start of a XLogReader, therefore read the size. If there are no more data to read then
  // it will return a block of zero size.
  if (!rsize_) {
    rec_start = true;
    rsize_    = round_up(xl_tot_len(ptr));

    log(DEBUG, "xl_tot_len = {} at {}", rsize_, next);
  }

  auto sz = std::min({size, static_cast<size_t>(block_space(next)), rsize_});

  rsize_ -= sz;
  next += sz;

  log(DEBUG, "consuming {} at {}, rem = {}", sz + u8(hsize), next, rsize_);

  return {std::exchange(curr_, next), sz + u8(hsize), rec_start};
}

void breakpoint();

void WalFuseSegment::check_modified(unsigned char const* buf, size_t size, off_t off) {
  if (std::memcmp(mapping() + off, buf, size) == 0)
    return;

  log(INFO, "The wal-buffer got updated at {} lsn={}", off, lsn());
  std::memcpy(mapping() + off, buf, size);
  tracker_.async_publish(map_, off, size);

  synced_ = std::min(synced_, off);
}

void WalFuseSegment::initialize() {
  if (!initialized_.test_and_set()) [[unlikely]] {
    bool   rec_start;
    size_t bytes = 0;
    do {
      std::tie(next_, bytes, rec_start) = state_.update(mapping(), kWalSegmentSize, 0);
    } while (bytes);

    log(INFO, "The file {} has data upto {}", lsn(), next_);

    synced_ = next_;
  }
}

void WalFuseSegment::write(unsigned char const* buf, size_t size, off_t off) {
  initialize();

  bool rec_start;
  // Already processed
  if ((off + i4(size)) < next_) {
    check_modified(buf, size, off);
    return;
  }

  // No wal records present, it is empty (zero elements).
  // If we have content, then it will be written again.
  if (next_ < off) {
    // assert(!modified(mapping() + off, buf, size, off));
    return;
  }
  check_modified(buf, u8(next_ - off), off);

  // forward the buff for already processed bytes.
  assert(next_ >= off);

  buf  = buf + (next_ - off);
  size = size - u8(next_ - off);
  off  = next_;

  // bytes with data.
  size_t bytes = 0;
  auto   begin = next_;

  do {
    std::tie(next_, bytes, rec_start) = state_.update(buf, size, off);
  } while (bytes);

  std::memcpy(mapping() + begin, buf, u8(next_ - begin));
  tracker_.async_publish(map_, begin, u8(next_ - begin));
}

int WalFuseSegment::fsync() {
  // page aligned size.
  auto size = next_ - synced_;
  if (size == 0) {
    log(DEBUG, "Not doing sync next={} synced={}", next_, synced_);
    return 0;
  }

  auto promise = std::make_shared<std::promise<int>>();
  auto future  = promise->get_future();

  tracker_.async_fsync(map_, synced_, u8(size), [p = std::move(promise)](Result<void> r) mutable {
    if (!r)
      p->set_value(-r.error().value());
    else
      p->set_value(0);
  });

  if (auto r = future.get(); r)
    return r;

  synced_ += size;
  return 0;
}

void WalFuseSegment::read(unsigned char* buf, size_t size, off_t off) {
  std::memcpy(buf, mapping() + off, size);
}
