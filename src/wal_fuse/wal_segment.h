#pragma once

#include "src/base/base.h"

class WalSegment;

struct WalTracker {
  virtual void async_publish(shared_ptr<WalSegment> segment, off_t offset, size_t size);

  /// Invoke msync. The args (buff and size) are page aligned.
  virtual void async_fsync(std::promise<int>      promise,
                           shared_ptr<WalSegment> segment,
                           char*                  buff,
                           size_t                 size);
};

class WalSegment : public std::enable_shared_from_this<WalSegment> {
 public:
  WalSegment(WalTracker& tracker, LogSeqNumber lsn, int fd, void* map)
      : tracker_(tracker),
        lsn_(lsn),
        fd_(fd),
        map_(reinterpret_cast<char*>(map)),
        next_(0),
        synced_(0),
        state_() {}

  ~WalSegment() {
    ::munmap(map_, kWalSegmentSize);
    ::close(fd_);
  }

  WalSegment(WalSegment const&)            = delete;
  WalSegment& operator=(WalSegment const&) = delete;

  WalSegment(WalSegment&&)            = delete;
  WalSegment& operator=(WalSegment&&) = delete;

  LogSeqNumber const& lsn() const { return lsn_; }

  asio::const_buffer buffer(off_t offset, size_t size) const {
    return asio::const_buffer{map_ + offset, size};
  }

  struct WalState {
    WalState() {}

    std::tuple<off_t, size_t> update(char const* ptr, size_t size, off_t offset);

   private:
    off_t  curr_{0};   /// The amount already parsed.
    size_t rsize_{0};  /// The size that is remaining.
  };

  /// Update the memory region.
  void write(char const* buf, size_t size, off_t offset);

  /// Read from the memory mapped region.
  void read(char* buf, size_t size, off_t offset);

  /// msync the modified region.
  int fsync();

  uint32_t offset(char const* p) const;

  bool page_start(char const* p) const;

  bool first_page(char const* p) const;

  uint32_t remaining_space(char const* p) const;

  uint16_t xlp_magic(unsigned page) const;

  /// Flags (first page, continuation, etc.)
  uint16_t xlp_info(unsigned page) const;

  /// Remaining bytes of a spanning record
  uint32_t xlp_rem_len(unsigned page) const;

  /// LSN of this page's start
  uint64_t xlp_pageaddr(unsigned page) const;

 private:
  WalTracker&        tracker_;
  LogSeqNumber const lsn_;
  int const          fd_;
  char* const        map_;
  off_t              next_;
  off_t              synced_;
  WalState           state_;
};
