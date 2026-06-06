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
  WalSegment(WalTracker& tracker, LogSeqNumber lsn, int fd, void* map, int prot)
      : tracker_(tracker),
        lsn_(lsn),
        fd_(fd),
        map_(reinterpret_cast<char*>(map)),
        prot_(prot),
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

  int                 prot() const { return prot_; }
  LogSeqNumber const& lsn() const { return lsn_; }

  asio::const_buffer buffer(off_t offset, size_t size) const {
    return asio::const_buffer{map_ + offset, size};
  }

  struct WalState {
    WalState() {}

    std::tuple<off_t, size_t> update(char const* ptr, size_t size, off_t offset);

    off_t  curr() const { return curr_; }
    size_t rsize() const { return rsize_; }

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

 private:
  WalTracker&        tracker_;
  LogSeqNumber const lsn_;
  int const          fd_;
  char* const        map_;
  int const          prot_;
  off_t              next_;
  off_t              synced_;
  WalState           state_;
};
