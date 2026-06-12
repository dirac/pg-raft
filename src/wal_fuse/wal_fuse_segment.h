#pragma once

#include "src/base/base.h"
#include "src/base/wal_segment.h"

class WalFuseSegment {
 public:
  WalFuseSegment(WalTracker& tracker, shared_ptr<WalSegment> map)
      : tracker_(tracker), map_(map), next_(0), synced_(0), state_() {}

  ~WalFuseSegment();

  WalFuseSegment(WalFuseSegment const&)            = delete;
  WalFuseSegment& operator=(WalFuseSegment const&) = delete;

  WalFuseSegment(WalFuseSegment&&)            = delete;
  WalFuseSegment& operator=(WalFuseSegment&&) = delete;

  LogSeqNumber const& lsn() const { return map_->lsn(); }
  unsigned char*      mapping() const { return map_->mapping(); }

  asio::const_buffer buffer(off_t offset, size_t size) const {
    return asio::const_buffer{mapping() + offset, size};
  }

  struct WalState {
    WalState() {}

    std::tuple<off_t, size_t, bool> update(unsigned char const* ptr, size_t size, off_t offset);

    off_t  curr() const { return curr_; }
    size_t rsize() const { return rsize_; }

   private:
    off_t  curr_{0};   /// The amount already parsed.
    size_t rsize_{0};  /// The size that is remaining.
  };

  /// Update the memory region.
  void write(unsigned char const* buf, size_t size, off_t offset);

  /// Read from the memory mapped region.
  void read(unsigned char* buf, size_t size, off_t offset);

  /// msync the modified region.
  int fsync();

 private:
  void initialize();

  /// The expectation is the data has not been modified.
  void check_modified(unsigned char const* buf, size_t size, off_t off);

  WalTracker&            tracker_;
  shared_ptr<WalSegment> map_;

  off_t    next_;
  off_t    synced_;
  WalState state_;

  std::atomic_flag initialized_;
};
