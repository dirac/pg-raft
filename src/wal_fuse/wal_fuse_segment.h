#pragma once

#include "src/base/base.h"
#include "src/base/wal_segment.h"

/// The FUSE layer's state of one WAL segment: the walk (what has been published), what is stored
/// and what is synced. One per segment, shared by every handle open on it (WalFuseOps keeps them):
/// PostgreSQL backends write the WAL through handles of their own, and a walk per handle would
/// publish every byte once per handle. Thread-safe.
class WalFuseSegment {
 public:
  WalFuseSegment(WalTracker& tracker, shared_ptr<WalSegment> map)
      : tracker_(tracker), map_(map), next_(0), synced_(0), state_(map_->lsn().lsn) {}

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
    /// |segment_start|: the segment's start LSN. A page header then counts only if its
    /// xlp_pageaddr is the page's own LSN: a header only partly stored (its magic from one write,
    /// the rest not yet) is the end of the walk, not a page with no records.
    explicit WalState(uint64_t segment_start) : segment_start_(segment_start) {}

    std::tuple<off_t, size_t, bool> update(unsigned char const* ptr, size_t size, off_t offset);

    off_t  curr() const { return curr_; }
    size_t rsize() const { return rsize_; }

   private:
    off_t                   curr_{0};   /// The amount already parsed.
    size_t                  rsize_{0};  /// The size that is remaining.
    std::optional<uint64_t> segment_start_;
  };

  /// Update the memory region.
  void write(unsigned char const* buf, size_t size, off_t offset);

  /// Read from the memory mapped region.
  void read(unsigned char* buf, size_t size, off_t offset);

  /// msync the modified region.
  int fsync();

 private:
  void initialize();

  /// Bytes already walked, written again: PostgreSQL rewrites the last, partial page on every
  /// flush. Only the smallest range that differs is stored and published again.
  void check_modified(unsigned char const* buf, size_t size, off_t off);

  WalTracker&            tracker_;
  shared_ptr<WalSegment> map_;

  std::mutex mtx_;          // guards everything below
  off_t      next_;         // the walk: [0, next_) is walked (published)
  off_t      synced_;       // [0, synced_) is durable
  off_t      written_{0};   // the highest offset written: [next_, written_) is stored, not walked
  uint64_t   modified_{0};  // counts the rewrites that pulled synced_ back
  WalState   state_;
  bool       initialized_{false};
};
