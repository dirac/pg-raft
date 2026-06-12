#pragma once

#include "src/base/base.h"
#include "src/base/logger.h"

struct WalSegmentCache;

class WalSegment : public std::enable_shared_from_this<WalSegment> {
 public:
  WalSegment(shared_ptr<WalSegmentCache> cache, std::filesystem::path const& dir, LogSeqNumber lsn);
  ~WalSegment();

  Result<void> open();

  /// Pointer to the mapped file.
  unsigned char* mapping() const { return map_->map; }

  // The start lsn.
  LogSeqNumber const& lsn() const { return lsn_; }

  // The backing file.
  std::filesystem::path const& file() const { return file_; }

  static constexpr int kNoConsensus =
      ECANCELED;  // 140, Asynchronous operation stopped before normal completion

  // Calls msync. When consensus is not reached but local msync is successful, we report
  // kNoConsensus error. The offset and size are not page aligned.
  Result<void> fsync(off_t offset, size_t size);

  // Release
  void gc() { gc(std::unique_lock{map_mtx()}); }

 private:
  struct Mapping {
    Mapping(unsigned char* m, int f) : map(m), fd(f) {}
    ~Mapping();

    Mapping(Mapping const&)            = delete;
    Mapping& operator=(Mapping const&) = delete;

    Mapping(Mapping&& that) noexcept;
    Mapping& operator=(Mapping&& that) noexcept;

    bool erase(unsigned t) const;

    unsigned char* map;
    int            fd;

    std::atomic_uint ref{0};  // Ref count.
    std::atomic_uint last_epoch{0};
  };

  friend struct WalSegmentCache;

  std::filesystem::path       file_;
  LogSeqNumber const          lsn_;
  shared_ptr<WalSegmentCache> cache_;

  Mapping* map_{nullptr};

  std::mutex&                 map_mtx();
  map<LogSeqNumber, Mapping>& mappings();

  /// Delete mappings that have not been used for 5 mins.
  void gc(std::unique_lock<std::mutex> lk);
};

struct WalSegmentCache {
  std::mutex                             mtx;
  map<LogSeqNumber, WalSegment::Mapping> mappings;
};

struct WalTracker {
  virtual void async_publish(shared_ptr<WalSegment> segment, off_t offset, size_t size) {
    (void)segment;
    (void)offset;
    (void)size;
  }

  /// Invoke msync. The args (offset and size) are not page aligned.
  virtual void async_fsync(shared_ptr<WalSegment>            segment,
                           off_t                             offset,
                           size_t                            size,
                           std::function<void(Result<void>)> completion_handler) {
    completion_handler(segment->fsync(offset, size));
  }
};
