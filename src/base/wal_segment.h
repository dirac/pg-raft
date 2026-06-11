#pragma once

#include "src/base/base.h"
#include "src/base/logger.h"

class WalSegment : public std::enable_shared_from_this<WalSegment> {
 public:
  WalSegment(std::filesystem::path const& dir, LogSeqNumber lsn);
  ~WalSegment();

  Result<void> open();

  /// Pointer to the mapped file.
  unsigned char* mapping() const { return map_->map; }

  // The start lsn.
  LogSeqNumber const& lsn() const { return lsn_; }

  // The backing file.
  std::filesystem::path const& file() const { return file_; }

  // Release
  static void gc() { gc(std::unique_lock{map_mtx()}); }

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

  std::filesystem::path const file_;
  LogSeqNumber const          lsn_;

  Mapping* map_{nullptr};

  static std::mutex&                 map_mtx();
  static map<LogSeqNumber, Mapping>& mappings();

  /// Delete mappings that have not been used for 5 mins.
  static void gc(std::unique_lock<std::mutex> lk);
};
