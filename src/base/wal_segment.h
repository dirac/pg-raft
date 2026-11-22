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
  // kNoConsensus error. The offset and size need not be page aligned.
  Result<void> fsync(off_t offset, size_t size);

  void write(void const* buffer, off_t offset, size_t size) {
    std::memcpy(mapping() + offset, buffer, size);
  }

  // Buffer
  asio::const_buffer const_buffer(off_t off, size_t size) const { return {mapping() + off, size}; }

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

  /// Unmap the unreferenced mappings idle for at least 5 s, and all unreferenced ones when more
  /// than 5 remain.
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

  /// PostgreSQL installed the history file of |timeline| (on promotion), with |content|. The raft
  /// layer replicates it as a TimelineOp. It is called after the rename that installs the file,
  /// before PostgreSQL writes WAL on the new timeline.
  virtual void async_timeline(uint32_t timeline, string const& content) {
    (void)timeline;
    (void)content;
  }

  /// A commit waits until the WAL up to |lsn| (a byte position) is replicated, then |completion|
  /// runs (PostgreSQL's sync_rep_wait_hook, wal_capture = "hook" with commit_wait = "sync_rep").
  /// Default: at once (the WAL is local, durable after its fsync).
  virtual void async_commit(uint64_t lsn, std::function<void(Result<void>)> completion) {
    (void)lsn;
    completion(ok());
  }

  virtual ~WalTracker() = default;
};

// Timeline history files, `%08X.history`, in a WAL directory.

/// The timeline of a history file name (with or without a leading '/'), or nullopt.
std::optional<uint32_t> history_timeline(std::string_view path);

/// The name of the history file of |timeline|.
inline string history_filename(uint32_t timeline) {
  return std::format("{:08X}.history", timeline);
}

/// Write the history file of |timeline| into |dir|: written to a temporary name, fsynced, then
/// renamed, so that a reader never sees a partial file.
Result<void> write_history_file(std::filesystem::path const& dir,
                                uint32_t                     timeline,
                                string const&                content);

/// Read the history file of |timeline| from |dir|.
Result<string> read_history_file(std::filesystem::path const& dir, uint32_t timeline);

/// The highest timeline with a history file in |dir|, or nullopt if there is none (timeline 1
/// has no history file).
std::optional<uint32_t> latest_history_timeline(std::filesystem::path const& dir);
