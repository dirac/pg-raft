#pragma once

#include "src/base/io_threads.h"
#include "src/base/logger.h"
#include "src/base/testing.h"
#include "src/base/xxhash.h"
#include "src/raft/raft_test_service.h"

// A Writer plays the primary: it writes WAL bytes into the leader's segments and appends
// WriteOp/FlushOp entries with exec().

using namespace std::chrono_literals;

namespace {

auto const options = Options{
    // The tests run in parallel: under that load a shorter timeout starts elections that only make
    // it worse.
    .election_timeout_msecs   = 1000,
    .reconnect_interval_msecs = 2000,
    .connection_timeout_secs  = 4,
    .max_entries              = 0x1U << 13U,
    .tick_seconds             = 2,
    .snapshot_entries         = 0x1U << 13U,
    .keep_old_logs            = 0x1U << 14U,
    .compaction_lag           = 0x1U << 16U,
    .log_req_response         = false,
    // The servers stop or crash as processes, never as machines: a sync only slows the tests.
    .no_sync = true,
};

/// Plays the primary: writes WAL bytes into the leader's segments at increasing offsets and
/// appends their entries, one at a time (the WAL is written in order).
struct Writer {
  explicit Writer(RaftTestService& service) : service_(service) {
    thread_ = std::thread([this] { run(); });
  }

  ~Writer() { stop(); }

  void stop() {
    stop_.test_and_set();
    if (thread_.joinable())
      thread_.join();
  }

  /// The acknowledged writes, in the order of their entries.
  vector<RaftTestService::Write> writes() const {
    auto lk = std::unique_lock{mtx_};
    return writes_;
  }

  unsigned errors() const { return errors_.load(); }

  /// Whether the writer now and then appends a new timeline (on by default).
  void set_timelines(bool on) { timelines_.store(on); }

 private:
  void run() {
    auto     rd = std::mt19937{std::random_device{}()};
    unsigned n  = 0;

    while (!stop_.test()) {
      auto l = service_.leader();
      auto s = l ? service_.server(*l) : nullptr;
      if (!s) {
        std::this_thread::sleep_for(20ms);
        continue;
      }

      auto size = static_cast<size_t>(1U + (rd() % 4096U));
      auto o    = next_;
      if ((o % kWalSegmentSize) + size > kWalSegmentSize)
        o += kWalSegmentSize - (o % kWalSegmentSize);
      next_ = o + size;

      auto const lsn = LogSeqNumber{.timeline = 1, .lsn = o - (o % kWalSegmentSize)};
      auto const op  = WriteOp{.offset = static_cast<off_t>(o % kWalSegmentSize), .size = size};

      auto bytes = Bytes(size);
      std::ranges::generate(bytes, [&rd] { return static_cast<uint8_t>(rd()); });

      auto seg = s->segment(lsn);
      BOOST_REQUIRE(seg.has_value());
      seg.value()->write(bytes.data(), op.offset, op.size);

      auto flushed = LogSeqNumber{.timeline = 1, .lsn = o + size};
      auto r       = exec(s, WalCommand{.flushed_lsn = flushed, .segment = lsn, .command = op});
      if (r) {
        auto lk = std::unique_lock{mtx_};
        writes_.emplace_back(RaftTestService::Write{
            .idx = r.value(), .segment = lsn, .op = op, .checksum = xxhash32(bytes)});
      } else {
        errors_.fetch_add(1);
        std::this_thread::sleep_for(10ms);  // No leader, or a new one: try again shortly.
      }

      // Now and then an fsync, the durability barrier. The WriteOps carried the bytes: an empty
      // FlushOp, as NodeWalTracker sends with nothing pending (a FlushOp carries its range's bytes,
      // so one from offset 0 would resend the segment so far).
      if ((++n % 16U) == 0) {
        auto const flush = FlushOp{.offset = op.offset + static_cast<off_t>(size), .size = 0};
        (void)exec(s, WalCommand{.flushed_lsn = flushed, .segment = lsn, .command = flush});
      }

      // Now and then a new timeline: every server that applies it writes its history file
      // (check_consistency).
      if (timelines_.load() && (n % 1024U) == 0) {
        auto const timeline = TimelineOp{
            .timeline = 1U + n / 1024U,
            .content  = std::format("{}\t{:X}/{:X}\tno recovery target specified\n",
                                   n / 1024U,
                                   o >> 32U,
                                   o & 0xFFFFFFFFU),
        };
        (void)exec(s, WalCommand{.flushed_lsn = flushed, .command = timeline});
      }
    }
  }

  static Result<RaftIndex> exec(shared_ptr<TestRaftServer> const& s, WalCommand cmd) {
    auto promise = make_shared<std::promise<Result<RaftIndex>>>();
    auto future  = promise->get_future();
    s->exec(std::move(cmd), [promise](Result<RaftIndex> r) { promise->set_value(r); });

    if (future.wait_for(30s) != std::future_status::ready) {
      BOOST_ERROR("exec did not complete in 30 seconds");
      return system_error_code(ETIMEDOUT);
    }
    return future.get();
  }

  RaftTestService& service_;

  uint64_t next_{0};

  mutable std::mutex             mtx_;
  vector<RaftTestService::Write> writes_;
  std::atomic_uint               errors_{0};
  std::atomic_bool               timelines_{true};

  std::atomic_flag stop_ = ATOMIC_FLAG_INIT;
  std::thread      thread_;
};

/// Stop the writer and check the ensemble: no violation, and some acknowledged writes.
[[maybe_unused]] RaftTestService::Consistency validate(RaftTestService& service, Writer& writer) {
  writer.stop();

  auto writes = writer.writes();
  log(INFO, "{} writes acknowledged, {} failed", writes.size(), writer.errors());
  BOOST_TEST(!writes.empty());

  // The writes are acknowledged in order.
  for (size_t i = 1; i < writes.size(); ++i)
    BOOST_TEST(writes.at(i - 1).idx < writes.at(i).idx);

  auto consistency = service.check_consistency(writes);
  for (auto const& v : consistency.violations)
    log(ERROR, "{}", v);
  BOOST_TEST(consistency.violations.empty());
  return consistency;
}

auto const timeout = 300U;

}  // namespace
