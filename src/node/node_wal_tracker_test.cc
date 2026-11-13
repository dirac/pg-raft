#define BOOST_TEST_MODULE node_wal_tracker
#include "src/node/node_wal_tracker.h"

#include <fcntl.h>
#include <unistd.h>

#include "src/raft/raft_ensemble_test_common.h"

// NodeWalTracker against an in-process raft ensemble, playing the FUSE layer: a segment in a
// "pg_wal" directory gets WAL bytes, and the tracker is told what FUSE would tell it.

namespace fs = std::filesystem;

namespace {

/// A 16 MiB segment file in |dir|, mapped.
shared_ptr<WalSegment> make_segment(fs::path const& dir, LogSeqNumber lsn) {
  fs::create_directories(dir);
  int fd = ::open((dir / filename(lsn)).c_str(), O_RDWR | O_CREAT, 0600);
  BOOST_REQUIRE(fd >= 0);
  BOOST_REQUIRE(::ftruncate(fd, kWalSegmentSize) == 0);
  ::close(fd);
  auto seg = make_shared<WalSegment>(make_shared<WalSegmentCache>(), dir, lsn);
  BOOST_REQUIRE(seg->open().has_value());
  return seg;
}

Bytes random_bytes(size_t n) {
  auto rd = std::mt19937{std::random_device{}()};
  auto b  = Bytes(n);
  std::ranges::generate(b, [&rd] { return static_cast<uint8_t>(rd()); });
  return b;
}

/// Call async_fsync and wait for its completion.
Result<void> fsync(NodeWalTracker& t, shared_ptr<WalSegment> seg, off_t off, size_t size) {
  auto p = make_shared<std::promise<Result<void>>>();
  auto f = p->get_future();
  t.async_fsync(seg, off, size, [p](Result<void> r) { p->set_value(r); });
  if (f.wait_for(30s) != std::future_status::ready)
    return system_error_code(ETIMEDOUT);
  return f.get();
}

struct Ensemble {
  boost::asio::io_context io;
  IoThreads               th{io, 8};
  TmpDir                  dir;
  Config                  cfg;
  RaftTestService         service{io, dir(), cfg, options, 3};

  Ensemble() {
    service.start_all();
    service.wait_for_leader();
  }
  ~Ensemble() {
    service.shutdown();
    io.stop();
  }
};

}  // namespace

BOOST_AUTO_TEST_CASE(leader_mode, *boost::unit_test::timeout(timeout)) {
  Ensemble e;
  auto     leader = e.service.leader().value();
  auto     lsn    = LogSeqNumber{.timeline = 1, .lsn = 0x1000000};
  auto     seg    = make_segment(e.dir() / "pg_wal", lsn);

  NodeWalTracker tracker{e.service.server(leader)};
  tracker.set_mode(NodeWalTracker::Mode::kLeader);
  tracker.set_flushed_lsn({.timeline = 1, .lsn = lsn.lsn + 12000});

  // What FUSE reports: a page, then a part of the next, then the fsync of both.
  auto bytes = random_bytes(12000);
  seg->write(bytes.data(), 0, bytes.size());
  tracker.async_publish(seg, 0, 8192);
  tracker.async_publish(seg, 8192, 12000 - 8192);
  BOOST_TEST(fsync(tracker, seg, 0, 12000).has_value());

  // Committed: every server has the bytes in its wal_dir, and the leader's state has the flush
  // LSN and the end of the WAL.
  auto c = e.service.check_consistency({});
  BOOST_TEST(c.violations.empty());
  for (unsigned i = 0; i < 3; ++i) {
    auto s    = e.service.server(i);
    auto copy = s->segment(lsn);
    BOOST_REQUIRE(copy.has_value());
    BOOST_TEST(std::memcmp(copy.value()->mapping(), bytes.data(), bytes.size()) == 0, i);

    auto state =
        s->inspect_for_test([](RaftState const& st, RaftLog const&, RaftIndex) { return st; });
    BOOST_TEST(state.flushed_lsn.lsn == lsn.lsn + 12000, i);
    BOOST_TEST(state.wal_end.lsn == lsn.lsn + 12000, i);
  }
}

// The writes travel with the flush, unless enough of them is pending: then a WriteOp of its own.
BOOST_AUTO_TEST_CASE(writes_batched_until_flush_or_threshold, *boost::unit_test::timeout(timeout)) {
  Ensemble e;
  auto     leader = e.service.leader().value();
  auto     lsn    = LogSeqNumber{.timeline = 1, .lsn = 0x4000000};
  auto     seg    = make_segment(e.dir() / "pg_wal", lsn);

  NodeWalTracker tracker{e.service.server(leader), false, false, /*write_op_bytes=*/16384};
  tracker.set_mode(NodeWalTracker::Mode::kLeader);
  auto bytes = random_bytes(30000);
  seg->write(bytes.data(), 0, bytes.size());

  // The committed entries of the segment, once there are |n| (a WriteOp commits on its own).
  auto wal_entries = [&](size_t n) {
    vector<WalCommand> r;
    for (int i = 0; i < 100; ++i, std::this_thread::sleep_for(50ms)) {
      r.clear();
      for (auto const& [idx, entry] : e.service.check_consistency({}).log)
        if (entry.cmd.segment == lsn)
          r.push_back(entry.cmd);
      if (r.size() >= n)
        break;
    }
    return r;
  };

  // Below the threshold: no entry.
  tracker.async_publish(seg, 0, 8192);
  BOOST_TEST(wal_entries(1).empty());

  // The pending range reaches 16 KiB: one WriteOp of all of it.
  tracker.async_publish(seg, 8192, 12000);
  auto w = wal_entries(1);
  BOOST_REQUIRE(w.size() == 1U);
  BOOST_TEST((std::get<WriteOp>(w[0].command) == WriteOp{.offset = 0, .size = 20192}));

  // The rest goes with the flush, and every node has every byte.
  tracker.async_publish(seg, 20192, 30000 - 20192);
  BOOST_TEST(fsync(tracker, seg, 0, 30000).has_value());
  w = wal_entries(2);
  BOOST_REQUIRE(w.size() == 2U);
  BOOST_TEST((std::get<FlushOp>(w[1].command) == FlushOp{.offset = 20192, .size = 30000 - 20192}));
  for (unsigned i = 0; i < 3; ++i) {
    auto copy = e.service.server(i)->segment(lsn);
    BOOST_REQUIRE(copy.has_value());
    BOOST_TEST(std::memcmp(copy.value()->mapping(), bytes.data(), bytes.size()) == 0, i);
  }

  // A flush with nothing new: an empty FlushOp, still the barrier.
  BOOST_TEST(fsync(tracker, seg, 30000, 0).has_value());
  w = wal_entries(3);
  BOOST_REQUIRE(w.size() == 3U);
  BOOST_TEST(std::get<FlushOp>(w[2].command).size == 0U);
}

BOOST_AUTO_TEST_CASE(leader_mode_on_a_follower_fails, *boost::unit_test::timeout(timeout)) {
  Ensemble e;
  auto     follower = (e.service.leader().value() + 1U) % 3U;
  auto     seg      = make_segment(e.dir() / "pg_wal", {.timeline = 1, .lsn = 0x2000000});

  // Not the leader: PostgreSQL's fsync must fail, so that it never acknowledges a commit that is
  // not on a quorum.
  NodeWalTracker tracker{e.service.server(follower)};
  tracker.set_mode(NodeWalTracker::Mode::kLeader);
  auto r = fsync(tracker, seg, 0, 8192);
  BOOST_REQUIRE(!r.has_value());
  BOOST_TEST(r.error().value() == WalSegment::kNoConsensus);
}

BOOST_AUTO_TEST_CASE(follower_mode_is_local, *boost::unit_test::timeout(timeout)) {
  Ensemble e;
  auto     leader = e.service.leader().value();
  auto     lsn    = LogSeqNumber{.timeline = 1, .lsn = 0x3000000};
  auto     seg    = make_segment(e.dir() / "pg_wal", lsn);

  // Even on the leader's server, follower mode keeps everything local: no entry, no copy.
  NodeWalTracker tracker{e.service.server(leader)};
  auto           bytes = random_bytes(8192);
  seg->write(bytes.data(), 0, bytes.size());
  tracker.async_publish(seg, 0, 8192);
  tracker.async_timeline(2, "1\t0/3000000\tno recovery target specified\n");
  BOOST_TEST(fsync(tracker, seg, 0, 8192).has_value());

  auto c = e.service.check_consistency({});
  BOOST_TEST(c.violations.empty());
  for (auto const& [idx, entry] : c.log)
    BOOST_TEST((std::holds_alternative<std::monostate>(entry.cmd.command)), idx);
  for (unsigned i = 0; i < 3; ++i) {
    BOOST_TEST(!fs::exists(e.service.wal_dir(i) / filename(lsn)), i);
    BOOST_TEST(!fs::exists(e.service.wal_dir(i) / history_filename(2)), i);
  }
}

BOOST_AUTO_TEST_CASE(timeline_reaches_every_node, *boost::unit_test::timeout(timeout)) {
  Ensemble e;
  auto     leader  = e.service.leader().value();
  auto     content = string{"1\t0/3000000\tno recovery target specified\n"};

  NodeWalTracker tracker{e.service.server(leader)};
  tracker.set_mode(NodeWalTracker::Mode::kLeader);
  tracker.async_timeline(2, content);

  // check_consistency waits until every server has applied its log.
  auto deadline = std::chrono::steady_clock::now() + 30s;
  auto all      = false;
  while (!all && std::chrono::steady_clock::now() < deadline) {
    BOOST_TEST(e.service.check_consistency({}).violations.empty());
    all = true;
    for (unsigned i = 0; i < 3; ++i)
      all = all && read_history_file(e.service.wal_dir(i), 2).value_or("") == content;
    if (!all)
      std::this_thread::sleep_for(200ms);
  }
  BOOST_TEST(all);
}

BOOST_AUTO_TEST_CASE(fenced_mode_fails_fsync, *boost::unit_test::timeout(timeout)) {
  Ensemble e;
  auto     leader = e.service.leader().value();
  auto     lsn    = LogSeqNumber{.timeline = 1, .lsn = 0x4000000};
  auto     seg    = make_segment(e.dir() / "pg_wal", lsn);

  // A node that lost leadership: even with the raft leader underneath, nothing is published and no
  // fsync succeeds.
  NodeWalTracker tracker{e.service.server(leader)};
  tracker.set_mode(NodeWalTracker::Mode::kFenced);
  tracker.async_publish(seg, 0, 8192);
  auto r = fsync(tracker, seg, 0, 8192);
  BOOST_REQUIRE(!r.has_value());
  BOOST_TEST(r.error().value() == WalSegment::kNoConsensus);
  BOOST_TEST(!fs::exists(e.service.wal_dir(leader) / filename(lsn)));

  // Restarted as a standby: follower mode, fsync is local again.
  tracker.set_mode(NodeWalTracker::Mode::kFollower);
  BOOST_TEST(fsync(tracker, seg, 0, 8192).has_value());
}
