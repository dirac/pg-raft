#define BOOST_TEST_MODULE raft_ensemble_keep_from
#include "src/raft/raft_ensemble_test_common.h"

// Compaction bounded by the latest backup (docs/backup.md §5): once set_wal_keep_from(segment) is
// called, every server drops its entries before the first one whose segment is at or after it,
// records first_idx and first_segment, and deletes the WAL segments wholly before it, in two
// steps. A server that lost its files then gets an InstallSnapshot of the oldest entries (with the
// leader's state at the entry before them) and follows the log from there.

namespace fs = std::filesystem;

BOOST_AUTO_TEST_CASE(keep_from, *boost::unit_test::timeout(timeout)) {
  auto io = boost::asio::io_context{};
  auto th = IoThreads{io, 8};

  auto dir     = TmpDir{};
  auto cfg     = Config{};
  auto service = RaftTestService{io, dir(), cfg, options, 3};

  service.start_all();
  service.wait_for_leader();

  // Writes over at least three segments.
  Writer writer{service};
  auto   segments = [&writer] {
    set<LogSeqNumber> s;
    for (auto const& w : writer.writes())
      s.insert(w.segment);
    return s;
  };
  for (auto end = std::chrono::steady_clock::now() + 120s;
       segments().size() < 3 && std::chrono::steady_clock::now() < end;
       std::this_thread::sleep_for(200ms)) {
  }
  BOOST_REQUIRE(segments().size() >= 3U);

  // The "latest backup" is in the third segment: the compaction (by whole ledger files) deletes
  // the first segment at least.
  auto const keep   = *std::next(segments().begin(), 2);
  auto const writes = writer.writes();
  auto const first_kept =
      std::ranges::find_if(writes, [&](auto const& w) { return !(w.segment < keep); })->idx;
  for (unsigned i = 0; i < 3; ++i)
    if (auto s = service.server(i))
      s->set_wal_keep_from(keep);
  std::this_thread::sleep_for(1s);  // the WAL deletion thread

  for (unsigned i = 0; i < 3; ++i) {
    auto s = service.server(i);
    auto [state, first] =
        s->inspect_for_test([](RaftState const& st, Ledger const& log, RaftIndex) {
          return std::make_tuple(st, log.first_idx());
        });
    // Compacted by whole ledger files: to the start of the file that holds the first entry in
    // the kept segment, and no further (so the first segment kept may be the one before).
    BOOST_TEST(first == state.first_idx, i);
    BOOST_TEST(first > RaftIndex{1}, i);
    BOOST_TEST(first <= first_kept, i);
    BOOST_TEST(!(keep < state.first_segment), i);
    BOOST_TEST(state.first_segment.lsn + 2U * kWalSegmentSize > keep.lsn, i);
    // The segments wholly before the first one kept are gone; it and the later ones are there.
    for (auto const& seg : segments()) {
      auto exists = fs::exists(service.wal_dir(i) / filename(seg));
      BOOST_TEST(exists == !(seg < state.first_segment), i << " " << filename(seg));
    }
  }

  // A follower loses everything and comes back: an InstallSnapshot of the oldest entries.
  auto const leader = service.leader().value();
  auto const node   = (leader + 1U) % 3U;
  service.stop(node, /*delete_files=*/true, /*crash=*/true);
  std::this_thread::sleep_for(1s);
  service.start(node);

  auto caught = false;
  for (auto end = std::chrono::steady_clock::now() + 60s;
       !caught && std::chrono::steady_clock::now() < end;
       std::this_thread::sleep_for(200ms)) {
    auto [state, first] = service.server(node)->inspect_for_test(
        [](RaftState const& st, Ledger const& log, RaftIndex) {
          return std::make_tuple(st, log.first_idx());
        });
    caught = state.needs_backup && first > RaftIndex{1} && first <= first_kept &&
             state.log_idx + 1000U > service.commit_idx();
  }
  BOOST_TEST(caught);

  validate(service, writer);

  service.shutdown();
  io.stop();
}
