#define BOOST_TEST_MODULE raft_ensemble_rematerialize
#include "src/raft/raft_ensemble_test_common.h"

// wal_dir is derived from the ledger (docs/ledger.md §9): a checkpoint saves the applied state, and
// a follower that loses its wal_dir and its saved state applies its ledger again, which writes
// every committed byte back.

namespace fs = std::filesystem;

BOOST_AUTO_TEST_CASE(rematerialize, *boost::unit_test::timeout(timeout)) {
  auto io = boost::asio::io_context{};
  auto th = IoThreads{io, 8};

  auto dir     = TmpDir{};
  auto cfg     = Config{};
  auto service = RaftTestService{io, dir(), cfg, options, 3};

  service.start_all();
  service.wait_for_leader();

  Writer writer{service};
  std::this_thread::sleep_for(5s);

  // A checkpoint saves the applied state (the entries' part of it) in the meta file.
  auto const leader = service.leader().value();
  auto const node   = (leader + 1U) % 3U;
  auto       s      = service.server(node);
  s->checkpoint_for_test();
  auto const applied =
      s->inspect_for_test([](RaftState const& st, Ledger const&, RaftIndex) { return st; });
  auto meta = read_meta_file(service.raft_dir(node) / "meta");
  BOOST_REQUIRE(meta.has_value());
  BOOST_TEST(meta->state.log_idx > RaftIndex{0});
  BOOST_TEST(meta->state.log_idx <= applied.log_idx);

  // A follower crashes and loses its wal_dir and its saved state; its ledger stays.
  s.reset();
  service.stop(node, /*delete_files=*/false, /*crash=*/true);
  std::this_thread::sleep_for(1s);
  fs::remove_all(service.wal_dir(node));
  fs::remove(service.raft_dir(node) / "meta");
  service.start(node);

  // It applies its whole ledger again, under the writes that go on.
  std::this_thread::sleep_for(5s);
  auto c = validate(service, writer);
  BOOST_TEST(writer.errors() == 0U);
  auto st = service.server(node)->inspect_for_test(
      [](RaftState const& st, Ledger const&, RaftIndex) { return st; });
  BOOST_TEST(st.log_idx > applied.log_idx);

  service.shutdown();
  io.stop();
}
