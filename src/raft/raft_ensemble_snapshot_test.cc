#define BOOST_TEST_MODULE raft_ensemble_snapshot
#include "src/raft/raft_ensemble_test_common.h"

// With frequent compaction, a node that lost its files gets an InstallSnapshot (the latest entries
// and their segments), catches up, and needs a PostgreSQL backup.

BOOST_AUTO_TEST_CASE(install_snapshot, *boost::unit_test::timeout(timeout)) {
  auto io = boost::asio::io_context{};
  auto th = IoThreads{io, 8};

  auto op             = options;
  op.compaction_lag   = 500;
  op.keep_old_logs    = 100;
  op.snapshot_entries = 200;

  auto dir     = TmpDir{};
  auto cfg     = Config{};
  auto service = RaftTestService{io, dir(), cfg, op, 3};

  service.start_all();
  service.wait_for_leader();

  // A follower loses everything, and stays down while the others compact their logs.
  auto leader = service.leader().value();
  auto node   = (leader + 1U) % 3U;
  service.stop(node, /*delete_files=*/true, /*crash=*/true);

  Writer writer{service};
  while (service.commit_idx() < RaftIndex{3000})
    std::this_thread::sleep_for(100ms);

  service.start(node);

  // The node catches up while the leader is busy: it gets a snapshot, then follows the log.
  auto state  = RaftState{};
  auto caught = false;
  for (auto end = std::chrono::steady_clock::now() + 30s;
       !caught && std::chrono::steady_clock::now() < end;
       std::this_thread::sleep_for(100ms)) {
    state = service.server(node)->inspect_for_test(
        [](RaftState const& s, RaftLog const&, RaftIndex) { return s; });
    caught = state.needs_backup && state.log_idx + 1000U > service.commit_idx();
  }
  log(INFO, "Node {} state {}", node, state);
  BOOST_TEST(caught);

  validate(service, writer);

  service.shutdown();
  io.stop();
}
