#define BOOST_TEST_MODULE raft_ensemble_needs_backup
#include "src/raft/raft_ensemble_test_common.h"

// A node with needs_backup set (no usable PostgreSQL) never becomes the leader: it doesn't start
// elections and doesn't take one from a RequestElection, but it still votes and replicates.

BOOST_AUTO_TEST_CASE(needs_backup_never_leads, *boost::unit_test::timeout(timeout)) {
  auto io = boost::asio::io_context{};
  auto th = IoThreads{io, 8};

  auto dir     = TmpDir{};
  auto cfg     = Config{};
  auto service = RaftTestService{io, dir(), cfg, options, 3};

  service.start_all();
  service.wait_for_leader();

  Writer writer{service};
  std::this_thread::sleep_for(2s);

  // The two followers need a backup.
  auto const leader = service.leader().value();
  for (unsigned i = 0; i < 3; ++i)
    if (i != leader)
      service.server(i)->set_needs_backup(true);

  // A step-down finds no node that may take over, so the leader stays.
  BOOST_TEST(service.stepdown());
  std::this_thread::sleep_for(4s);
  BOOST_TEST((service.leader() == std::optional<unsigned>{leader}));

  // Without the leader there is no leader at all: the other two never start an election.
  service.stop(leader, /*delete_files=*/false, /*crash=*/true);
  std::this_thread::sleep_for(5s);
  BOOST_TEST(!service.leader().has_value());

  // The leader comes back and leads again; the others still replicate.
  service.start(leader);
  service.wait_for_leader();
  BOOST_TEST((service.leader() == std::optional<unsigned>{leader}));
  std::this_thread::sleep_for(2s);

  // needs_backup is persisted: a follower that restarts still doesn't lead.
  auto const other = (leader + 1U) % 3U;
  service.stop(other, /*delete_files=*/false, /*crash=*/true);
  service.start(other);
  BOOST_TEST(service.server(other)->inspect_for_test(
      [](RaftState const& s, RaftLog const&, RaftIndex) { return s.needs_backup; }));

  // Cleared, it may lead again.
  for (unsigned i = 0; i < 3; ++i)
    if (i != leader)
      service.server(i)->set_needs_backup(false);
  BOOST_TEST(service.stepdown());
  std::this_thread::sleep_for(4s);
  BOOST_TEST((service.leader().has_value() && service.leader() != std::optional<unsigned>{leader}));

  validate(service, writer);

  service.shutdown();
  io.stop();
}
