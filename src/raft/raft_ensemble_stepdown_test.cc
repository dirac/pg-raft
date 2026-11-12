#define BOOST_TEST_MODULE raft_ensemble_stepdown
#include "src/raft/raft_ensemble_test_common.h"

// Ported from sparrowdb lib/servers/zqlite/raft_ensemble_stepdown_test.cpp: the leader hands over
// the leadership (RequestElection) a few times.

BOOST_AUTO_TEST_CASE(stepdown, *boost::unit_test::timeout(timeout)) {
  auto io = boost::asio::io_context{};
  auto th = IoThreads{io, 8};

  auto dir     = TmpDir{};
  auto cfg     = Config{};
  auto service = RaftTestService{io, dir(), cfg, options, 3};

  service.start_all();
  service.wait_for_leader();

  Writer writer{service};

  for (unsigned i = 0; i < 4; i++) {
    std::this_thread::sleep_for(3s);
    auto before = service.leader();
    BOOST_TEST(service.stepdown());
    std::this_thread::sleep_for(3s);
    auto after = service.leader();
    BOOST_TEST((after.has_value() && after != before));
  }

  validate(service, writer);

  service.shutdown();
  io.stop();
}
