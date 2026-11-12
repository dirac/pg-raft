#define BOOST_TEST_MODULE raft_ensemble_sanity
#include "src/raft/raft_ensemble_test_common.h"

// Ported from sparrowdb lib/servers/zqlite/raft_ensemble_sanity_test.cpp.

BOOST_AUTO_TEST_CASE(sanity, *boost::unit_test::timeout(timeout)) {
  auto io = boost::asio::io_context{};
  auto th = IoThreads{io, 8};

  auto dir     = TmpDir{};
  auto cfg     = Config{};
  auto service = RaftTestService{io, dir(), cfg, options, 5};

  service.start_all();
  service.wait_for_leader();

  Writer writer{service};
  std::this_thread::sleep_for(20s);

  validate(service, writer);
  BOOST_TEST(writer.errors() == 0U);

  service.shutdown();
  io.stop();
}
