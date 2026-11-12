#define BOOST_TEST_MODULE raft_ensemble_leader_stopped
#include "src/raft/raft_ensemble_test_common.h"

// The leader crashes three times, and comes back.

BOOST_AUTO_TEST_CASE(leader_stopped, *boost::unit_test::timeout(timeout)) {
  auto io = boost::asio::io_context{};
  auto th = IoThreads{io, 8};

  auto dir     = TmpDir{};
  auto cfg     = Config{};
  auto service = RaftTestService{io, dir(), cfg, options, 5};

  service.start_all();
  service.wait_for_leader();

  Writer writer{service};

  std::this_thread::sleep_for(3s);
  for (unsigned i = 0; i < 3; i++) {
    auto idx = service.leader();
    while (!idx) {
      log(ERROR, "Leader not found");
      std::this_thread::sleep_for(1s);
      idx = service.leader();
    }
    service.stop(idx.value(), /*delete_files=*/false, /*crash=*/true);
    std::this_thread::sleep_for(5s);
    service.start(idx.value());
    std::this_thread::sleep_for(2s);
  }
  std::this_thread::sleep_for(5s);

  validate(service, writer);

  service.shutdown();
  io.stop();
}
