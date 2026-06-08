#define BOOST_TEST_MODULE raft_ensemble
#include "src/base/testing.h"
#include "src/raft/raft_ensemble_connection.h"
#include "src/raft/zk_test_service.h"

using namespace std::chrono_literals;

namespace {

auto const options = Options{
    .election_timeout   = 400,
    .max_entries        = 0x1 << 13,  // Max enteries to send in an append entries request.
    .tick_seconds       = 2,          // Every two seconds we add a tick entry.
    .snapshot_lag       = 0x1 << 13,  // Create a new snapshot if it exceeds the lag, 8K
    .keep_old_logs      = 0x1 << 14,  // Keep older logs upto when compacting.
    .compaction_lag     = 0x1 << 16,  // The follower will compact the log, 65k
    .timeout_secs       = 10,
    .reconnect_interval = 2000,  // Attempt reconnect after 2 seconds.
    .log_req_response   = false,
};

std::atomic_uint client_counter;

template <unsigned N>
struct Clients {
  Clients(boost::asio::io_context& io,
          Config const&            cfg,
          std::chrono::seconds     timeout  = std::chrono::seconds(5),
          unsigned                 capacity = 40000) {
    stop.clear();

    results.reserve(capacity);
    timings.reserve(capacity);

    start_epoch = epoch_msec();
    while (threads.size() < N) {
      auto conn =
          fn::make_shared<RaftEnsembleConnection>(io,
                                                  cfg,
                                                  Role::kTest,
                                                  fn::format("{0}", client_counter.fetch_add(1)),
                                                  Certificates{cfg},
                                                  timeout);
      connections.emplace_back(conn);
      threads.emplace_back([this, conn]() { thread_fn(conn); });
    }
  }

  void append(RaftIndex idx, uint64_t increment, uint64_t value) {
    std::unique_lock<std::mutex> lk{mtx};
    results.emplace_back(idx, increment, value);
  }

  void timing(unsigned t) {
    std::unique_lock<std::mutex> lk{tmtx};
    timings.emplace_back(t);
  }
  std::atomic_uint requests{0};

  void thread_fn(shared_ptr<RaftEnsembleConnection> conn) {
    RaftIndex last_idx{0};

    conn->connect();
    while (!stop.test()) {
      uint64_t inc = static_cast<uint64_t>(rand() % 10) + 1U;

      auto       s = epoch_msec();
      ZkResponse response =
          conn->exec(ZkRequest{.commands = {IncCounterCommand{.increment = inc}}});
      timing(epoch_msec() - s);
      if ((requests.fetch_add(1) % 500) == 0)
        LOG_INFO << "Requests = " << requests.load();

      if (response.status == ResultStatus::kSessionExpired) {
        LOG_ERROR << "Session expired";
        return;
      }
      if (response.status != ResultStatus::kOk) {
        if (!stop.test()) {
          LOG_ERROR << "Got " << to_string(response.status);
          errors.fetch_add(1);
        }
      } else {
        auto idx = response.zxid;
        auto val = std::get<IncCounterResult>(response.results.at(0).result).value;

        append(idx, inc, val);

        BOOST_TEST(last_idx < idx);
        last_idx = idx;
      }
    }
    LOG_INFO << "Stopped " << conn->name();
  }

  unsigned start_epoch;

  bool validate() {
    auto duration = (epoch_msec() - start_epoch) / 1000;
    LOG_INFO << "Stopping ... requests = " << requests.load() << " at rate "
             << (requests.load() / duration) << " per seconds.";
    stop.test_and_set();
    std::for_each(
        std::begin(connections), std::end(connections), [](auto& conn) { conn->close(); });
    std::for_each(std::begin(threads), std::end(threads), [](auto& th) { th.join(); });

    LOG_INFO << "Requests " << results.size();
    LOG_INFO << "Max "
             << std::accumulate(
                    std::begin(timings), std::end(timings), 0U, [](unsigned result, unsigned t) {
                      return std::max(result, t);
                    });
    LOG_INFO << "Avg "
             << std::accumulate(std::begin(timings),
                                std::end(timings),
                                u8(0U),
                                [](uint64_t result, unsigned t) { return result + t; }) /
                    timings.size();

    std::sort(std::begin(results), std::end(results));

    uint64_t  counter = 0;
    RaftIndex last_idx{0};
    std::for_each(std::begin(results), std::end(results), [&counter, &last_idx](auto const& v) {
      auto [idx, inc, val] = v;
      counter += inc;
      BOOST_TEST(last_idx < idx);
      BOOST_TEST(counter == val);
      counter  = val;
      last_idx = idx;
    });

    return errors == 0;
  }

  std::atomic_flag stop;
  std::atomic_uint errors{0};

  boost::container::static_vector<std::thread, N>                        threads;
  boost::container::static_vector<shared_ptr<RaftEnsembleConnection>, N> connections;

  std::mutex                                        mtx;
  vector<std::tuple<RaftIndex, uint64_t, uint64_t>> results;

  std::mutex       tmtx;
  vector<unsigned> timings;
};
}  // namespace

BOOST_AUTO_TEST_CASE(leader_stopped, *boost::unit_test::timeout(100)) {
  auto op = options;

  auto dir   = TmpDir{};
  auto cfg   = test_config(dir());
  auto certs = Certificates{cfg};
  auto zk    = ZkTestService{certs, cfg, 5, 2, op};

  zk.start_all();

  auto io = boost::asio::io_context{};
  auto th = IoThreads{io, 2};

  Clients<3> clients{io, cfg, std::chrono::seconds(1)};

  std::this_thread::sleep_for(3s);
  for (unsigned i = 0; i < 3; i++) {
    auto idx = zk.leader();
    while (!idx) {
      LOG_ERROR << "Leader not found";
      std::this_thread::sleep_for(1s);
      idx = zk.leader();
    }
    zk.stop(idx.value(), /*graceful=*/true);
    std::this_thread::sleep_for(5s);
    zk.start(idx.value());
    std::this_thread::sleep_for(2s);
  }
  std::this_thread::sleep_for(5s);

  BOOST_TEST(clients.validate());

  zk.shutdown();
  io.stop();
}

BOOST_AUTO_TEST_CASE(frequent_compaction, *boost::unit_test::timeout(100)) {
  auto op           = options;
  op.keep_old_logs  = 10;
  op.compaction_lag = 48;

  auto dir   = TmpDir{};
  auto cfg   = test_config(dir());
  auto certs = Certificates{cfg};
  auto zk    = ZkTestService{certs, cfg, 5, 2, op};

  zk.start_all();

  auto io = boost::asio::io_context{};
  auto th = IoThreads{io, 2};

  Clients<3> clients{io, cfg};

  std::this_thread::sleep_for(10s);
  BOOST_TEST(clients.validate());

  zk.shutdown();
  io.stop();
}

BOOST_AUTO_TEST_CASE(stepdown, *boost::unit_test::timeout(100)) {
  auto op = options;

  auto dir   = TmpDir{};
  auto cfg   = test_config(dir());
  auto certs = Certificates{cfg};
  auto zk    = ZkTestService{certs, cfg, 5, 2, op};

  zk.start_all();

  auto io = boost::asio::io_context{};
  auto th = IoThreads{io, 2};

  Clients<3> clients{io, cfg};

  std::this_thread::sleep_for(5s);
  for (unsigned i = 0; i < 4; i++) {
    BOOST_TEST(zk.stepdown());
    std::this_thread::sleep_for(3s);
  }
  std::this_thread::sleep_for(2s);

  BOOST_TEST(clients.validate());

  zk.shutdown();
  io.stop();
}
