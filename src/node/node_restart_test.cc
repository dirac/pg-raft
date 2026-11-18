#define BOOST_TEST_MODULE node_restart
#include <random>

#include "src/backup/dir_backup_store.h"
#include "src/base/io_threads.h"
#include "src/base/testing.h"
#include "src/node/init.h"
#include "src/node/node.h"
#include "src/status/pg_client.h"

// A node's restart: after an unclean end of its previous run (node<N>/running left behind), the
// node rebuilds pg/pg_wal from raft/wal before PostgreSQL starts on it, so WAL that never committed
// (here a planted segment raft never had) is gone; after a clean shutdown it starts on pg/pg_wal as
// it is. Either way the standby converges with the primary.

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

constexpr unsigned kNodes    = 3;
constexpr uint16_t kPgPort   = 15700;  // + i
constexpr uint16_t kHaPort   = 15710;  // + i
constexpr uint16_t kInitPort = 15720;

string query(uint16_t port, char const* sql) {
  auto conn = PgClient::connect(port);
  if (!conn)
    return "";
  string out;
  if (auto stmt = (*conn)->prepare(sql); stmt) {
    std::promise<Result<Rows>> p;
    (*conn)->async_exec(*stmt, [&p](Result<Rows> r) { p.set_value(std::move(r)); });
    if (auto rows = p.get_future().get(); rows && !rows->empty() && rows->at(0).at(0))
      out = *rows->at(0).at(0);
  }
  (*conn)->close();
  return out;
}

uint64_t lsn(string const& s) {
  unsigned hi = 0, lo = 0;
  sscanf(s.c_str(), "%X/%X", &hi, &lo);
  return (uint64_t{hi} << 32U) | lo;
}

bool wait_for(std::chrono::seconds timeout, std::function<bool()> const& pred) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred())
      return true;
    std::this_thread::sleep_for(500ms);
  }
  return pred();
}

void pgbench(Config const& cfg, uint16_t port, std::vector<string> args) {
  args.insert(args.begin(), {"-h", "127.0.0.1", "-p", std::to_string(port)});
  args.push_back("postgres");
  Process p((pg_bin(cfg) / "pgbench").string(), args);
  BOOST_REQUIRE(p.start());
  BOOST_REQUIRE_EQUAL(p.wait(), 0);
}

/// A segment of random bytes, far ahead of the WAL: what raft never had.
void plant(fs::path const& path) {
  std::mt19937 gen(7);
  Bytes        b(kWalSegmentSize);
  for (auto& c : b)
    c = static_cast<uint8_t>(gen());
  std::ofstream(path, std::ios::binary)
      .write(reinterpret_cast<char const*>(b.data()), static_cast<std::streamsize>(b.size()));
}

}  // namespace

BOOST_AUTO_TEST_CASE(restart_rebuilds_after_unclean_end, *boost::unit_test::timeout(900)) {
  auto cfg = Config{};
  if (access((pg_bin(cfg) / "postgres").c_str(), X_OK) != 0) {
    BOOST_TEST_MESSAGE("PostgreSQL not available; skipping");
    return;
  }

  IoThreads io{16};
  TmpDir    tmp;
  cfg.wdir             = (tmp() / "w").string();
  cfg.backup_store     = (tmp() / "store").string();
  cfg.ledger_file_size = uint64_t{64} << 20U;  // the smallest: each node zero-fills a spare
  for (unsigned i = 0; i < kNodes; ++i)
    cfg.servers.push_back({.hostname = "127.0.0.1",
                           .pg_port  = static_cast<uint16_t>(kPgPort + i),
                           .ha_port  = static_cast<uint16_t>(kHaPort + i)});

  auto store = DirBackupStore{cfg.backup_store};
  BOOST_REQUIRE(init_cluster(cfg, store, kInitPort).has_value());

  auto options                        = NodeOptions{};
  options.raft.election_timeout_msecs = 1000;

  vector<shared_ptr<Node>> nodes;
  for (unsigned i = 0; i < kNodes; ++i) {
    nodes.push_back(Node::create(cfg, RaftNodeId(static_cast<uint16_t>(i)), options));
    BOOST_REQUIRE(nodes.back()->start().has_value());
  }
  shared_ptr<Node> primary;
  BOOST_REQUIRE(wait_for(120s, [&] {
    for (auto const& n : nodes)
      if (n->is_primary())
        primary = n;
    return primary != nullptr;
  }));
  pgbench(cfg, primary->pg_port(), {"-i", "-s", "1"});

  auto const victim = INT(primary->id()) == 0U ? 1U : 0U;
  auto const dir    = fs::path{cfg.wdir} / std::format("node{}", victim);
  auto const junk   = dir / "pg" / "pg_wal" / "00000002000000000000007F";
  BOOST_TEST(fs::exists(dir / "running"));

  // Writes, then the victim catches up and its node restarts; returns when it has converged.
  auto converge = [&](char const* what) {
    pgbench(cfg, primary->pg_port(), {"-n", "-t", "100", "-c", "4"});
    auto const flushed = lsn(query(primary->pg_port(), "SELECT pg_current_wal_flush_lsn()::text"));
    auto const rows    = query(primary->pg_port(), "SELECT count(*)::text FROM pgbench_history");
    auto const port    = nodes.at(victim)->pg_port();
    BOOST_TEST(wait_for(
                   180s,
                   [&] { return lsn(query(port, "SELECT pg_last_wal_replay_lsn()::text")) >= flushed; }),
               what);
    BOOST_TEST(query(port, "SELECT count(*)::text FROM pgbench_history") == rows, what);
  };
  auto restart = [&] {
    nodes.at(victim) = Node::create(cfg, RaftNodeId(static_cast<uint16_t>(victim)), options);
    BOOST_REQUIRE(nodes.at(victim)->start().has_value());
    BOOST_TEST(fs::exists(dir / "running"));
  };

  // A clean shutdown removes the marker.
  nodes.at(victim)->shutdown();
  nodes.at(victim).reset();
  BOOST_TEST(!fs::exists(dir / "running"));

  // The previous run ended uncleanly (the marker is there), and pg/pg_wal holds WAL that raft never
  // had: the start rebuilds pg/pg_wal, which drops it.
  plant(junk);
  std::ofstream(dir / "running").close();
  restart();
  BOOST_TEST(!fs::exists(junk));
  converge("after the rebuild");

  // A clean shutdown, then a start on pg/pg_wal as it is: no rebuild.
  nodes.at(victim)->shutdown();
  nodes.at(victim).reset();
  BOOST_TEST(!fs::exists(dir / "running"));
  plant(junk);
  restart();
  BOOST_TEST(fs::exists(junk));
  fs::remove(junk);
  converge("after a clean restart");

  for (auto& n : nodes)
    if (n)
      n->shutdown();
}
