#define BOOST_TEST_MODULE node
#include "src/node/node.h"

#include "src/backup/dir_backup_store.h"
#include "src/base/io_threads.h"
#include "src/base/testing.h"
#include "src/node/init.h"
#include "src/proxy/proxy.h"
#include "src/status/pg_client.h"

// docs/node.md test 1, the steady state: pg_raft_initdb, then 3 nodes in one process (3 raft
// servers, 3 FUSE mounts, 3 PostgreSQL instances). One becomes the primary (timeline 2, after the
// promotion of the restored backup), pgbench runs on it, and the two standbys converge through
// their WAL senders, by LSN and row count. Then a hand-over: the primary steps down, another
// promotes (timeline 3), the old one is demoted to a standby, and all converge again. pgbench
// runs through the proxy's primary port, which follows the primary.

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

constexpr unsigned kNodes     = 3;
constexpr uint16_t kPgPort    = 15530;  // + i
constexpr uint16_t kHaPort    = 15540;  // + i
constexpr uint16_t kInitPort  = 15550;
constexpr uint16_t kProxyPort = 15660;  // + 1 + i for the servers

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

}  // namespace

BOOST_AUTO_TEST_CASE(steady_state, *boost::unit_test::timeout(600)) {
  auto cfg = Config{};
  if (access((pg_bin(cfg) / "postgres").c_str(), X_OK) != 0) {
    BOOST_TEST_MESSAGE("PostgreSQL not available; skipping");
    return;
  }

  IoThreads io{16};
  TmpDir    tmp;
  cfg.wdir         = (tmp() / "w").string();
  cfg.backup_store = (tmp() / "store").string();
  cfg.socket_dir   = "/tmp";
  cfg.ledger_file_size     = uint64_t{64} << 20U;  // the smallest: each node zero-fills a spare
  for (unsigned i = 0; i < kNodes; ++i)
    cfg.servers.push_back({.hostname = "127.0.0.1",
                           .pg_port  = static_cast<uint16_t>(kPgPort + i),
                           .ha_port  = static_cast<uint16_t>(kHaPort + i)});

  cfg.proxy_pg_port   = kProxyPort;
  cfg.proxy_http_port = 0;  // the admin interface on an ephemeral port

  auto store = DirBackupStore{cfg.backup_store};
  BOOST_REQUIRE(init_cluster(cfg, store, kInitPort).has_value());

  // The clients go through the proxy's primary port.
  auto proxy_options           = ProxyOptions{};
  proxy_options.listen_address = "127.0.0.1";
  auto proxy                   = Proxy::create(io_context(), cfg, proxy_options);
  BOOST_REQUIRE(proxy->start().has_value());

  auto options                        = NodeOptions{};
  options.raft.election_timeout_msecs = 1000;

  vector<shared_ptr<Node>> nodes;
  for (unsigned i = 0; i < kNodes; ++i) {
    nodes.push_back(Node::create(cfg, RaftNodeId(static_cast<uint16_t>(i)), options));
    auto r = nodes.back()->start();
    BOOST_REQUIRE_MESSAGE(r.has_value(), "node " << i << ": " << (r ? "" : r.error().message()));
  }

  // One primary, on timeline 2.
  shared_ptr<Node> primary;
  BOOST_REQUIRE(wait_for(120s, [&] {
    for (auto const& n : nodes)
      if (n->is_primary())
        primary = n;
    return primary != nullptr;
  }));
  BOOST_TEST(std::ranges::count_if(nodes, [](auto const& n) { return n->is_primary(); }) == 1);
  BOOST_TEST(query(primary->pg_port(), "SELECT pg_is_in_recovery()::text") == "false");
  BOOST_TEST(query(primary->pg_port(),
                   "SELECT substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8)") == "00000002");
  BOOST_TEST(primary->raft()->serving());

  // Writes on the primary: every commit waits for a quorum.
  BOOST_REQUIRE(wait_for(
      30s, [&] { return proxy->primary() == std::optional<unsigned>{INT(primary->id())}; }));
  pgbench(cfg, kProxyPort, {"-i"});
  pgbench(cfg, kProxyPort, {"-t", "200", "-c", "4"});
  auto flushed = lsn(query(primary->pg_port(), "SELECT pg_current_wal_flush_lsn()::text"));
  auto rows    = query(primary->pg_port(), "SELECT count(*)::text FROM pgbench_history");
  BOOST_TEST(rows == "800");

  // The standbys converge: on timeline 2, past the primary's flush LSN, with the same rows.
  for (auto const& n : nodes) {
    if (n == primary)
      continue;
    auto port = n->pg_port();
    BOOST_TEST(
        wait_for(
            120s,
            [&] { return lsn(query(port, "SELECT pg_last_wal_replay_lsn()::text")) >= flushed; }),
        "node " << n->id());
    BOOST_TEST(query(port, "SELECT pg_is_in_recovery()::text") == "true");
    BOOST_TEST(query(port, "SELECT received_tli::text FROM pg_stat_wal_receiver") == "2");
    BOOST_TEST(query(port, "SELECT count(*)::text FROM pgbench_history") == rows);
  }

  // A hand-over: the primary steps down and kills its PostgreSQL, another node promotes it to
  // timeline 3, and the old primary comes back as a standby, its pg_wal rebuilt from raft/wal.
  auto old = primary;
  old->raft()->stepdown();
  primary.reset();
  BOOST_REQUIRE(wait_for(120s, [&] {
    for (auto const& n : nodes)
      if (n != old && n->is_primary())
        primary = n;
    return primary != nullptr;
  }));
  BOOST_TEST(query(primary->pg_port(),
                   "SELECT substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8)") == "00000003");
  BOOST_TEST(wait_for(
      120s, [&] { return query(old->pg_port(), "SELECT pg_is_in_recovery()::text") == "true"; }));

  // -n: no vacuum and no truncation of pgbench_history, so that the rows of before the hand-over
  // must still be there.
  BOOST_REQUIRE(wait_for(
      30s, [&] { return proxy->primary() == std::optional<unsigned>{INT(primary->id())}; }));
  pgbench(cfg, kProxyPort, {"-n", "-t", "100", "-c", "4"});
  flushed = lsn(query(primary->pg_port(), "SELECT pg_current_wal_flush_lsn()::text"));
  rows    = query(primary->pg_port(), "SELECT count(*)::text FROM pgbench_history");
  BOOST_TEST(rows == "1200");

  for (auto const& n : nodes) {
    if (n == primary)
      continue;
    auto port = n->pg_port();
    BOOST_TEST(
        wait_for(
            120s,
            [&] { return lsn(query(port, "SELECT pg_last_wal_replay_lsn()::text")) >= flushed; }),
        "node " << n->id());
    BOOST_TEST(query(port, "SELECT received_tli::text FROM pg_stat_wal_receiver") == "3");
    BOOST_TEST(query(port, "SELECT count(*)::text FROM pgbench_history") == rows);
  }

  proxy->stop();
  for (auto& n : nodes)
    n->shutdown();
}
