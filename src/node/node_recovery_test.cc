#define BOOST_TEST_MODULE node_recovery
#include "src/backup/dir_backup_store.h"
#include "src/base/io_threads.h"
#include "src/base/testing.h"
#include "src/node/init.h"
#include "src/node/node.h"
#include "src/status/pg_client.h"

// docs/node.md test 3: a node that lost everything comes back. With backups every 15 s, the
// cluster keeps its raft entries from the latest backup on (set_wal_keep_from), so the leader
// compacts. A follower's whole directory is deleted; it restarts, restores the latest backup, gets
// an InstallSnapshot of the leader's oldest entries (and restores again, needs_backup), and
// catches up with the primary.

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

constexpr unsigned kNodes    = 3;
constexpr uint16_t kPgPort   = 15600;  // + i
constexpr uint16_t kHaPort   = 15610;  // + i
constexpr uint16_t kInitPort = 15620;

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

BOOST_AUTO_TEST_CASE(wiped_node_recovers, *boost::unit_test::timeout(600)) {
  auto cfg = Config{};
  config_from_env(cfg);  // PG_RAFT_WAL_CAPTURE=hook etc. (testing.h)
  if (access((pg_bin(cfg) / "postgres").c_str(), X_OK) != 0) {
    BOOST_TEST_MESSAGE("PostgreSQL not available; skipping");
    return;
  }

  IoThreads io{16};
  TmpDir    tmp;
  cfg.wdir                 = (tmp() / "w").string();
  cfg.backup_store         = (tmp() / "store").string();
  cfg.backup_interval_secs = 15;
  for (unsigned i = 0; i < kNodes; ++i)
    cfg.servers.push_back({.hostname = "127.0.0.1",
                           .pg_port  = static_cast<uint16_t>(kPgPort + i),
                           .ha_port  = static_cast<uint16_t>(kHaPort + i)});

  auto store = DirBackupStore{cfg.backup_store};
  BOOST_REQUIRE(init_cluster(cfg, store, kInitPort).has_value());

  auto options                        = NodeOptions{};
  options.raft.election_timeout_msecs = 1000;
  options.ledger_file_size_for_test   = size_t{4} << 20U;  // the WAL spans several files
  options.backup_check_interval       = 2s;

  vector<shared_ptr<Node>> nodes;
  for (unsigned i = 0; i < kNodes; ++i) {
    nodes.push_back(Node::create(cfg, RaftNodeId(static_cast<uint16_t>(i)), options));
    BOOST_REQUIRE(nodes.back()->start().has_value());
  }
  auto primary_of = [&] {
    shared_ptr<Node> p;
    for (auto const& n : nodes)
      if (n && n->is_primary())
        p = n;
    return p;
  };
  BOOST_REQUIRE(wait_for(120s, [&] { return primary_of() != nullptr; }));
  auto primary = primary_of();

  // WAL over a few segments, then a backup after it: the leader compacts up to its segment.
  pgbench(cfg, primary->pg_port(), {"-i", "-s", "2"});
  pgbench(cfg, primary->pg_port(), {"-n", "-t", "200", "-c", "4"});
  auto const after = std::time(nullptr);
  BOOST_REQUIRE(wait_for(150s, [&] {
    auto latest = store.latest();
    return latest && *latest && (*latest)->time > after;
  }));
  auto const latest = store.latest().value().value();
  // By whole ledger files: the first file kept may start in the segment before.
  BOOST_REQUIRE(wait_for(60s, [&] {
    auto s = primary->raft()->applied_state();
    return s.first_idx > RaftIndex{1} &&
           s.first_segment.lsn + kWalSegmentSize >= keep_from(latest).lsn;
  }));

  // A follower loses everything.
  auto const victim = [&] {
    for (unsigned i = 0; i < kNodes; ++i)
      if (nodes.at(i) != primary)
        return i;
    return 0U;
  }();
  nodes.at(victim)->shutdown();
  nodes.at(victim).reset();
  fs::remove_all(fs::path{cfg.wdir} / std::format("node{}", victim));

  // It comes back, restores, and follows: more writes, then the same LSN and rows.
  nodes.at(victim) = Node::create(cfg, RaftNodeId(static_cast<uint16_t>(victim)), options);
  BOOST_REQUIRE(nodes.at(victim)->start().has_value());
  pgbench(cfg, primary->pg_port(), {"-n", "-t", "100", "-c", "4"});
  auto const flushed = lsn(query(primary->pg_port(), "SELECT pg_current_wal_flush_lsn()::text"));
  auto const rows    = query(primary->pg_port(), "SELECT count(*)::text FROM pgbench_history");
  BOOST_TEST(rows == "1200");

  auto const port = nodes.at(victim)->pg_port();
  BOOST_TEST(wait_for(
      240s, [&] { return lsn(query(port, "SELECT pg_last_wal_replay_lsn()::text")) >= flushed; }));
  BOOST_TEST(query(port, "SELECT count(*)::text FROM pgbench_history") == rows);
  BOOST_TEST(!nodes.at(victim)->raft()->applied_state().needs_backup);

  for (auto& n : nodes)
    if (n)
      n->shutdown();
}
