#define BOOST_TEST_MODULE node_backup
#include "src/backup/dir_backup_store.h"
#include "src/base/io_threads.h"
#include "src/base/testing.h"
#include "src/node/init.h"
#include "src/node/node.h"
#include "src/status/pg_client.h"

// Backups after bootstrap (docs/backup.md §4): with backup_interval = 15 s, a standby (never the
// primary) takes a pg_basebackup on its staggered deadline, under a lease, and stores it; the
// backup restores and holds the data.

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

constexpr unsigned kNodes       = 3;
constexpr uint16_t kPgPort      = 15560;  // + i
constexpr uint16_t kHaPort      = 15570;  // + i
constexpr uint16_t kInitPort    = 15580;
constexpr uint16_t kRestorePort = 15581;

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

bool wait_for(std::chrono::seconds timeout, std::function<bool()> const& pred) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred())
      return true;
    std::this_thread::sleep_for(500ms);
  }
  return pred();
}

}  // namespace

BOOST_AUTO_TEST_CASE(standby_takes_backups, *boost::unit_test::timeout(600)) {
  auto cfg = Config{};
  if (access((pg_bin(cfg) / "postgres").c_str(), X_OK) != 0) {
    BOOST_TEST_MESSAGE("PostgreSQL not available; skipping");
    return;
  }

  IoThreads io{16};
  TmpDir    tmp;
  cfg.wdir                 = (tmp() / "w").string();
  cfg.backup_store         = (tmp() / "store").string();
  cfg.backup_interval_secs = 15;
  cfg.ledger_file_size     = uint64_t{64} << 20U;  // the smallest: each node zero-fills a spare
  for (unsigned i = 0; i < kNodes; ++i)
    cfg.servers.push_back({.hostname = "127.0.0.1",
                           .pg_port  = static_cast<uint16_t>(kPgPort + i),
                           .ha_port  = static_cast<uint16_t>(kHaPort + i)});

  auto store = DirBackupStore{cfg.backup_store};
  BOOST_REQUIRE(init_cluster(cfg, store, kInitPort).has_value());

  auto options                        = NodeOptions{};
  options.raft.election_timeout_msecs = 1000;
  options.backup_check_interval       = 2s;

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

  // Some data, then a backup started after it.
  Process p((pg_bin(cfg) / "pgbench").string(),
            {"-h", "127.0.0.1", "-p", std::to_string(primary->pg_port()), "-i", "postgres"});
  BOOST_REQUIRE(p.start());
  BOOST_REQUIRE_EQUAL(p.wait(), 0);
  auto const after = std::time(nullptr);

  std::optional<BackupInfo> backup;
  BOOST_REQUIRE(wait_for(150s, [&] {
    auto latest = store.latest();
    if (latest && *latest && (*latest)->time > after)
      backup = **latest;
    return backup.has_value();
  }));

  // A standby's, on the cluster's timeline, and no lease left over.
  BOOST_TEST(backup->node != primary->id());
  BOOST_TEST(backup->lsn.timeline == 2U);
  BOOST_TEST(wait_for(30s, [&] { return store.pending().value().empty(); }));

  for (auto& n : nodes)
    n->shutdown();

  // It restores: started on its own (no standby.signal), it recovers from its backup_label.
  auto data = tmp() / "restored";
  BOOST_REQUIRE(store.get(*backup, data).has_value());
  fs::remove(data / "standby.signal");
  fs::remove(data / "pg_raft.conf");
  auto pg = PgInstance{pg_bin(cfg), data, kRestorePort, PgInstance::Role::kPrimary};
  BOOST_REQUIRE(pg.start());
  BOOST_REQUIRE(pg.wait_ready(kRestorePort, 60));
  BOOST_TEST(query(kRestorePort, "SELECT count(*)::text FROM pgbench_accounts") == "100000");
  pg.stop(PgInstance::StopMode::Fast);
}
