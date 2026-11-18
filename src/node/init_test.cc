#define BOOST_TEST_MODULE init
#include "src/node/init.h"

#include <fstream>

#include "src/backup/dir_backup_store.h"
#include "src/base/io_threads.h"
#include "src/base/testing.h"
#include "src/process/pg_instance.h"
#include "src/status/pg_client.h"

// pg_raft_initdb: init_cluster stores one verified backup, which a node can restore and start;
// a second init on the same store is refused.

namespace fs = std::filesystem;

static constexpr uint16_t kInitPort    = 15470;
static constexpr uint16_t kRestorePort = 15471;

static bool pg_available(Config const& c) {
  return access((pg_bin(c) / "postgres").c_str(), X_OK) == 0;
}

static string query(uint16_t port, char const* sql) {
  auto conn = PgClient::connect(port);
  if (!conn)
    return "";
  auto   stmt = (*conn)->prepare(sql);
  string out;
  if (stmt) {
    std::promise<Result<Rows>> p;
    (*conn)->async_exec(*stmt, [&p](Result<Rows> r) { p.set_value(std::move(r)); });
    auto rows = p.get_future().get();
    if (rows && !rows->empty() && rows->at(0).at(0))
      out = *rows->at(0).at(0);
  }
  (*conn)->close();
  return out;
}

BOOST_AUTO_TEST_CASE(backup_end_lsn_from_manifest) {
  TmpDir tmp;
  std::ofstream(tmp() / "backup_manifest") << R"({
    "PostgreSQL-Backup-Manifest-Version": 2,
    "WAL-Ranges": [
      { "Timeline": 1, "Start-LSN": "0/2000028", "End-LSN": "0/2000120" },
      { "Timeline": 2, "Start-LSN": "0/3000000", "End-LSN": "1/5A1F3B0" }
    ]
  })";
  auto lsn = backup_end_lsn(tmp());
  BOOST_REQUIRE(lsn.has_value());
  BOOST_TEST((*lsn == LogSeqNumber{.timeline = 2, .lsn = 0x105A1F3B0ULL}));

  BOOST_TEST(!backup_end_lsn(tmp() / "missing").has_value());
  std::ofstream(tmp() / "backup_manifest") << "{ not json";
  BOOST_TEST(!backup_end_lsn(tmp()).has_value());
}

BOOST_AUTO_TEST_CASE(init_cluster_creates_the_first_backup, *boost::unit_test::timeout(120)) {
  auto cfg = Config{};
  if (!pg_available(cfg)) {
    BOOST_TEST_MESSAGE("PostgreSQL not available; skipping");
    return;
  }

  IoThreads io{2};
  TmpDir    tmp;
  cfg.wdir         = (tmp() / "wdir").string();
  cfg.backup_store = (tmp() / "store").string();
  cfg.pg_config    = {{"max_connections", "50"}};
  auto store       = DirBackupStore{cfg.backup_store};

  auto info = init_cluster(cfg, store, kInitPort);
  BOOST_REQUIRE(info.has_value());
  BOOST_TEST(info->lsn.timeline == 1U);
  BOOST_TEST(info->lsn.lsn > 0U);
  BOOST_TEST(INT(info->node) == 0);
  BOOST_TEST((store.list().value() == vector<BackupInfo>{*info}));
  BOOST_TEST(store.pending().value().empty());

  // The scratch directory is gone.
  for (auto const& e : fs::directory_iterator(cfg.wdir))
    BOOST_TEST(!e.path().filename().string().starts_with("pg_raft_initdb."), e.path());

  // A node restores it: started without a signal file it recovers from its backup_label and runs
  // as a primary, with the pg-raft settings and the configured ones.
  auto data = tmp() / "restored";
  BOOST_REQUIRE(store.get(*info, data).has_value());
  {
    auto pg = PgInstance{pg_bin(cfg), data, kRestorePort, PgInstance::Role::kPrimary};
    BOOST_REQUIRE(pg.start());
    BOOST_REQUIRE(pg.wait_ready(kRestorePort));
    BOOST_TEST(query(kRestorePort, "SELECT pg_is_in_recovery()::text") == "false");
        BOOST_TEST(query(kRestorePort, "SHOW wal_sync_method") == "fdatasync");
    BOOST_TEST(query(kRestorePort, "SHOW max_connections") == "50");
    pg.stop(PgInstance::StopMode::Fast);
  }

  // A second cluster in the same store is refused.
  BOOST_TEST(!init_cluster(cfg, store, kInitPort).has_value());
  BOOST_TEST(store.list().value().size() == 1U);
}
