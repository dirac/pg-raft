#define BOOST_TEST_MODULE pg_instance

#include "src/base/io_threads.h"
#include "src/base/testing.h"
#include "src/process/pg_instance.h"

static std::string const  kPgBin = "/opt/pg_raft/postgresql/17/bin";
static constexpr uint16_t kPort  = 15438;

static bool pg_available() {
  return access((kPgBin + "/postgres").c_str(), X_OK) == 0;
}

static std::string read_file(std::filesystem::path const& p) {
  std::ifstream f(p);
  return {std::istreambuf_iterator<char>(f), {}};
}

static int count_regular_files(std::filesystem::path const& dir) {
  int n = 0;
  for (auto const& e : std::filesystem::directory_iterator(dir))
    if (std::filesystem::is_regular_file(e))
      ++n;
  return n;
}

// ── init ──────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(init, * boost::unit_test::timeout(60)) {
  if (!pg_available()) { BOOST_TEST_MESSAGE("PostgreSQL not found; skipping"); return; }
  IoThreads threads{1};
  TmpDir    tmp;
  auto data_dir = tmp() / "pgdata";

  PgInstance pg(kPgBin, data_dir, kPort);
  BOOST_TEST(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_TEST(std::filesystem::exists(data_dir / "postgresql.conf"));
  BOOST_TEST(std::filesystem::exists(data_dir / "pg_wal"));
  BOOST_TEST(count_regular_files(data_dir / "pg_wal") > 0);
}

// ── write_config ──────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(write_config_appends_defaults, * boost::unit_test::timeout(60)) {
  if (!pg_available()) { BOOST_TEST_MESSAGE("PostgreSQL not found; skipping"); return; }
  IoThreads threads{1};
  TmpDir    tmp;
  auto data_dir = tmp() / "pgdata";

  PgInstance pg(kPgBin, data_dir, kPort);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_TEST(pg.write_config({{"max_connections", "50"}}));

  auto conf = read_file(data_dir / "postgresql.conf");
  BOOST_TEST(conf.find("max_connections = 50") != std::string::npos);
  BOOST_TEST(conf.find("wal_sync_method") != std::string::npos);
  BOOST_TEST(conf.find("wal_recycle") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(write_config_overrides_default, * boost::unit_test::timeout(60)) {
  if (!pg_available()) { BOOST_TEST_MESSAGE("PostgreSQL not found; skipping"); return; }
  IoThreads threads{1};
  TmpDir    tmp;
  auto data_dir = tmp() / "pgdata";

  PgInstance pg(kPgBin, data_dir, kPort);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  // wal_recycle defaults to "off"; user overrides with "on".
  BOOST_TEST(pg.write_config({{"wal_recycle", "on"}}));

  auto conf = read_file(data_dir / "postgresql.conf");
  BOOST_TEST(conf.find("wal_recycle = on") != std::string::npos);
  BOOST_TEST(conf.find("wal_recycle = off") == std::string::npos);
}

// ── write_hba ─────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(write_hba_appends_lines_and_defaults, * boost::unit_test::timeout(60)) {
  if (!pg_available()) { BOOST_TEST_MESSAGE("PostgreSQL not found; skipping"); return; }
  IoThreads threads{1};
  TmpDir    tmp;
  auto data_dir = tmp() / "pgdata";

  PgInstance pg(kPgBin, data_dir, kPort);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_TEST(pg.write_hba({"host  replication  all  127.0.0.1/32  trust"}));

  auto hba = read_file(data_dir / "pg_hba.conf");
  BOOST_TEST(hba.find("host  replication  all  127.0.0.1/32  trust") != std::string::npos);
  BOOST_TEST(hba.find("scram-sha-256") != std::string::npos);
}

// ── create_raft_wal ───────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(create_raft_wal_moves_segments, * boost::unit_test::timeout(60)) {
  if (!pg_available()) { BOOST_TEST_MESSAGE("PostgreSQL not found; skipping"); return; }
  IoThreads threads{1};
  TmpDir    tmp;
  auto data_dir    = tmp() / "pgdata";
  auto raft_dir    = tmp() / "pgraft";
  auto raft_pg_wal = raft_dir / "pg_wal";
  std::filesystem::create_directory(raft_dir);

  PgInstance pg(kPgBin, data_dir, kPort);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));

  int initial = count_regular_files(data_dir / "pg_wal");
  BOOST_REQUIRE_GT(initial, 0);

  BOOST_TEST(pg.create_raft_wal(raft_dir));

  // All segments moved to raft_dir/pg_wal.
  BOOST_TEST(count_regular_files(raft_pg_wal) == initial);
  // pg_wal is now empty, ready for a FUSE mount.
  BOOST_TEST(count_regular_files(data_dir / "pg_wal") == 0);
}

// ── start / stop / is_running ─────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(start_and_stop, * boost::unit_test::timeout(60)) {
  if (!pg_available()) { BOOST_TEST_MESSAGE("PostgreSQL not found; skipping"); return; }
  IoThreads threads{1};
  TmpDir    tmp;
  auto data_dir = tmp() / "pgdata";

  PgInstance pg(kPgBin, data_dir, kPort, PgInstance::Role::kPrimary);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_REQUIRE(pg.write_config({}));
  BOOST_REQUIRE(pg.write_hba({}));

  BOOST_TEST(!pg.is_running());
  BOOST_TEST(pg.start());
  BOOST_TEST(pg.is_running());
  BOOST_TEST(pg.wait_ready(kPort));

  pg.stop();
  BOOST_TEST(!pg.is_running());
}

BOOST_AUTO_TEST_CASE(stop_immediate, * boost::unit_test::timeout(60)) {
  if (!pg_available()) { BOOST_TEST_MESSAGE("PostgreSQL not found; skipping"); return; }
  IoThreads threads{1};
  TmpDir    tmp;
  auto data_dir = tmp() / "pgdata";

  PgInstance pg(kPgBin, data_dir, kPort, PgInstance::Role::kPrimary);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_REQUIRE(pg.write_config({}));
  BOOST_REQUIRE(pg.write_hba({}));
  BOOST_REQUIRE(pg.start());
  BOOST_REQUIRE(pg.wait_ready(kPort));

  pg.stop(PgInstance::StopMode::Immediate);
  BOOST_TEST(!pg.is_running());
}

// ── backup / restore ──────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(backup, * boost::unit_test::timeout(60)) {
  if (!pg_available()) { BOOST_TEST_MESSAGE("PostgreSQL not found; skipping"); return; }
  IoThreads threads{1};
  TmpDir    tmp;
  auto data_dir   = tmp() / "pgdata";
  auto backup_dir = tmp() / "backup";

  PgInstance pg(kPgBin, data_dir, kPort, PgInstance::Role::kPrimary);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_REQUIRE(pg.write_config({}));
  BOOST_REQUIRE(pg.write_hba({}));
  BOOST_REQUIRE(pg.start());
  BOOST_REQUIRE(pg.wait_ready(kPort));

  BOOST_TEST(pg.backup(backup_dir));
  BOOST_TEST(pg.is_running());
  BOOST_TEST(std::filesystem::exists(backup_dir / "postgresql.conf"));
  BOOST_TEST(std::filesystem::exists(backup_dir / "pg_wal"));
}

BOOST_AUTO_TEST_CASE(restore_and_start, * boost::unit_test::timeout(60)) {
  if (!pg_available()) { BOOST_TEST_MESSAGE("PostgreSQL not found; skipping"); return; }
  IoThreads threads{1};
  TmpDir    tmp;
  auto data_dir    = tmp() / "pgdata";
  auto backup_dir  = tmp() / "backup";
  auto restore_dir = tmp() / "restored";

  // Set up a primary, let it run briefly, then back it up.
  {
    PgInstance primary(kPgBin, data_dir, kPort, PgInstance::Role::kPrimary);
    BOOST_REQUIRE(primary.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
    BOOST_REQUIRE(primary.write_config({}));
    BOOST_REQUIRE(primary.write_hba({}));
    BOOST_REQUIRE(primary.start());
    BOOST_REQUIRE(primary.wait_ready(kPort));
    BOOST_REQUIRE(primary.backup(backup_dir));
  }

  // Restore the backup and start a new instance from it.
  PgInstance replica(kPgBin, restore_dir, kPort, PgInstance::Role::kPrimary);
  BOOST_REQUIRE(replica.restore_and_start(backup_dir));
  BOOST_TEST(replica.wait_ready(kPort));
  replica.stop();
  BOOST_TEST(!replica.is_running());
}
