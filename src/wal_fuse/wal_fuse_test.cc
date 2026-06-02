#define BOOST_TEST_MODULE WalFuseTest

#include "src/base/io_threads.h"
#include "src/base/testing.h"
#include "src/process/pg_instance.h"
#include "src/process/process.h"
#include "src/wal_fuse/wal_fuse.h"

static std::string const  kPgBin = "/opt/pg_raft/postgresql/17/bin";
static constexpr uint16_t kPort  = 15436;

static bool pg_available() {
  return access((kPgBin + "/postgres").c_str(), X_OK) == 0;
}

static int count_wal_segments(std::filesystem::path const& dir) {
  DIR*           dp = opendir(dir.c_str());
  if (!dp) return -1;
  int            n  = 0;
  struct dirent* de;
  while ((de = readdir(dp)) != nullptr)
    if (de->d_name[0] != '.' && de->d_type == DT_REG)
      ++n;
  closedir(dp);
  return n;
}

static void run_pgbench() {
  Process init(kPgBin + "/pgbench",
               {"-h", "127.0.0.1", "-p", std::to_string(kPort), "-i", "postgres"});
  BOOST_REQUIRE(init.start());
  BOOST_REQUIRE_EQUAL(init.wait(), 0);

  Process bench(kPgBin + "/pgbench",
                {"-h", "127.0.0.1", "-p", std::to_string(kPort), "-t", "200", "-c", "4", "postgres"});
  BOOST_REQUIRE(bench.start());
  BOOST_REQUIRE_EQUAL(bench.wait(), 0);
}

// ── tests ─────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(WalMovedToRaftDir) {
  if (!pg_available()) { BOOST_TEST_MESSAGE("PostgreSQL not found; skipping"); return; }
  IoThreads threads{1};
  TmpDir    tmp;
  auto data_dir = tmp() / "pgdata";
  auto raft_dir = tmp() / "pgraft";
  std::filesystem::create_directory(raft_dir);

  PgInstance pg(kPgBin, data_dir, raft_dir);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));

  int initial = count_wal_segments(data_dir / "pg_wal");
  BOOST_REQUIRE_GT(initial, 0);

  BOOST_TEST(pg.create_raft_wal());
  BOOST_TEST(count_wal_segments(raft_dir / "pg_wal") == initial);
  BOOST_TEST(count_wal_segments(data_dir / "pg_wal") == 0);
}

BOOST_AUTO_TEST_CASE(WalWritesToBackingDir) {
  if (!pg_available()) { BOOST_TEST_MESSAGE("PostgreSQL not found; skipping"); return; }
  IoThreads threads{1};
  TmpDir    tmp;
  auto data_dir    = tmp() / "pgdata";
  auto raft_dir    = tmp() / "pgraft";
  auto raft_pg_wal = raft_dir / "pg_wal";
  std::filesystem::create_directory(raft_dir);

  // fuse declared before pg so pg destructs first (stops postgres before FUSE unmounts).
  WalFuse    fuse(data_dir / "pg_wal", std::make_unique<WalFuseOps>(raft_pg_wal));
  PgInstance pg(kPgBin, data_dir, raft_dir, {"-p", std::to_string(kPort)});

  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_REQUIRE(pg.create_raft_wal());
  BOOST_REQUIRE(pg.write_config({{"listen_addresses", "'127.0.0.1'"}}));
  BOOST_REQUIRE(pg.write_hba({"host  all  all  127.0.0.1/32  trust"}));

  BOOST_REQUIRE_GT(count_wal_segments(raft_pg_wal), 0);
  BOOST_REQUIRE_EQUAL(count_wal_segments(data_dir / "pg_wal"), 0);

  BOOST_REQUIRE(fuse.start());
  BOOST_REQUIRE(pg.start());
  BOOST_REQUIRE(pg.wait_ready(kPort));

  // pg_wal's st_dev must differ from its parent when the FUSE mount is live.
  struct stat st_wal{}, st_data{};
  BOOST_REQUIRE_EQUAL(stat((data_dir / "pg_wal").c_str(), &st_wal), 0);
  BOOST_REQUIRE_EQUAL(stat(data_dir.c_str(), &st_data), 0);
  BOOST_CHECK_NE(st_wal.st_dev, st_data.st_dev);

  run_pgbench();

  int backing_count = count_wal_segments(raft_pg_wal);
  BOOST_CHECK_GT(backing_count, 0);
  // The FUSE view of pg_wal must show the same files as the backing directory.
  BOOST_CHECK_EQUAL(count_wal_segments(data_dir / "pg_wal"), backing_count);
}
