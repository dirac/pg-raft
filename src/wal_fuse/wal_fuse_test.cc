#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <string>
#include <thread>

#include "src/process/pg_instance.h"
#include "src/process/process.h"
#include "src/wal_fuse/wal_fuse.h"

#define BOOST_TEST_MODULE WalFuseTest
#include <boost/test/included/unit_test.hpp>


// ── constants ─────────────────────────────────────────────────────────────────

static std::string const kPgBin = "/opt/pg_raft/postgresql/17/bin";
static int const         kPort  = 15436;

// ── helpers ───────────────────────────────────────────────────────────────────

static bool wait_for_pg(std::string const& host, int port, int timeout_s) {
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
  while (std::chrono::steady_clock::now() < deadline) {
    Process p(kPgBin + "/pg_isready",
              {"-h", host, "-p", std::to_string(port), "-d", "postgres", "-q"});
    p.start();
    if (p.wait() == 0)
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
  return false;
}

static void rmrf(std::string const& path) {
  std::string cmd = "rm -rf " + path;
  system(cmd.c_str());
}

// Count regular WAL segment files (non-hidden, non-directory) in a directory.
static int count_wal_segments(std::string const& dir) {
  DIR* dp = opendir(dir.c_str());
  if (!dp)
    return -1;
  int            n = 0;
  struct dirent* de;
  while ((de = readdir(dp)) != nullptr) {
    if (de->d_name[0] == '.')
      continue;
    if (de->d_type == DT_REG)
      ++n;
  }
  closedir(dp);
  return n;
}

static void run_pgbench() {
  // ── pgbench: initialize schema ────────────────────────────────────────────────
  {
    Process p(kPgBin + "/pgbench",
              {"-h", "127.0.0.1", "-p", std::to_string(kPort), "-i", "postgres"});
    BOOST_REQUIRE(p.start());
    if (p.wait() != 0) {
      BOOST_FAIL("pgbench -i failed");
    }
  }

  // ── pgbench: run transactions ─────────────────────────────────────────────────
  {
    Process p(kPgBin + "/pgbench",
              {"-h", "127.0.0.1", "-p", std::to_string(kPort), "-t", "200", "-c", "4", "postgres"});
    BOOST_REQUIRE(p.start());
    if (p.wait() != 0) {
      BOOST_FAIL("pgbench run failed");
    }
  }
}

// ── test ─────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(WalWritesToPgRaftWal) {
  if (access((kPgBin + "/postgres").c_str(), X_OK) != 0) {
    BOOST_TEST_MESSAGE("PostgreSQL not found at " + kPgBin + "; skipping");
    return;
  }

  // ── temp directory ───────────────────────────────────────────────────────────
  char  tmpl[] = "/tmp/wal_fuse_test_XXXXXX";
  char* base   = mkdtemp(tmpl);
  BOOST_REQUIRE(base != nullptr);
  std::string const data_dir = std::string(base) + "/pgdata";
  auto              cleanup  = [&]() { rmrf(base); };

  // ── initialize PostgreSQL ────────────────────────────────────────────────────
  PgInstance pg(
      kPgBin, data_dir, {"-p", std::to_string(kPort), "-c", "listen_addresses=127.0.0.1"});
  if (!pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"})) {
    cleanup();
    BOOST_FAIL("PgInstance::init() failed");
  }
  if (!pg.create_raft_wal()) {
    cleanup();
    BOOST_FAIL("PgInstance::create_raft_wal() failed");
  }

  pg.write_config({});
  pg.write_hba({"host  all  all  127.0.0.1/32  trust"});

  // ── verify initial WAL was moved to pg_raft_wal ──────────────────────────────
  std::string const pg_raft_wal = data_dir + "/pg_raft_wal";
  BOOST_REQUIRE_GT(count_wal_segments(pg_raft_wal), 0);

  // pg_wal itself must be empty (its contents were moved to pg_raft_wal).
  BOOST_REQUIRE_EQUAL(count_wal_segments(data_dir + "/pg_wal"), 0);

  WalFuse fuse{data_dir + "/pg_wal", std::make_unique<WalFuseOpBase>(pg_raft_wal)};
  if (!fuse.start()) {
    cleanup();
    BOOST_FAIL("WalFuse::start() failed");
  }

  if (!pg.start()) {
    cleanup();
    BOOST_FAIL("PgInstance::start() failed");
  }

  // ── pg_wal must be a live FUSE mount ─────────────────────────────────────────
  struct stat st_wal{}, st_data{};
  BOOST_REQUIRE_EQUAL(stat((data_dir + "/pg_wal").c_str(), &st_wal), 0);
  BOOST_REQUIRE_EQUAL(stat(data_dir.c_str(), &st_data), 0);
  BOOST_CHECK_NE(st_wal.st_dev, st_data.st_dev);

  // ── wait for PostgreSQL ──────────────────────────────────────────────────────
  if (!wait_for_pg("127.0.0.1", kPort, 30)) {
    pg.stop();
    fuse.stop();
    cleanup();
    BOOST_FAIL("PostgreSQL did not become ready");
  }

  // ── generate WAL ─────────────────────────────────────────────────────────────
  run_pgbench();

  // ── WAL files live in pg_raft_wal ────────────────────────────────────────────
  // After a checkpoint, at least one WAL segment must exist in pg_raft_wal.
  int backing_count = count_wal_segments(pg_raft_wal);
  BOOST_CHECK_GT(backing_count, 0);

  // pg_wal (via FUSE) must show the same set of files as the backing directory.
  BOOST_CHECK_EQUAL(count_wal_segments(data_dir + "/pg_wal"), backing_count);

  // ── teardown ─────────────────────────────────────────────────────────────────
  pg.stop();
  fuse.stop();
  cleanup();
}
