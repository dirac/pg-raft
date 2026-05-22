#include "src/wal_fuse/pg_initializer.h"
#include "src/wal_fuse/wal_fuse_manager.h"
#include "src/process/process.h"

#include <libpq-fe.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <string>
#include <thread>

#define BOOST_TEST_MODULE WalFuseTest
#include <boost/test/included/unit_test.hpp>

// ── constants ─────────────────────────────────────────────────────────────────

static const std::string kPgBin = "/opt/pg_raft/pgsql/v17/bin";
static const int kPort = 15435;

// ── helpers ───────────────────────────────────────────────────────────────────

static bool wait_for_pg(const std::string& host, int port, int timeout_s) {
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
  while (std::chrono::steady_clock::now() < deadline) {
    Process p(kPgBin + "/pg_isready",
              {"-h", host, "-p", std::to_string(port), "-d", "postgres", "-q"});
    p.start();
    if (p.wait() == 0) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
  return false;
}

static void append_conf(const std::string& path, const std::string& content) {
  std::ofstream f(path, std::ios::app);
  f << content;
}

static void rmrf(const std::string& path) {
  std::string cmd = "rm -rf " + path;
  system(cmd.c_str());
}

// Count regular WAL segment files (non-hidden, non-directory) in a directory.
static int count_wal_segments(const std::string& dir) {
  DIR* dp = opendir(dir.c_str());
  if (!dp) return -1;
  int n = 0;
  struct dirent* de;
  while ((de = readdir(dp)) != nullptr) {
    if (de->d_name[0] == '.') continue;
    if (de->d_type == DT_REG) ++n;
  }
  closedir(dp);
  return n;
}

// ── test ─────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(WalWritesToPgRaftWal) {
  if (access((kPgBin + "/postgres").c_str(), X_OK) != 0) {
    BOOST_TEST_MESSAGE("PostgreSQL not found at " + kPgBin + "; skipping");
    return;
  }

  // ── temp directory ───────────────────────────────────────────────────────────
  char tmpl[] = "/tmp/wal_fuse_test_XXXXXX";
  char* base = mkdtemp(tmpl);
  BOOST_REQUIRE(base != nullptr);
  const std::string data_dir = std::string(base) + "/pgdata";
  auto cleanup = [&]() { rmrf(base); };

  // ── initialize PostgreSQL ────────────────────────────────────────────────────
  PgInitializer init(kPgBin, data_dir,
                     {"-E", "UTF8", "-A", "trust", "--no-sync"});
  if (!init.run()) { cleanup(); BOOST_FAIL("PgInitializer::run() failed"); }

  // Allow TCP connections from localhost.
  append_conf(data_dir + "/pg_hba.conf",
              "host  all  all  127.0.0.1/32  trust\n");

  // ── verify initial WAL was moved to pg_raft_wal ──────────────────────────────
  const std::string pg_raft_wal = data_dir + "/pg_raft_wal";
  BOOST_REQUIRE_GT(count_wal_segments(pg_raft_wal), 0);

  // pg_wal itself must be empty (its contents were moved to pg_raft_wal).
  BOOST_REQUIRE_EQUAL(count_wal_segments(data_dir + "/pg_wal"), 0);

  // ── start WalFuseManager ─────────────────────────────────────────────────────
  WalFuseManager mgr(kPgBin + "/postgres", data_dir,
                     {"-p", std::to_string(kPort),
                      "-c", "listen_addresses=127.0.0.1"});
  if (!mgr.start()) { cleanup(); BOOST_FAIL("WalFuseManager::start() failed"); }

  // ── pg_wal must be a live FUSE mount ─────────────────────────────────────────
  struct stat st_wal{}, st_data{};
  BOOST_REQUIRE_EQUAL(stat((data_dir + "/pg_wal").c_str(), &st_wal), 0);
  BOOST_REQUIRE_EQUAL(stat(data_dir.c_str(), &st_data), 0);
  BOOST_CHECK_NE(st_wal.st_dev, st_data.st_dev);

  // ── wait for PostgreSQL ──────────────────────────────────────────────────────
  if (!wait_for_pg("127.0.0.1", kPort, 30)) {
    mgr.stop(); cleanup();
    BOOST_FAIL("PostgreSQL did not become ready");
  }

  // ── generate WAL ─────────────────────────────────────────────────────────────
  const std::string conn_str =
      "host=127.0.0.1 port=" + std::to_string(kPort) +
      " dbname=postgres user=" +
      std::string(getenv("USER") != nullptr ? getenv("USER") : "postgres");

  PGconn* conn = PQconnectdb(conn_str.c_str());
  BOOST_REQUIRE_EQUAL(PQstatus(conn), CONNECTION_OK);

  auto exec = [&](const char* sql) {
    PGresult* r = PQexec(conn, sql);
    ExecStatusType s = PQresultStatus(r);
    PQclear(r);
    return s == PGRES_COMMAND_OK || s == PGRES_TUPLES_OK;
  };
  BOOST_REQUIRE(exec("CREATE TABLE t (x int)"));
  BOOST_REQUIRE(exec("INSERT INTO t SELECT generate_series(1, 1000)"));
  BOOST_REQUIRE(exec("CHECKPOINT"));
  PQfinish(conn);

  // ── WAL files live in pg_raft_wal ────────────────────────────────────────────
  // After a checkpoint, at least one WAL segment must exist in pg_raft_wal.
  int backing_count = count_wal_segments(pg_raft_wal);
  BOOST_CHECK_GT(backing_count, 0);

  // pg_wal (via FUSE) must show the same set of files as the backing directory.
  BOOST_CHECK_EQUAL(count_wal_segments(data_dir + "/pg_wal"), backing_count);

  // ── teardown ─────────────────────────────────────────────────────────────────
  mgr.stop();
  cleanup();
}
