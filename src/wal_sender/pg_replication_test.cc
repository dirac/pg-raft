#include "src/wal_sender/wal_file_reader.h"
#include "src/wal_sender/wal_sender_server.h"
#include "src/process/process.h"

#include <libpq-fe.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>

#define BOOST_TEST_MODULE PgReplicationTest
#include <boost/test/included/unit_test.hpp>

// ── constants ─────────────────────────────────────────────────────────────────

static const std::string kPgBin  = "/opt/pg_raft/pgsql/v17/bin";
static const int kPrimaryPort    = 15432;
static const int kStandbyPort    = 15433;
static const int kWalSenderPort  = 15434;

// ── helpers ───────────────────────────────────────────────────────────────────

// Run pg_isready until success or timeout.
static bool wait_for_pg(const std::string& host, int port, int timeout_s) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::seconds(timeout_s);
  while (std::chrono::steady_clock::now() < deadline) {
    Process p(kPgBin + "/pg_isready",
              {"-h", host, "-p", std::to_string(port), "-d", "postgres", "-q"});
    p.start();
    if (p.wait() == 0) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
  return false;
}

// Append lines to a file.
static void append_conf(const std::string& path,
                        const std::string& content) {
  std::ofstream f(path, std::ios::app);
  f << content;
}

// Parse the database system identifier from pg_controldata output.
static uint64_t read_system_id(const std::string& data_dir) {
  std::string cmd =
      kPgBin + "/pg_controldata -D " + data_dir + " 2>/dev/null";
  FILE* f = popen(cmd.c_str(), "r");
  if (!f) return 0;
  char line[512];
  uint64_t id = 0;
  while (fgets(line, sizeof(line), f)) {
    if (sscanf(line, "Database system identifier: %llu",
               reinterpret_cast<unsigned long long*>(&id)) == 1) break;
  }
  pclose(f);
  return id;
}

// Execute a single-value SQL query via libpq and return the text result.
static std::string pg_query_str(const std::string& connstr,
                                const std::string& sql) {
  PGconn* conn = PQconnectdb(connstr.c_str());
  if (PQstatus(conn) != CONNECTION_OK) { PQfinish(conn); return ""; }
  PGresult* res = PQexec(conn, sql.c_str());
  std::string val;
  if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0)
    val = PQgetvalue(res, 0, 0);
  PQclear(res);
  PQfinish(conn);
  return val;
}

// Parse "X/Y" hex LSN string into uint64.
static uint64_t parse_lsn(const std::string& s) {
  uint32_t hi = 0, lo = 0;
  sscanf(s.c_str(), "%X/%X", &hi, &lo);
  return (static_cast<uint64_t>(hi) << 32) | lo;
}

// Recursively remove a directory tree.
static void rmrf(const std::string& path) {
  std::string cmd = "rm -rf " + path;
  system(cmd.c_str());
}

// ── test ─────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(PrimaryStandbyWithPgbench) {
  // Skip if PostgreSQL binaries are absent.
  if (access((kPgBin + "/postgres").c_str(), X_OK) != 0) {
    BOOST_TEST_MESSAGE("PostgreSQL not found at " + kPgBin + "; skipping");
    return;
  }

  // ── temp directories ────────────────────────────────────────────────────────
  char tmpl[] = "/tmp/pg_raft_test_XXXXXX";
  char* base  = mkdtemp(tmpl);
  BOOST_REQUIRE(base != nullptr);

  const std::string base_dir    = base;
  const std::string primary_dir = base_dir + "/primary";
  const std::string standby_dir = base_dir + "/standby";

  // Ensure cleanup even on test failure.
  auto cleanup = [&]() { rmrf(base_dir); };

  // ── initdb primary ──────────────────────────────────────────────────────────
  {
    Process p(kPgBin + "/initdb", {"-D", primary_dir, "-E", "UTF8", "-A", "trust"});
    BOOST_REQUIRE(p.start());
    if (p.wait() != 0) { cleanup(); BOOST_FAIL("initdb failed"); }
  }

  // ── configure primary ───────────────────────────────────────────────────────
  append_conf(primary_dir + "/postgresql.conf",
              "\nlisten_addresses = '127.0.0.1'\n"
              "port = " + std::to_string(kPrimaryPort) + "\n"
              "wal_level = replica\n"
              "max_wal_senders = 3\n"
              "wal_keep_size = 128\n"   // MB – keeps segments for file reader
              "log_min_messages = warning\n");

  append_conf(primary_dir + "/pg_hba.conf",
              "host  all         all  127.0.0.1/32  trust\n"
              "host  replication all  127.0.0.1/32  trust\n");

  // ── start primary ───────────────────────────────────────────────────────────
  Process primary(kPgBin + "/postgres", {"-D", primary_dir});
  BOOST_REQUIRE(primary.start());
  BOOST_TEST_MESSAGE(primary.to_string());
    


  if (!wait_for_pg("127.0.0.1", kPrimaryPort, 30)) {
    primary.stop();
    cleanup();
    BOOST_FAIL("primary did not start");
  }

  const std::string primary_conn =
      "host=127.0.0.1 port=" + std::to_string(kPrimaryPort) +
      " dbname=postgres user=" + std::string(getenv("USER") ?: "venkat");

  // ── pg_basebackup to set up standby ─────────────────────────────────────────
  {
    Process p(kPgBin + "/pg_basebackup",
              {"-h", "127.0.0.1", "-p", std::to_string(kPrimaryPort),
               "-D", standby_dir, "-P", "-X", "none"});
    BOOST_REQUIRE(p.start());
    BOOST_TEST_MESSAGE(p.to_string());
    if (p.wait() != 0) {
      primary.stop(); cleanup();
      BOOST_FAIL("pg_basebackup failed");
    }
  }

  // ── read primary system_id and current WAL position ─────────────────────────
  uint64_t system_id = read_system_id(primary_dir);
  BOOST_REQUIRE(system_id != 0);

  // ── configure standby ───────────────────────────────────────────────────────
  // standby.signal enables standby mode (PG 12+).
  std::ofstream(standby_dir + "/standby.signal").close();

  append_conf(standby_dir + "/postgresql.conf",
              "\nlisten_addresses = '127.0.0.1'\n"
              "port = " + std::to_string(kStandbyPort) + "\n"
              "primary_conninfo = 'host=127.0.0.1 port=" +
              std::to_string(kWalSenderPort) + " application_name=standby'\n"
              "hot_standby = on\n"
              "log_min_messages = warning\n");

  // ── start WalSenderServer ───────────────────────────────────────────────────
  const std::string pg_wal_dir = primary_dir + "/pg_wal";

  // Shared mutable state for the reader thread.
  std::mutex reader_mu;
  WalFileReader* active_reader = nullptr;

  WalSenderServer wal_server(
      kWalSenderPort, system_id, /*timeline=*/1, /*end_lsn=*/0,
      [&](WalSender& sender) {
        auto reader = std::make_unique<WalFileReader>(pg_wal_dir, /*timeline=*/1);
        {
          std::lock_guard<std::mutex> lk(reader_mu);
          active_reader = reader.get();
        }

        std::thread reader_thread;
        sender.set_replication_start_cb([&](uint64_t lsn) {
          reader_thread = std::thread([r = reader.get(), lsn, &sender]() {
            r->run(lsn, [&sender](WalSegment seg) {
              sender.send(std::move(seg));
            });
          });
        });

        sender.run();

        // Stop the reader first, then wait for its thread to exit.
        reader->stop();
        if (reader_thread.joinable()) reader_thread.join();

        std::lock_guard<std::mutex> lk(reader_mu);
        active_reader = nullptr;
      });

  BOOST_REQUIRE(wal_server.start());

  // ── start standby ───────────────────────────────────────────────────────────
  Process standby(kPgBin + "/postgres", {"-D", standby_dir});
  BOOST_REQUIRE(standby.start());
  BOOST_TEST_MESSAGE(standby.to_string());

  if (!wait_for_pg("127.0.0.1", kStandbyPort, 30)) {
    standby.stop();
    wal_server.stop();
    primary.stop();
    cleanup();
    BOOST_FAIL("standby did not start");
  }

  const std::string standby_conn =
      "host=127.0.0.1 port=" + std::to_string(kStandbyPort) +
      " dbname=postgres user=" + std::string(getenv("USER") ?: "venkat");

  // ── pgbench: initialize schema on primary ────────────────────────────────────
  {
    Process p(kPgBin + "/pgbench",
              {"-h", "127.0.0.1", "-p", std::to_string(kPrimaryPort),
               "-i", "postgres"});
    BOOST_REQUIRE(p.start());
    if (p.wait() != 0) {
      standby.stop(); wal_server.stop(); primary.stop(); cleanup();
      BOOST_FAIL("pgbench -i failed");
    }
  }

  // ── pgbench: run transactions ─────────────────────────────────────────────────
  {
    Process p(kPgBin + "/pgbench",
              {"-h", "127.0.0.1", "-p", std::to_string(kPrimaryPort),
               "-t", "200", "-c", "4", "postgres"});
    BOOST_REQUIRE(p.start());
    if (p.wait() != 0) {
      standby.stop(); wal_server.stop(); primary.stop(); cleanup();
      BOOST_FAIL("pgbench run failed");
    }
  }

  // ── wait for standby to catch up ──────────────────────────────────────────────
  std::string primary_lsn_str =
      pg_query_str(primary_conn, "SELECT pg_current_wal_flush_lsn()");
  uint64_t target_lsn = parse_lsn(primary_lsn_str);

  bool caught_up = false;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (std::chrono::steady_clock::now() < deadline) {
    std::string sb_lsn_str =
        pg_query_str(standby_conn,
                     "SELECT COALESCE(pg_last_wal_replay_lsn(),'0/0')");
    uint64_t sb_lsn = parse_lsn(sb_lsn_str);
    if (sb_lsn >= target_lsn) { caught_up = true; break; }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  BOOST_CHECK(caught_up);

  // ── verify data matches ───────────────────────────────────────────────────────
  std::string primary_count =
      pg_query_str(primary_conn, "SELECT COUNT(*) FROM pgbench_accounts");
  std::string standby_count =
      pg_query_str(standby_conn, "SELECT COUNT(*) FROM pgbench_accounts");

  BOOST_CHECK(!primary_count.empty());
  BOOST_CHECK_EQUAL(primary_count, standby_count);

  // ── teardown ─────────────────────────────────────────────────────────────────
  standby.stop();
  wal_server.stop();
  primary.stop();
  cleanup();
}
