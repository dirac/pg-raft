#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif
#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif
#include <fuse/fuse.h>

#include "src/wal_fuse/pg_initializer.h"
#include "src/wal_fuse/wal_fuse.h"
#include "src/wal_fuse/wal_fuse_ops.h"
#include "src/wal_sender/wal_sender_server.h"
#include "src/process/process.h"

#include <libpq-fe.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <deque>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <thread>

#define BOOST_TEST_MODULE PgReplicationTest
#include <boost/test/included/unit_test.hpp>

// ── constants ─────────────────────────────────────────────────────────────────

static const std::string kPgBin        = "/opt/pg_raft/pgsql/v17/bin";
static const int         kPrimaryPort  = 15432;
static const int         kStandbyPort  = 15433;
static const int         kWalSenderPort = 15434;
static const uint32_t    kWalSegSize   = 16u * 1024u * 1024u;

// ── WAL capture state ─────────────────────────────────────────────────────────
//
// All WAL written by the primary passes through the FUSE layer.  op_fsync
// calls capture_wal(), which reads bytes newly written since the last fsync
// and either sends them immediately (if a standby is connected) or buffers
// them until one connects.

struct WalCapture {
  std::mutex              mu;
  std::deque<WalSegment>  buffer;   // segments buffered before standby connects
  WalSender*              sender = nullptr;
  std::map<std::string, off_t> file_pos;  // backing path → bytes sent so far
};
static WalCapture g_capture;

// Derive the start LSN of a WAL segment from its backing-directory path.
// Filename format: TLI(8hex) HIGH(8hex) LOW(8hex)
// start_lsn = (HIGH << 32) | (LOW * kWalSegSize)
static uint64_t lsn_from_backing_path(const std::string& path) {
  const char* p = path.c_str();
  const char* slash = strrchr(p, '/');
  const char* name  = slash ? slash + 1 : p;
  if (strlen(name) != 24) return 0;
  uint32_t hi = 0, lo = 0;
  sscanf(name + 8, "%8X%8X", &hi, &lo);
  return (static_cast<uint64_t>(hi) << 32) |
         (static_cast<uint64_t>(lo) * kWalSegSize);
}

// Returns true for the 24-hex-char WAL segment filename (ignores archive_status
// entries, .partial files, etc.).
static bool is_wal_segment(const char* fuse_path) {
  const char* slash = strrchr(fuse_path, '/');
  const char* name  = slash ? slash + 1 : fuse_path;
  if (strlen(name) != 24) return false;
  for (int i = 0; i < 24; ++i)
    if (!isxdigit(static_cast<unsigned char>(name[i]))) return false;
  return true;
}

// Read bytes written to |backing_path| since the last capture and deliver them
// as a WalSegment.  Holds g_capture.mu for the duration so it is safe to call
// from concurrent FUSE threads.
static void capture_wal(const std::string& backing_path) {
  std::lock_guard<std::mutex> lk(g_capture.mu);

  off_t& pos = g_capture.file_pos[backing_path];

  int fd = open(backing_path.c_str(), O_RDONLY);
  if (fd < 0) return;

  off_t end = lseek(fd, 0, SEEK_END);
  if (end <= pos) { close(fd); return; }

  size_t sz = static_cast<size_t>(end - pos);
  std::vector<uint8_t> data(sz);
  ssize_t n = pread(fd, data.data(), sz, pos);
  close(fd);
  if (n <= 0) return;
  data.resize(static_cast<size_t>(n));

  uint64_t seg_lsn = lsn_from_backing_path(backing_path);
  WalSegment seg{seg_lsn + static_cast<uint64_t>(pos), std::move(data)};
  pos += static_cast<off_t>(n);

  if (g_capture.sender)
    g_capture.sender->send(std::move(seg));
  else
    g_capture.buffer.push_back(std::move(seg));
}

// ── custom FUSE ops ───────────────────────────────────────────────────────────
//
// Identical to WalFuseOpBase for every op except fsync, which also captures
// the newly written WAL bytes.

class WalSendingOps {
 public:
  static struct fuse_operations make_ops() {
    struct fuse_operations ops = WalFuseOpBase::make_ops();
    ops.fsync = op_fsync;
    return ops;
  }

 private:
  static int op_fsync(const char* path, int datasync,
                      struct fuse_file_info* fi) {
    (void)datasync;
    if (fsync(static_cast<int>(fi->fh)) == -1) return -errno;
    if (is_wal_segment(path)) {
      const char* bdir =
          static_cast<const char*>(fuse_get_context()->private_data);
      capture_wal(std::string(bdir) + path);
    }
    return 0;
  }
};

// ── helpers ───────────────────────────────────────────────────────────────────

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

static bool wait_for_fuse_mount(const std::string& mount_point,
                                const std::string& parent, int timeout_ms) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    struct stat st_m{}, st_p{};
    if (stat(mount_point.c_str(), &st_m) == 0 &&
        stat(parent.c_str(), &st_p) == 0 &&
        st_m.st_dev != st_p.st_dev)
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return false;
}

static void append_conf(const std::string& path, const std::string& content) {
  std::ofstream f(path, std::ios::app);
  f << content;
}

static uint64_t read_system_id(const std::string& data_dir) {
  std::string cmd =
      kPgBin + "/pg_controldata -D " + data_dir + " 2>/dev/null";
  FILE* f = popen(cmd.c_str(), "r");
  if (!f) return 0;
  char line[512];
  uint64_t id = 0;
  while (fgets(line, sizeof(line), f))
    if (sscanf(line, "Database system identifier: %llu",
               reinterpret_cast<unsigned long long*>(&id)) == 1)
      break;
  pclose(f);
  return id;
}

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

static uint64_t parse_lsn(const std::string& s) {
  uint32_t hi = 0, lo = 0;
  sscanf(s.c_str(), "%X/%X", &hi, &lo);
  return (static_cast<uint64_t>(hi) << 32) | lo;
}

static void rmrf(const std::string& path) {
  std::string cmd = "rm -rf " + path;
  system(cmd.c_str());
}

// ── test ─────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(PrimaryStandbyWithWalFuse) {
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
  auto cleanup = [&]() { rmrf(base_dir); };

  // ── initialize primary (initdb + create pg_raft_wal) ────────────────────────
  PgInitializer init(kPgBin, primary_dir,
                     {"-E", "UTF8", "-A", "trust", "--no-sync"});
  if (!init.run()) { cleanup(); BOOST_FAIL("PgInitializer failed"); }

  append_conf(primary_dir + "/postgresql.conf",
              "\nlisten_addresses = '127.0.0.1'\n"
              "port = " + std::to_string(kPrimaryPort) + "\n"
              "wal_level = replica\n"
              "max_wal_senders = 3\n"
              "log_min_messages = warning\n");
  append_conf(primary_dir + "/pg_hba.conf",
              "host  all         all  127.0.0.1/32  trust\n"
              "host  replication all  127.0.0.1/32  trust\n");

  // ── mount FUSE over pg_wal with WAL-capturing ops ───────────────────────────
  WalFuse fuse(primary_dir + "/pg_wal", primary_dir + "/pg_raft_wal",
               WalSendingOps{});
  if (!fuse.start()) { cleanup(); BOOST_FAIL("WalFuse start failed"); }
  if (!wait_for_fuse_mount(primary_dir + "/pg_wal", primary_dir, 5000)) {
    fuse.stop(); cleanup(); BOOST_FAIL("FUSE mount timed out");
  }

  // ── start primary ───────────────────────────────────────────────────────────
  Process primary(kPgBin + "/postgres", {"-D", primary_dir});
  BOOST_REQUIRE(primary.start());
  BOOST_TEST_MESSAGE(primary.to_string());

  if (!wait_for_pg("127.0.0.1", kPrimaryPort, 30)) {
    primary.stop(); fuse.stop(); cleanup();
    BOOST_FAIL("primary did not start");
  }

  const std::string primary_conn =
      "host=127.0.0.1 port=" + std::to_string(kPrimaryPort) +
      " dbname=postgres user=" + std::string(getenv("USER") ?: "venkat");

  // ── pg_basebackup to seed the standby ───────────────────────────────────────
  {
    Process p(kPgBin + "/pg_basebackup",
              {"-h", "127.0.0.1", "-p", std::to_string(kPrimaryPort),
               "-D", standby_dir, "-P", "-X", "none"});
    BOOST_REQUIRE(p.start());
    BOOST_TEST_MESSAGE(p.to_string());
    if (p.wait() != 0) {
      primary.stop(); fuse.stop(); cleanup();
      BOOST_FAIL("pg_basebackup failed");
    }
  }

  uint64_t system_id = read_system_id(primary_dir);
  BOOST_REQUIRE(system_id != 0);

  // ── configure standby to replicate from WalSenderServer ─────────────────────
  std::ofstream(standby_dir + "/standby.signal").close();
  append_conf(standby_dir + "/postgresql.conf",
              "\nlisten_addresses = '127.0.0.1'\n"
              "port = " + std::to_string(kStandbyPort) + "\n"
              "primary_conninfo = 'host=127.0.0.1 port=" +
              std::to_string(kWalSenderPort) + " application_name=standby'\n"
              "hot_standby = on\n"
              "log_min_messages = warning\n");

  // ── start WalSenderServer ───────────────────────────────────────────────────
  //
  // When the standby sends START_REPLICATION(start_lsn):
  //   1. Drain any WAL captured before the standby connected, trimming
  //      segments that precede start_lsn.
  //   2. Register the sender so future fsync callbacks deliver WAL directly.
  WalSenderServer wal_server(
      kWalSenderPort, system_id, /*timeline=*/1, /*end_lsn=*/0,
      [](WalSender& sender) {
        sender.set_replication_start_cb([&sender](uint64_t start_lsn) {
          std::lock_guard<std::mutex> lk(g_capture.mu);
          for (auto& seg : g_capture.buffer) {
            uint64_t seg_end =
                seg.start_lsn + static_cast<uint64_t>(seg.data.size());
            if (seg_end <= start_lsn) continue;
            if (seg.start_lsn < start_lsn) {
              size_t trim =
                  static_cast<size_t>(start_lsn - seg.start_lsn);
              seg.data.erase(seg.data.begin(),
                             seg.data.begin() + static_cast<ptrdiff_t>(trim));
              seg.start_lsn = start_lsn;
            }
            sender.send(std::move(seg));
          }
          g_capture.buffer.clear();
          g_capture.sender = &sender;
        });

        sender.run();

        std::lock_guard<std::mutex> lk(g_capture.mu);
        g_capture.sender = nullptr;
      });

  BOOST_REQUIRE(wal_server.start());

  // ── start standby ───────────────────────────────────────────────────────────
  Process standby(kPgBin + "/postgres", {"-D", standby_dir});
  BOOST_REQUIRE(standby.start());
  BOOST_TEST_MESSAGE(standby.to_string());

  if (!wait_for_pg("127.0.0.1", kStandbyPort, 30)) {
    standby.stop(); wal_server.stop(); primary.stop(); fuse.stop(); cleanup();
    BOOST_FAIL("standby did not start");
  }

  const std::string standby_conn =
      "host=127.0.0.1 port=" + std::to_string(kStandbyPort) +
      " dbname=postgres user=" + std::string(getenv("USER") ?: "venkat");

  // ── pgbench: initialize schema ────────────────────────────────────────────────
  {
    Process p(kPgBin + "/pgbench",
              {"-h", "127.0.0.1", "-p", std::to_string(kPrimaryPort),
               "-i", "postgres"});
    BOOST_REQUIRE(p.start());
    if (p.wait() != 0) {
      standby.stop(); wal_server.stop(); primary.stop(); fuse.stop(); cleanup();
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
      standby.stop(); wal_server.stop(); primary.stop(); fuse.stop(); cleanup();
      BOOST_FAIL("pgbench run failed");
    }
  }

  // ── wait for standby to catch up ──────────────────────────────────────────────
  uint64_t target_lsn =
      parse_lsn(pg_query_str(primary_conn, "SELECT pg_current_wal_flush_lsn()"));

  bool caught_up = false;
  auto deadline  = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (std::chrono::steady_clock::now() < deadline) {
    uint64_t sb_lsn = parse_lsn(pg_query_str(
        standby_conn, "SELECT COALESCE(pg_last_wal_replay_lsn(), '0/0')"));
    if (sb_lsn >= target_lsn) { caught_up = true; break; }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  BOOST_CHECK(caught_up);

  // ── verify row counts match ───────────────────────────────────────────────────
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
  fuse.stop();
  cleanup();
}
