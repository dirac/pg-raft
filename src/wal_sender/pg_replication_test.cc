#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif
#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif
#include <fuse/fuse.h>

#include "src/postgres/pg_instance.h"
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
// All WAL written by the primary passes through the FUSE layer.  op_write
// calls capture_wal_write(), which validates WAL page headers and either
// sends bytes immediately (if a standby is connected) or buffers them.

struct WalCapture {
  std::mutex              mu;
  std::deque<WalSegment>  buffer;
  WalSender*              sender = nullptr;
  // Only capture WAL after the primary has been restarted.  Before restart the
  // standby has all WAL in its local pg_wal (from the backup); re-sending it
  // would delay delivery of the critical post-restart records.
  std::atomic<bool>       enabled{false};
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

// Called from the FUSE write hook with the exact bytes and file offset being
// written.  Validates WAL page headers (magic + xlp_pageaddr) and enqueues
// valid bytes for delivery to the standby.
//
// Using write rather than fsync means we work regardless of the WAL sync
// method (including macOS F_FULLFSYNC which FUSE does not intercept).
static void capture_wal_write(const char* fuse_path, const char* buf,
                               size_t size, off_t offset,
                               const char* bdir) {
  if (!g_capture.enabled.load(std::memory_order_relaxed)) return;
  static constexpr uint16_t kXlogMagic = 0xD116;
  static constexpr uint32_t kPageSize  = 8192;

  std::string backing_path = std::string(bdir) + fuse_path;
  uint64_t seg_lsn = lsn_from_backing_path(backing_path);

  // Walk the written bytes page by page.  Stop at the first page whose
  // magic is wrong (pre-zeroing) or whose xlp_pageaddr doesn't match the
  // expected segment LSN (recycled-segment stale content).
  size_t valid_bytes = 0;
  for (size_t off = 0; off < size; ) {
    uint32_t file_off = static_cast<uint32_t>(offset) + static_cast<uint32_t>(off);
    bool at_page_start = (file_off % kPageSize == 0);

    if (at_page_start) {
      if (off + 16 > size) break;

      uint16_t magic = static_cast<uint16_t>(
          static_cast<uint16_t>(static_cast<uint8_t>(buf[off])) |
          static_cast<uint16_t>(
              static_cast<uint16_t>(static_cast<uint8_t>(buf[off + 1])) << 8));
      if (magic != kXlogMagic) break;

      uint64_t pageaddr = 0;
      for (int b = 0; b < 8; ++b)
        pageaddr |= static_cast<uint64_t>(
                        static_cast<uint8_t>(buf[off + 8 + static_cast<size_t>(b)]))
                    << static_cast<unsigned>(b * 8);
      if (pageaddr != seg_lsn + static_cast<uint64_t>(file_off)) break;
    }

    uint32_t page_off          = file_off % kPageSize;
    uint32_t remaining_in_page = kPageSize - page_off;
    size_t   chunk = std::min(static_cast<size_t>(remaining_in_page), size - off);
    valid_bytes += chunk;
    off         += chunk;
  }

  std::cerr << "[capture_write] valid=" << valid_bytes
            << " seg_lsn=0x" << std::hex << seg_lsn
            << " off=0x" << offset << std::dec << "\n";
  if (valid_bytes == 0) return;

  uint64_t start_lsn = seg_lsn + static_cast<uint64_t>(offset);
  std::vector<uint8_t> data(reinterpret_cast<const uint8_t*>(buf),
                             reinterpret_cast<const uint8_t*>(buf) + valid_bytes);
  WalSegment seg{start_lsn, std::move(data)};

  std::lock_guard<std::mutex> lk(g_capture.mu);
  std::cerr << "[capture_write] queued lsn=0x" << std::hex << start_lsn
            << " bytes=" << std::dec << valid_bytes
            << " sender=" << (g_capture.sender ? "yes" : "no") << "\n";
  // Always buffer (append-only) so reconnecting standbys can get historical WAL.
  g_capture.buffer.push_back(seg);  // copy into buffer
  if (g_capture.sender)
    g_capture.sender->send(std::move(seg));  // also deliver live
}

// ── custom FUSE ops ───────────────────────────────────────────────────────────
//
// Identical to WalFuseOpBase for every op except write, which also captures
// the newly written WAL bytes.

class WalSendingOps {
 public:
  static struct fuse_operations make_ops() {
    struct fuse_operations ops = WalFuseOpBase::make_ops();
    ops.write = op_write;
    return ops;
  }

 private:
  static int op_write(const char* path, const char* buf, size_t size,
                      off_t offset, struct fuse_file_info* fi) {
    ssize_t res = pwrite(static_cast<int>(fi->fh), buf, size, offset);
    if (res == -1) return -errno;
    if (is_wal_segment(path)) {
      std::cerr << "[write] " << path << " off=" << offset
                << " size=" << size << "\n";
      const char* bdir =
          static_cast<const char*>(fuse_get_context()->private_data);
      capture_wal_write(path, buf, static_cast<size_t>(res), offset, bdir);
    }
    return static_cast<int>(res);
  }
};

// ── helpers ───────────────────────────────────────────────────────────────────

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

  PgInstance primary(kPgBin, primary_dir);
  primary.write_config({
      {"listen_addresses", "'127.0.0.1'"},
      {"port",             std::to_string(kPrimaryPort)},
      {"wal_level",        "replica"},
      {"max_wal_senders",  "3"},
      {"log_min_messages", "warning"},
      {"wal_recycle",      "off"},
  });
  primary.write_hba({
      "host  all         all  127.0.0.1/32  trust",
      "host  replication all  127.0.0.1/32  trust",
  });

  // ── mount FUSE over pg_wal with WAL-capturing ops ───────────────────────────
  WalFuse fuse(primary_dir + "/pg_wal", primary_dir + "/pg_raft_wal",
               WalSendingOps{});
  if (!fuse.start()) { cleanup(); BOOST_FAIL("WalFuse start failed"); }
  if (!wait_for_fuse_mount(primary_dir + "/pg_wal", primary_dir, 5000)) {
    fuse.stop(); cleanup(); BOOST_FAIL("FUSE mount timed out");
  }

  // ── start primary ───────────────────────────────────────────────────────────
  if (!primary.start()) {
    fuse.stop(); cleanup(); BOOST_FAIL("primary start() failed");
  }
  if (!primary.wait_ready("127.0.0.1", kPrimaryPort)) {
    primary.stop(); fuse.stop(); cleanup();
    BOOST_FAIL("primary did not become ready");
  }

  const std::string primary_conn =
      "host=127.0.0.1 port=" + std::to_string(kPrimaryPort) +
      " dbname=postgres user=" + std::string(getenv("USER") ?: "venkat");

  // ── cold backup: stop primary, copy data dir, restart primary ───────────────
  // A clean shutdown ensures pg_control has a valid checkpoint that the standby
  // can use as its starting LSN without needing WAL replay.
  const std::string backup_dir = base_dir + "/backup";
  if (!primary.backup(backup_dir)) {
    fuse.stop(); cleanup(); BOOST_FAIL("primary backup failed");
  }

  uint64_t system_id = read_system_id(primary_dir);
  BOOST_REQUIRE(system_id != 0);

  // ── restore backup as standby, then configure ────────────────────────────────
  PgInstance standby(kPgBin, standby_dir);
  if (!standby.restore(backup_dir)) {
    fuse.stop(); cleanup(); BOOST_FAIL("standby restore failed");
  }
  std::ofstream(standby_dir + "/standby.signal").close();
  standby.write_config({
      {"listen_addresses", "'127.0.0.1'"},
      {"port",             std::to_string(kStandbyPort)},
      {"primary_conninfo", "'host=127.0.0.1 port=" +
                           std::to_string(kWalSenderPort) +
                           " application_name=standby'"},
      {"hot_standby",      "on"},
      {"log_min_messages", "debug1"},
  });

  // ── restart primary ──────────────────────────────────────────────────────────
  // Enable WAL capture now.  The standby already has all pre-restart WAL in its
  // local pg_wal (from the backup); only post-restart records need streaming.
  g_capture.enabled = true;

  if (!primary.start()) {
    fuse.stop(); cleanup(); BOOST_FAIL("primary restart failed");
  }
  if (!primary.wait_ready("127.0.0.1", kPrimaryPort)) {
    primary.stop(); fuse.stop(); cleanup();
    BOOST_FAIL("primary did not become ready after restart");
  }

  // After a clean-shutdown restart postgres writes no WAL until the first
  // transaction.  Force a checkpoint so the FUSE buffer gets a valid WAL
  // record at the position immediately following the backup's shutdown
  // checkpoint — without it the standby can't advance past that point.
  pg_query_str(primary_conn, "CHECKPOINT");

  // ── start WalSenderServer ───────────────────────────────────────────────────
  //
  // When the standby sends START_REPLICATION(start_lsn):
  //   1. Drain any WAL captured before the standby connected, trimming
  //      segments that precede start_lsn.
  //   2. Register the sender so future write callbacks deliver WAL directly.
  // Query the primary's current flush LSN so IDENTIFY_SYSTEM returns a
  // realistic walEnd rather than 0/0 (which confuses some standby versions).
  uint64_t primary_flush_lsn = parse_lsn(
      pg_query_str(primary_conn, "SELECT pg_current_wal_flush_lsn()"));
  std::cerr << "[setup] primary_flush_lsn=0x" << std::hex << primary_flush_lsn
            << std::dec << "\n";

  WalSenderServer wal_server(
      kWalSenderPort, system_id, /*timeline=*/1, primary_flush_lsn,
      [](WalSender& sender) {
        std::cerr << "[connection] new WAL sender connection\n";
        sender.set_replication_start_cb([&sender](uint64_t start_lsn) {
          std::lock_guard<std::mutex> lk(g_capture.mu);
          // Iterate buffer by const-ref: copy-send each relevant segment.
          // Buffer is kept intact for reconnection resilience.
          for (const auto& seg : g_capture.buffer) {
            uint64_t seg_end =
                seg.start_lsn + static_cast<uint64_t>(seg.data.size());
            if (seg_end <= start_lsn) continue;
            WalSegment to_send = seg;  // copy
            if (to_send.start_lsn < start_lsn) {
              size_t trim =
                  static_cast<size_t>(start_lsn - to_send.start_lsn);
              to_send.data.erase(to_send.data.begin(),
                                 to_send.data.begin() +
                                     static_cast<ptrdiff_t>(trim));
              to_send.start_lsn = start_lsn;
            }
            sender.send(std::move(to_send));
          }
          // Do NOT clear g_capture.buffer: keep it for reconnections.
          g_capture.sender = &sender;
          std::cerr << "[start_cb] sender registered, start_lsn="
                    << std::hex << start_lsn << std::dec << "\n";
        });

        sender.run();

        std::lock_guard<std::mutex> lk(g_capture.mu);
        g_capture.sender = nullptr;
      });

  BOOST_REQUIRE(wal_server.start());

  // ── start standby ───────────────────────────────────────────────────────────
  if (!standby.start()) {
    wal_server.stop(); primary.stop(); fuse.stop(); cleanup();
    BOOST_FAIL("standby start() failed");
  }
  if (!standby.wait_ready("127.0.0.1", kStandbyPort)) {
    standby.stop(); wal_server.stop(); primary.stop(); fuse.stop(); cleanup();
    BOOST_FAIL("standby did not become ready");
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

  std::cerr << "[check] target_lsn=0x" << std::hex << target_lsn << std::dec << "\n";
  bool caught_up = false;
  auto deadline  = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (std::chrono::steady_clock::now() < deadline) {
    uint64_t sb_lsn = parse_lsn(pg_query_str(
        standby_conn, "SELECT COALESCE(pg_last_wal_replay_lsn(), '0/0')"));
    std::string rcv_status = pg_query_str(standby_conn,
        "SELECT status||' '||received_lsn||' '||last_msg_receipt_time"
        " FROM pg_stat_wal_receiver");
    std::cerr << "[check] sb_replay=0x" << std::hex << sb_lsn << std::dec
              << " walrcv=" << rcv_status << "\n";
    if (sb_lsn >= target_lsn) { caught_up = true; break; }
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
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
