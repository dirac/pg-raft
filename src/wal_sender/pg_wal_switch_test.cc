#define BOOST_TEST_MODULE pg_wal_switch

#include "src/base/io_threads.h"
#include "src/base/testing.h"
#include "src/process/pg_instance.h"
#include "src/status/pg_client.h"
#include "src/wal_sender/wal_sender_server.h"

static constexpr char const* kPgBin       = "/opt/pg_raft/postgresql/17/bin";
static constexpr uint16_t    kPrimaryPort = 15448;
static constexpr uint16_t    kStandbyPort = 15449;

static bool pg_available() {
  return access((string(kPgBin) + "/postgres").c_str(), X_OK) == 0;
}

// ── helpers ───────────────────────────────────────────────────────────────────

static Result<Rows> exec_sync(shared_ptr<PgClient> conn, PgStmt const& stmt) {
  std::promise<Result<Rows>> p;
  conn->async_exec(stmt, [&p](Result<Rows> r) { p.set_value(std::move(r)); });
  return p.get_future().get();
}

static string exec_one(shared_ptr<PgClient> conn, char const* sql) {
  auto stmt = conn->prepare(sql);
  if (!stmt) return "";
  auto rows = exec_sync(conn, *stmt);
  if (!rows || rows->empty() || rows->at(0).empty() || !rows->at(0)[0]) return "";
  return *rows->at(0)[0];
}

static uint64_t parse_lsn(string const& s) {
  uint32_t hi = 0, lo = 0;
  sscanf(s.c_str(), "%X/%X", &hi, &lo);
  return (static_cast<uint64_t>(hi) << 32) | lo;
}

static uint64_t read_system_id(string const& data_dir) {
  string cmd = string(kPgBin) + "/pg_controldata -D " + data_dir + " 2>/dev/null";
  FILE*    f  = popen(cmd.c_str(), "r");
  if (!f) return 0;
  char     line[512];
  uint64_t id = 0;
  while (fgets(line, sizeof(line), f))
    if (sscanf(line, "Database system identifier: %llu",
               reinterpret_cast<unsigned long long*>(&id)) == 1)
      break;
  pclose(f);
  return id;
}

// ── test ──────────────────────────────────────────────────────────────────────

// Runs pgbench transactions on the primary while periodically calling
// pg_switch_wal() to force WAL segment boundaries.  Verifies that the standby
// replicated through WalSenderServer still catches up correctly.
BOOST_AUTO_TEST_CASE(wal_switch_during_replication) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not available at " + string(kPgBin) + "; skipping");
    return;
  }

  IoThreads io{4};
  TmpDir    tmp{};

  auto primary_dir = tmp() / "primary";
  auto standby_dir = tmp() / "standby";
  auto backup_dir  = tmp() / "backup";

  // ── init and start primary ────────────────────────────────────────────────
  PgInstance primary(string(kPgBin), primary_dir, kPrimaryPort, PgInstance::Role::kPrimary);
  BOOST_REQUIRE(primary.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_REQUIRE(primary.write_config({
      {"wal_level",        "replica"},
      {"max_wal_senders",  "3"},
      {"log_min_messages", "warning"},
  }));
  BOOST_REQUIRE(primary.write_hba({
      "host  all         all  127.0.0.1/32  trust",
      "host  replication all  127.0.0.1/32  trust",
  }));
  BOOST_REQUIRE(primary.start());
  BOOST_REQUIRE(primary.wait_ready(kPrimaryPort));

  // ── pgbench schema init ───────────────────────────────────────────────────
  {
    Process p(string(kPgBin) + "/pgbench",
              {"-h", "127.0.0.1", "-p", std::to_string(kPrimaryPort),
               "-i", "postgres"});
    BOOST_REQUIRE(p.start());
    BOOST_REQUIRE_EQUAL(p.wait(), 0);
  }

  // ── cold backup: stop primary, copy, read system_id, restart ─────────────
  BOOST_REQUIRE(primary.backup(backup_dir));
  uint64_t system_id = read_system_id(primary_dir);
  BOOST_REQUIRE(system_id != 0);

  BOOST_REQUIRE(primary.start());
  BOOST_REQUIRE(primary.wait_ready(kPrimaryPort));

  // ── query initial flush LSN ───────────────────────────────────────────────
  auto primary_conn = PgClient::connect(kPrimaryPort).value();
  uint64_t initial_lsn =
      parse_lsn(exec_one(primary_conn, "SELECT pg_current_wal_flush_lsn()::text"));
  primary_conn->close();
  log(INFO, "initial_lsn={:x}/{:x}",
      static_cast<uint32_t>(initial_lsn >> 32),
      static_cast<uint32_t>(initial_lsn));

  // ── set up WAL sender server ──────────────────────────────────────────────
  std::mutex            sender_mu;
  shared_ptr<WalSender> active_sender;

  // Unix domain socket paths on macOS are limited to 104 bytes; use /tmp.
  auto sock_dir = std::filesystem::path("/tmp");
  auto wal_dir  = primary_dir / "pg_wal";
  auto server   = WalSenderServer::create(
      sock_dir, 15450, wal_dir, system_id, /*timeline=*/1, initial_lsn,
      [&sender_mu, &active_sender](shared_ptr<WalSender> s) {
        std::lock_guard lk(sender_mu);
        active_sender = s;
        log(INFO, "new WAL sender connection");
      });
  BOOST_REQUIRE(server->start());
  BOOST_REQUIRE(std::filesystem::exists(server->socket_path()));

  // ── restore backup as standby ─────────────────────────────────────────────
  PgInstance standby(string(kPgBin), standby_dir, kStandbyPort);
  BOOST_REQUIRE(standby.restore(backup_dir));
  std::ofstream(standby_dir / "standby.signal").close();
  BOOST_REQUIRE(standby.write_config({
      {"primary_conninfo", "'host=" + sock_dir.string() + " port=" + std::to_string(server->bound_port()) +
                           " application_name=standby'"},
      {"hot_standby",      "on"},
      {"log_min_messages", "warning"},
  }));
  BOOST_REQUIRE(standby.start());
  BOOST_REQUIRE(standby.wait_ready(kStandbyPort));

  // ── LSN polling thread: queries primary via PgClient and calls advance() ──
  std::atomic<bool> poll_done{false};
  std::thread       poll_thread([&]() {
    auto conn = PgClient::connect(kPrimaryPort);
    if (!conn) return;
    auto stmt = (*conn)->prepare("SELECT pg_current_wal_flush_lsn()::text");
    if (!stmt) return;

    while (!poll_done.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      auto rows = exec_sync(*conn, *stmt);
      if (!rows || rows->empty() || !rows->at(0)[0]) continue;

      uint64_t lsn = parse_lsn(*rows->at(0)[0]);

      shared_ptr<WalSender> s;
      {
        std::lock_guard lk(sender_mu);
        s = active_sender;
      }
      if (s) s->advance(lsn);
    }
    (*conn)->close();
  });

  // ── start pgbench in the background ──────────────────────────────────────
  Process pgbench(string(kPgBin) + "/pgbench",
                  {"-h", "127.0.0.1", "-p", std::to_string(kPrimaryPort),
                   "-t", "200", "-c", "4", "postgres"});
  BOOST_REQUIRE(pgbench.start());

  // ── call pg_switch_wal() several times while pgbench runs ────────────────
  {
    auto switch_conn = PgClient::connect(kPrimaryPort).value();
    auto switch_stmt = switch_conn->prepare("SELECT pg_switch_wal()::text").value();

    for (int i = 0; i < 4; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(150));
      auto rows = exec_sync(switch_conn, switch_stmt);
      if (rows && !rows->empty() && rows->at(0)[0])
        log(INFO, "pg_switch_wal() -> {}", *rows->at(0)[0]);
    }
    switch_conn->close();
  }

  BOOST_REQUIRE_EQUAL(pgbench.wait(), 0);

  // ── wait for standby to catch up ──────────────────────────────────────────
  auto target_conn = PgClient::connect(kPrimaryPort).value();
  uint64_t target_lsn =
      parse_lsn(exec_one(target_conn, "SELECT pg_current_wal_flush_lsn()::text"));
  target_conn->close();
  log(INFO, "target_lsn={:x}/{:x}",
      static_cast<uint32_t>(target_lsn >> 32),
      static_cast<uint32_t>(target_lsn));

  auto sb_conn     = PgClient::connect(kStandbyPort).value();
  auto replay_stmt =
      sb_conn->prepare("SELECT COALESCE(pg_last_wal_replay_lsn(),'0/0')::text").value();

  bool caught_up = false;
  auto deadline  = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (std::chrono::steady_clock::now() < deadline) {
    auto rows = exec_sync(sb_conn, replay_stmt);
    if (rows && !rows->empty() && rows->at(0)[0]) {
      uint64_t sb_lsn = parse_lsn(*rows->at(0)[0]);
      log(INFO, "standby replay lsn={:x}/{:x}",
          static_cast<uint32_t>(sb_lsn >> 32),
          static_cast<uint32_t>(sb_lsn));
      if (sb_lsn >= target_lsn) { caught_up = true; break; }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  sb_conn->close();
  BOOST_CHECK(caught_up);

  // ── verify row counts match ───────────────────────────────────────────────
  auto pc = PgClient::connect(kPrimaryPort).value();
  auto sc = PgClient::connect(kStandbyPort).value();
  string primary_count = exec_one(pc, "SELECT COUNT(*)::text FROM pgbench_accounts");
  string standby_count = exec_one(sc, "SELECT COUNT(*)::text FROM pgbench_accounts");
  pc->close();
  sc->close();
  BOOST_CHECK(!primary_count.empty());
  BOOST_CHECK_EQUAL(primary_count, standby_count);

  // ── teardown ──────────────────────────────────────────────────────────────
  poll_done = true;
  poll_thread.join();

  standby.stop(PgInstance::StopMode::Fast);
  server->shutdown();
  primary.stop(PgInstance::StopMode::Fast);
}
