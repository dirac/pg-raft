#define BOOST_TEST_MODULE timeline_switch

#include "src/base/io_threads.h"
#include "src/base/testing.h"
#include "src/base/wal_segment.h"
#include "src/process/pg_instance.h"
#include "src/status/pg_client.h"
#include "src/wal_sender/wal_sender_server.h"

// A failover, as WalSender sees it: a standby that streams through WalSender follows the new
// primary onto timeline 2.
//
//   primary P (timeline 1) ──streaming──▶ standby S2 (becomes the new primary)
//            └── WAL dir M ──WalSender──▶ standby S1 (the follower under test)
//
// M plays raft/wal: it gets timeline 1's segments from P, then timeline 2's segments and
// 00000002.history from S2 once it is promoted, as hard links (a raft follower would have written
// them as it applied the entries). P is stopped, S2 is promoted and runs pgbench, and S1 must end
// on timeline 2 with S2's rows. WalSender learns timeline 2 from M's history file and ends
// timeline 1 at its switch point. S1 then streams timeline 2 from the start of the segment that
// holds the switch point; WalSender reads the bytes before the switch point from timeline 1's
// segment, never from timeline 2's (which, in pg-raft, has zeros there).

static std::string const kPgBin   = kPgTestBin;
static constexpr uint16_t    kPPort   = 15460;
static constexpr uint16_t    kS2Port  = 15461;
static constexpr uint16_t    kS1Port  = 15462;
static constexpr uint16_t    kWalPort = 15463;

namespace fs = std::filesystem;

static bool pg_available() {
  return access((string(kPgBin) + "/postgres").c_str(), X_OK) == 0;
}

static Result<Rows> exec_sync(shared_ptr<PgClient> conn, PgStmt const& stmt) {
  std::promise<Result<Rows>> p;
  conn->async_exec(stmt, [&p](Result<Rows> r) { p.set_value(std::move(r)); });
  return p.get_future().get();
}

static string exec_one(uint16_t port, char const* sql) {
  auto conn = PgClient::connect(port);
  if (!conn)
    return "";
  auto stmt = (*conn)->prepare(sql);
  if (!stmt) {
    (*conn)->close();
    return "";
  }
  auto   rows = exec_sync(*conn, *stmt);
  string out;
  if (rows && !rows->empty() && !rows->at(0).empty() && rows->at(0)[0])
    out = *rows->at(0)[0];
  (*conn)->close();
  return out;
}

static uint64_t parse_lsn(string const& s) {
  uint32_t hi = 0, lo = 0;
  sscanf(s.c_str(), "%X/%X", &hi, &lo);
  return (static_cast<uint64_t>(hi) << 32) | lo;
}

static std::map<string, string> controldata(fs::path const& data_dir) {
  std::map<string, string> out;
  string cmd = string(kPgBin) + "/pg_controldata -D " + data_dir.string() + " 2>/dev/null";
  FILE*  f   = popen(cmd.c_str(), "r");
  if (!f)
    return out;
  char line[512];
  while (fgets(line, sizeof(line), f)) {
    string l(line);
    auto   colon = l.find(':');
    if (colon == string::npos)
      continue;
    auto val = l.substr(colon + 1);
    while (!val.empty() && isspace(static_cast<unsigned char>(val.front())))
      val.erase(val.begin());
    while (!val.empty() && isspace(static_cast<unsigned char>(val.back())))
      val.pop_back();
    out[l.substr(0, colon)] = val;
  }
  pclose(f);
  return out;
}

static void pgbench(uint16_t port, std::vector<string> args) {
  args.insert(args.begin(), {"-h", "127.0.0.1", "-p", std::to_string(port)});
  args.push_back("postgres");
  Process p(string(kPgBin) + "/pgbench", args);
  BOOST_REQUIRE(p.start());
  BOOST_REQUIRE_EQUAL(p.wait(), 0);
}

/// Hard-link into |to| the files of |from| that |select| picks and |to| doesn't have yet.
static void mirror(fs::path const&                    from,
                   fs::path const&                    to,
                   std::function<bool(string const&)> select) {
  std::error_code ec;
  for (auto const& entry : fs::directory_iterator(from, ec)) {
    auto name = entry.path().filename().string();
    if (!entry.is_regular_file(ec) || !select(name) || fs::exists(to / name, ec))
      continue;
    fs::create_hard_link(entry.path(), to / name, ec);
  }
}

/// Wait until |pred| holds, up to |secs| seconds.
static bool wait_for(int secs, std::function<bool()> pred) {
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(secs);
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred())
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  return pred();
}

BOOST_AUTO_TEST_CASE(standby_follows_promotion, *boost::unit_test::timeout(180)) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not available at " + string(kPgBin) + "; skipping");
    return;
  }

  IoThreads io{4};
  TmpDir    tmp;

  auto p_dir   = tmp() / "p";
  auto s2_dir  = tmp() / "s2";
  auto s1_dir  = tmp() / "s1";
  auto backup  = tmp() / "backup";
  auto wal_dir = tmp() / "wal";  // M: plays raft/wal
  fs::create_directories(wal_dir);

  // ── primary P on timeline 1 ───────────────────────────────────────────────
  PgInstance p(string(kPgBin), p_dir, kPPort, PgInstance::Role::kPrimary);
  BOOST_REQUIRE(p.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_REQUIRE(p.write_config({{"max_wal_senders", "5"}, {"log_min_messages", "warning"}}));
  BOOST_REQUIRE(p.write_hba({"host  all  all  127.0.0.1/32  trust"}));
  BOOST_REQUIRE(p.start());
  BOOST_REQUIRE(p.wait_ready(kPPort));
  pgbench(kPPort, {"-i"});

  BOOST_REQUIRE(p.backup(backup));
  auto system_id = std::stoull(controldata(p_dir)["Database system identifier"]);
  auto initial   = parse_lsn(exec_one(kPPort, "SELECT pg_current_wal_flush_lsn()::text"));

  auto is_tl = [](char const* prefix) {
    return [prefix](string const& name) { return name.starts_with(prefix); };
  };
  mirror(p_dir / "pg_wal", wal_dir, is_tl("00000001"));

  // ── WalSender over M ──────────────────────────────────────────────────────
  std::mutex            sender_mu;
  shared_ptr<WalSender> sender;
  auto                  sock_dir = fs::path("/tmp");
  auto                  server   = WalSenderServer::create(sock_dir,
                                        kWalPort,
                                        wal_dir,
                                        system_id,
                                        /*timeline=*/1,
                                        initial,
                                        [&](shared_ptr<WalSender> s) {
                                          std::lock_guard lk(sender_mu);
                                          sender = s;
                                        });
  BOOST_REQUIRE(server->start());

  // ── S2: a standby of P, the future primary ────────────────────────────────
  PgInstance s2(string(kPgBin), s2_dir, kS2Port);
  BOOST_REQUIRE(s2.restore(backup));
  std::ofstream(s2_dir / "standby.signal").close();
  BOOST_REQUIRE(s2.write_config({
      {"primary_conninfo", std::format("'host=127.0.0.1 port={} application_name=s2'", kPPort)},
      {"log_min_messages", "warning"},
  }));
  BOOST_REQUIRE(s2.start());
  BOOST_REQUIRE(s2.wait_ready(kS2Port));

  // ── S1: a standby through WalSender ───────────────────────────────────────
  PgInstance s1(string(kPgBin), s1_dir, kS1Port);
  BOOST_REQUIRE(s1.restore(backup));
  std::ofstream(s1_dir / "standby.signal").close();
  BOOST_REQUIRE(s1.write_config({
      {"primary_conninfo",
       std::format(
           "'host={} port={} application_name=s1'", sock_dir.string(), server->bound_port())},
      {"wal_retrieve_retry_interval", "500ms"},
      {"log_min_messages", "warning"},
  }));
  BOOST_REQUIRE(s1.start());
  BOOST_REQUIRE(s1.wait_ready(kS1Port));

  // ── the "raft" loop: mirror the WAL into M, then advance the sender ───────
  // flush: the committed WAL. Before the failover it follows P; during it, it is fixed by the test;
  // after it, it follows S2 on timeline 2.
  std::atomic<int>      stage{0};  // 0: follow P, 1: fixed, 2: follow S2
  std::atomic<uint64_t> fixed_lsn{0};
  std::atomic<bool>     done{false};
  std::thread           loop([&] {
    while (!done) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      LogSeqNumber flush{};
      if (stage == 0) {
        mirror(p_dir / "pg_wal", wal_dir, is_tl("00000001"));
        flush = {1, parse_lsn(exec_one(kPPort, "SELECT pg_current_wal_flush_lsn()::text"))};
      } else if (stage == 1) {
        flush = {1, fixed_lsn.load()};
      } else {
        // The history file first: raft applies the TimelineOp before the entries on timeline 2.
        mirror(s2_dir / "pg_wal", wal_dir, [](string const& name) {
          return name == "00000002.history" || name.starts_with("00000002");
        });
        auto file = exec_one(kS2Port, "SELECT pg_walfile_name(pg_current_wal_flush_lsn())");
        auto lsn = exec_one(kS2Port, "SELECT pg_current_wal_flush_lsn()::text");
        if (file.size() < 8 || lsn.empty())
          continue;
        flush = {static_cast<uint32_t>(std::stoul(file.substr(0, 8), nullptr, 16)), parse_lsn(lsn)};
      }
      if (flush.lsn == 0)
        continue;
      std::lock_guard lk(sender_mu);
      if (sender)
        sender->advance(flush);
    }
  });

  // ── timeline 1: pgbench on P, S1 catches up through WalSender ─────────────
  pgbench(kPPort, {"-t", "100", "-c", "4"});
  auto p_flush = parse_lsn(exec_one(kPPort, "SELECT pg_current_wal_flush_lsn()::text"));
  BOOST_REQUIRE(wait_for(60, [&] {
    return parse_lsn(exec_one(kS1Port, "SELECT pg_last_wal_replay_lsn()::text")) >= p_flush;
  }));

  // ── the failover: stop P, S2 receives its last WAL, promote S2 ────────────
  BOOST_REQUIRE(wait_for(60, [&] {
    return parse_lsn(exec_one(kS2Port, "SELECT pg_last_wal_receive_lsn()::text")) >= p_flush;
  }));
  p.stop(PgInstance::StopMode::Fast);
  mirror(p_dir / "pg_wal", wal_dir, is_tl("00000001"));

  // The end of timeline 1: S2 replays P's shutdown checkpoint record.
  auto ckpt = parse_lsn(controldata(p_dir)["Latest checkpoint location"]);
  BOOST_REQUIRE(ckpt > 0);
  uint64_t tl1_end = 0;
  BOOST_REQUIRE(wait_for(60, [&] {
    tl1_end = parse_lsn(exec_one(kS2Port, "SELECT pg_last_wal_replay_lsn()::text"));
    return tl1_end > ckpt;
  }));
  fixed_lsn = tl1_end;
  stage     = 1;

  s2.promote();  // the promote file, then SIGUSR1
  BOOST_REQUIRE(wait_for(
      60, [&] { return exec_one(kS2Port, "SELECT pg_is_in_recovery()::text") == "false"; }));
  BOOST_REQUIRE(fs::exists(s2_dir / "pg_wal" / "00000002.history"));
  stage = 2;

  // ── timeline 2: pgbench on S2, S1 follows through WalSender ───────────────
  pgbench(kS2Port, {"-t", "100", "-c", "4"});
  auto s2_flush = parse_lsn(exec_one(kS2Port, "SELECT pg_current_wal_flush_lsn()::text"));
  log(INFO,
      "timeline 1 ended at {:X}/{:X}, S2 flushed {:X}/{:X}",
      tl1_end >> 32,
      tl1_end & 0xFFFFFFFF,
      s2_flush >> 32,
      s2_flush & 0xFFFFFFFF);

  bool caught_up = wait_for(90, [&] {
    return parse_lsn(exec_one(kS1Port, "SELECT pg_last_wal_replay_lsn()::text")) >= s2_flush;
  });
  BOOST_CHECK(caught_up);

  auto s1_tli = exec_one(kS1Port, "SELECT received_tli::text FROM pg_stat_wal_receiver");
  BOOST_CHECK_EQUAL(s1_tli, "2");
  BOOST_CHECK(fs::exists(s1_dir / "pg_wal" / "00000002.history"));

  auto s2_rows = exec_one(kS2Port, "SELECT count(*)::text FROM pgbench_history");
  auto s1_rows = exec_one(kS1Port, "SELECT count(*)::text FROM pgbench_history");
  BOOST_CHECK(!s2_rows.empty());
  BOOST_CHECK_EQUAL(s1_rows, s2_rows);

  done = true;
  loop.join();
  s1.stop(PgInstance::StopMode::Fast);
  server->shutdown();
  s2.stop(PgInstance::StopMode::Fast);
}
