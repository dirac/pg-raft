#define BOOST_TEST_MODULE pg_client

#include "src/base/io_threads.h"
#include "src/base/testing.h"
#include "src/process/pg_instance.h"
#include "src/status/pg_client.h"

static constexpr char const* kPgBin = "/opt/pg_raft/postgresql/17/bin";
static constexpr uint16_t    kPort  = 15441;

static bool pg_available() {
  return access((string(kPgBin) + "/postgres").c_str(), X_OK) == 0;
}

// ---- fixture ----

struct Setup {
  TmpDir                    tmp;
  IoThreads                 io{4};
  std::optional<PgInstance> pg;

  Setup() {
    if (!pg_available())
      return;

    auto data = tmp() / "pgdata";

    pg.emplace(string(kPgBin), data, kPort, PgInstance::Role::kPrimary);
    BOOST_REQUIRE(pg->init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
    BOOST_REQUIRE(pg->write_config({}));
    BOOST_REQUIRE(pg->write_hba({}));
    BOOST_REQUIRE(pg->start());
    BOOST_REQUIRE(pg->wait_ready(kPort));
  }

  ~Setup() {
    if (pg)
      pg->stop(PgInstance::StopMode::Fast);
  }
};

// Block until async_exec delivers its result.
static Result<Rows> exec_sync(shared_ptr<PgClient> conn, PgStmt const& stmt) {
  std::promise<Result<Rows>> p;
  conn->async_exec(stmt, [&p](Result<Rows> r) { p.set_value(std::move(r)); });
  return p.get_future().get();
}

// ---- tests ----

BOOST_FIXTURE_TEST_CASE(basic_query, Setup) {
  if (!pg) { BOOST_TEST_MESSAGE("PostgreSQL not available, skipping"); return; }

  auto conn = PgClient::connect(kPort).value();
  auto stmt = conn->prepare("SELECT 1::text, 'hello'::text").value();

  auto r = exec_sync(conn, stmt);
  BOOST_REQUIRE(!r.has_error());
  BOOST_REQUIRE(r->size() == 1);
  BOOST_REQUIRE(r->at(0).size() == 2);
  BOOST_REQUIRE(r->at(0)[0]);
  BOOST_TEST(*r->at(0)[0] == string{"1"});
  BOOST_REQUIRE(r->at(0)[1]);
  BOOST_TEST(*r->at(0)[1] == string{"hello"});

  conn->close();
}

BOOST_FIXTURE_TEST_CASE(null_column, Setup) {
  if (!pg) { BOOST_TEST_MESSAGE("PostgreSQL not available, skipping"); return; }

  auto conn = PgClient::connect(kPort).value();
  auto stmt = conn->prepare("SELECT NULL::text, 42::text").value();

  auto r = exec_sync(conn, stmt);
  BOOST_REQUIRE(!r.has_error());
  BOOST_REQUIRE(r->size() == 1);
  BOOST_REQUIRE(r->at(0).size() == 2);
  BOOST_TEST(!r->at(0)[0].has_value());
  BOOST_REQUIRE(r->at(0)[1]);
  BOOST_TEST(*r->at(0)[1] == string{"42"});

  conn->close();
}

BOOST_FIXTURE_TEST_CASE(multiple_rows, Setup) {
  if (!pg) { BOOST_TEST_MESSAGE("PostgreSQL not available, skipping"); return; }

  auto conn = PgClient::connect(kPort).value();
  auto stmt = conn->prepare("SELECT generate_series(1, 5)::text").value();

  auto r = exec_sync(conn, stmt);
  BOOST_REQUIRE(!r.has_error());
  BOOST_TEST(r->size() == 5);
  for (size_t i = 0; i < r->size(); ++i) {
    BOOST_REQUIRE(r->at(i)[0]);
    BOOST_TEST(*r->at(i)[0] == to_string(i + 1));
  }

  conn->close();
}

BOOST_FIXTURE_TEST_CASE(queued_execs, Setup) {
  if (!pg) { BOOST_TEST_MESSAGE("PostgreSQL not available, skipping"); return; }

  auto conn = PgClient::connect(kPort).value();
  auto stmt = conn->prepare("SELECT 42::text").value();

  constexpr int     N = 5;
  std::latch        done{N};
  std::atomic<int>  ok{0};

  for (int i = 0; i < N; ++i) {
    conn->async_exec(stmt, [&done, &ok](Result<Rows> r) {
      if (!r.has_error() && r->size() == 1 && r->at(0)[0] && *r->at(0)[0] == string{"42"})
        ok++;
      done.count_down();
    });
  }

  done.wait();
  BOOST_TEST(ok == N);

  conn->close();
}

BOOST_FIXTURE_TEST_CASE(multiple_stmts, Setup) {
  if (!pg) { BOOST_TEST_MESSAGE("PostgreSQL not available, skipping"); return; }

  auto conn = PgClient::connect(kPort).value();
  auto s1   = conn->prepare("SELECT 'one'::text").value();
  auto s2   = conn->prepare("SELECT 'two'::text").value();

  constexpr int    N = 4;
  std::latch       done{N * 2};
  std::atomic<int> s1_ok{0}, s2_ok{0};

  for (int i = 0; i < N; ++i) {
    conn->async_exec(s1, [&done, &s1_ok](Result<Rows> r) {
      if (!r.has_error() && r->at(0)[0] && *r->at(0)[0] == string{"one"})
        s1_ok++;
      done.count_down();
    });
    conn->async_exec(s2, [&done, &s2_ok](Result<Rows> r) {
      if (!r.has_error() && r->at(0)[0] && *r->at(0)[0] == string{"two"})
        s2_ok++;
      done.count_down();
    });
  }

  done.wait();
  BOOST_TEST(s1_ok == N);
  BOOST_TEST(s2_ok == N);

  conn->close();
}

BOOST_FIXTURE_TEST_CASE(exec_error_then_recover, Setup) {
  if (!pg) { BOOST_TEST_MESSAGE("PostgreSQL not available, skipping"); return; }

  auto conn      = PgClient::connect(kPort).value();
  auto bad_stmt  = conn->prepare("SELECT (1/0)::text").value();  // runtime division by zero
  auto good_stmt = conn->prepare("SELECT 'ok'::text").value();

  // Bad exec should report an error.
  auto r1 = exec_sync(conn, bad_stmt);
  BOOST_TEST(r1.has_error());

  // Connection must remain usable after the error.
  auto r2 = exec_sync(conn, good_stmt);
  BOOST_REQUIRE(!r2.has_error());
  BOOST_REQUIRE(r2->at(0)[0]);
  BOOST_TEST(*r2->at(0)[0] == string{"ok"});

  conn->close();
}

BOOST_FIXTURE_TEST_CASE(wal_lsn_query, Setup) {
  if (!pg) { BOOST_TEST_MESSAGE("PostgreSQL not available, skipping"); return; }

  auto conn = PgClient::connect(kPort).value();
  auto stmt = conn->prepare(
                    "SELECT * FROM pg_walfile_name_offset(pg_current_wal_flush_lsn())")
                  .value();

  auto r = exec_sync(conn, stmt);
  BOOST_REQUIRE(!r.has_error());
  BOOST_REQUIRE(r->size() == 1);
  BOOST_REQUIRE(r->at(0).size() == 2);  // file_name, file_offset
  BOOST_TEST(r->at(0)[0].has_value());
  BOOST_TEST(r->at(0)[1].has_value());
  log(INFO, "WAL: file={} offset={}", *r->at(0)[0], *r->at(0)[1]);

  conn->close();
}
