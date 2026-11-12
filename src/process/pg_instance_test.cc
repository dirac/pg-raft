#define BOOST_TEST_MODULE pg_instance

#include "src/process/pg_instance.h"

#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>

#include "src/base/io_threads.h"
#include "src/base/testing.h"

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

BOOST_AUTO_TEST_CASE(init, *boost::unit_test::timeout(60)) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not found; skipping");
    return;
  }
  IoThreads threads{1};
  TmpDir    tmp;
  auto      data_dir = tmp() / "pgdata";

  PgInstance pg(kPgBin, data_dir, kPort);
  BOOST_TEST(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_TEST(std::filesystem::exists(data_dir / "postgresql.conf"));
  BOOST_TEST(std::filesystem::exists(data_dir / "pg_wal"));
  BOOST_TEST(count_regular_files(data_dir / "pg_wal") > 0);
}

// ── write_config ──────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(write_config_appends_defaults, *boost::unit_test::timeout(60)) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not found; skipping");
    return;
  }
  IoThreads threads{1};
  TmpDir    tmp;
  auto      data_dir = tmp() / "pgdata";

  PgInstance pg(kPgBin, data_dir, kPort);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_TEST(pg.write_config({{"max_connections", "50"}}));

  auto conf = read_file(data_dir / "postgresql.conf");
  BOOST_TEST(conf.find("max_connections = 50") != std::string::npos);
  BOOST_TEST(conf.find("wal_sync_method") != std::string::npos);
  BOOST_TEST(conf.find("wal_recycle") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(write_config_overrides_default, *boost::unit_test::timeout(60)) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not found; skipping");
    return;
  }
  IoThreads threads{1};
  TmpDir    tmp;
  auto      data_dir = tmp() / "pgdata";

  PgInstance pg(kPgBin, data_dir, kPort);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  // wal_recycle defaults to "off"; user overrides with "on".
  BOOST_TEST(pg.write_config({{"wal_recycle", "on"}}));

  auto conf = read_file(data_dir / "postgresql.conf");
  BOOST_TEST(conf.find("wal_recycle = on") != std::string::npos);
  BOOST_TEST(conf.find("wal_recycle = off") == std::string::npos);
}

// ── write_hba ─────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(write_hba_appends_lines_and_defaults, *boost::unit_test::timeout(60)) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not found; skipping");
    return;
  }
  IoThreads threads{1};
  TmpDir    tmp;
  auto      data_dir = tmp() / "pgdata";

  PgInstance pg(kPgBin, data_dir, kPort);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_TEST(pg.write_hba({"host  replication  all  127.0.0.1/32  trust"}));

  auto hba = read_file(data_dir / "pg_hba.conf");
  BOOST_TEST(hba.find("host  replication  all  127.0.0.1/32  trust") != std::string::npos);
  BOOST_TEST(hba.find("scram-sha-256") != std::string::npos);
}

// ── create_raft_wal ───────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(create_raft_wal_moves_segments, *boost::unit_test::timeout(60)) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not found; skipping");
    return;
  }
  IoThreads threads{1};
  TmpDir    tmp;
  auto      data_dir    = tmp() / "pgdata";
  auto      raft_dir    = tmp() / "pgraft";
  auto      raft_pg_wal = raft_dir / "pg_wal";
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

BOOST_AUTO_TEST_CASE(start_and_stop, *boost::unit_test::timeout(60)) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not found; skipping");
    return;
  }
  IoThreads threads{1};
  TmpDir    tmp;
  auto      data_dir = tmp() / "pgdata";

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

BOOST_AUTO_TEST_CASE(stop_immediate, *boost::unit_test::timeout(60)) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not found; skipping");
    return;
  }
  IoThreads threads{1};
  TmpDir    tmp;
  auto      data_dir = tmp() / "pgdata";

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

BOOST_AUTO_TEST_CASE(backup, *boost::unit_test::timeout(60)) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not found; skipping");
    return;
  }
  IoThreads threads{1};
  TmpDir    tmp;
  auto      data_dir   = tmp() / "pgdata";
  auto      backup_dir = tmp() / "backup";

  PgInstance pg(kPgBin, data_dir, kPort, PgInstance::Role::kPrimary);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_REQUIRE(pg.write_config({}));
  BOOST_REQUIRE(pg.write_hba({}));
  BOOST_REQUIRE(pg.start());
  BOOST_REQUIRE(pg.wait_ready(kPort));

  BOOST_REQUIRE(pg.backup(backup_dir));
  BOOST_TEST(pg.is_running());
  BOOST_TEST(std::filesystem::exists(backup_dir / "postgresql.conf"));
  BOOST_TEST(std::filesystem::exists(backup_dir / "pg_wal"));
}

BOOST_AUTO_TEST_CASE(verify_backup_standby, *boost::unit_test::timeout(60)) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not found; skipping");
    return;
  }
  IoThreads threads{1};
  TmpDir    tmp;
  auto      data_dir   = tmp() / "pgdata";
  auto      backup_dir = tmp() / "backup";

  // kStandby: backup() stops the instance and copies, producing a cleanly
  // shut-down backup_dir that pg_checksums can verify.
  PgInstance pg(kPgBin, data_dir, kPort, PgInstance::Role::kStandby);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_REQUIRE(pg.write_config({}));
  BOOST_REQUIRE(pg.write_hba({}));
  BOOST_REQUIRE(pg.start());
  BOOST_REQUIRE(pg.wait_ready(kPort));

  auto lk = pg.backup(backup_dir);
  BOOST_REQUIRE(lk);
  lk = pg.verify_backup(backup_dir, std::move(lk));
  BOOST_TEST(bool(lk));
}

BOOST_AUTO_TEST_CASE(verify_backup_primary, *boost::unit_test::timeout(60)) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not found; skipping");
    return;
  }
  IoThreads threads{1};
  TmpDir    tmp;
  auto      data_dir   = tmp() / "pgdata";
  auto      backup_dir = tmp() / "backup";

  // kPrimary: backup() uses pg_basebackup; instance stays running.
  // verify_backup detects "in production" state and uses pg_verifybackup.
  PgInstance pg(kPgBin, data_dir, kPort, PgInstance::Role::kPrimary);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_REQUIRE(pg.write_config({}));
  BOOST_REQUIRE(pg.write_hba({}));
  BOOST_REQUIRE(pg.start());
  BOOST_REQUIRE(pg.wait_ready(kPort));

  auto lk = pg.backup(backup_dir);
  BOOST_REQUIRE(lk);
  BOOST_TEST(pg.is_running());

  lk = pg.verify_backup(backup_dir, std::move(lk));
  BOOST_TEST(bool(lk));
  BOOST_TEST(pg.is_running());
}

BOOST_AUTO_TEST_CASE(restore_and_start, *boost::unit_test::timeout(60)) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not found; skipping");
    return;
  }
  IoThreads threads{1};
  TmpDir    tmp;
  auto      data_dir    = tmp() / "pgdata";
  auto      backup_dir  = tmp() / "backup";
  auto      restore_dir = tmp() / "restored";

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

// ── kill ──────────────────────────────────────────────────────────────────────

// kill() takes everything down at once, including a backend in the middle of a query, which a
// signal to the postmaster's process group would miss (every child calls setsid).
BOOST_AUTO_TEST_CASE(kill_takes_down_every_process, *boost::unit_test::timeout(60)) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not found; skipping");
    return;
  }
  IoThreads threads{2};
  TmpDir    tmp;
  auto      data_dir = tmp() / "pgdata";

  PgInstance pg(kPgBin, data_dir, kPort, PgInstance::Role::kPrimary);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_REQUIRE(pg.write_config({}));
  BOOST_REQUIRE(pg.write_hba({}));
  BOOST_REQUIRE(pg.start());
  BOOST_REQUIRE(pg.wait_ready(kPort));

  // A backend busy in a query.
  Process psql(
      kPgBin + "/psql",
      {"-h", "127.0.0.1", "-p", std::to_string(kPort), "-c", "SELECT pg_sleep(30)", "postgres"});
  BOOST_REQUIRE(psql.start());
  std::this_thread::sleep_for(std::chrono::seconds(1));

  auto children = child_pids(pg.pid());
  BOOST_TEST(children.size() >= 5U);  // checkpointer, bgwriter, walwriter, ... and the backend
  for (pid_t c : children)
    BOOST_TEST(::getpgid(c) == c);  // each its own group

  pg.kill();

  // Every child is gone at once (they are reparented and reaped by init); psql lost its backend.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  for (pid_t c : children)
    BOOST_TEST((::kill(c, 0) == -1 && errno == ESRCH), c);
  BOOST_TEST(psql.wait() != 0);

  // The postmaster is reaped by stop(), and PostgreSQL restarts in the same directory.
  pg.stop(PgInstance::StopMode::Fast);
  BOOST_TEST(!pg.is_running());
  BOOST_REQUIRE(pg.start());
  BOOST_TEST(pg.wait_ready(kPort));
  pg.stop(PgInstance::StopMode::Fast);
}

// restore() keeps the data directory itself (its inode keys PostgreSQL's System V IPC) and replaces
// its contents.
BOOST_AUTO_TEST_CASE(restore_keeps_the_directory, *boost::unit_test::timeout(60)) {
  IoThreads threads{1};
  TmpDir    tmp;
  auto      data_dir = tmp() / "pgdata";
  auto      backup   = tmp() / "backup";
  std::filesystem::create_directories(data_dir / "base");
  std::filesystem::create_directories(backup / "global");
  std::ofstream(data_dir / "stale") << "old";
  std::ofstream(backup / "PG_VERSION") << "17\n";

  struct stat before{};
  BOOST_REQUIRE(::stat(data_dir.c_str(), &before) == 0);

  PgInstance pg(kPgBin, data_dir, kPort);
  BOOST_REQUIRE(pg.restore(backup));

  struct stat after{};
  BOOST_REQUIRE(::stat(data_dir.c_str(), &after) == 0);
  BOOST_TEST(after.st_ino == before.st_ino);
  BOOST_TEST(!std::filesystem::exists(data_dir / "stale"));
  BOOST_TEST(!std::filesystem::exists(data_dir / "base"));
  BOOST_TEST(read_file(data_dir / "PG_VERSION") == "17\n");
  BOOST_TEST(std::filesystem::is_directory(data_dir / "global"));
}

// ── System V IPC after a kill ────────────────────────────────────────────────

namespace {

/// The shared memory key of postmaster.pid (7th line), 0 if none.
long shm_key(std::filesystem::path const& data_dir) {
  std::ifstream in(data_dir / "postmaster.pid");
  std::string   line;
  for (int i = 0; i < 7 && std::getline(in, line); ++i) {
  }
  long key = 0;
  std::istringstream(line) >> key;
  return key;
}

/// The semaphore sets of keys [key, key + 256).
int semaphore_sets(long key) {
  int n = 0;
  for (long k = key; k < key + 256; ++k)
    if (::semget(static_cast<key_t>(k), 0, 0) >= 0)
      ++n;
  return n;
}

}  // namespace

// A killed PostgreSQL leaves its System V segment and semaphore sets behind; stop() reaps it and
// releases them, and leaves a running PostgreSQL's alone.
BOOST_AUTO_TEST_CASE(kill_releases_ipc, *boost::unit_test::timeout(120)) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not found; skipping");
    return;
  }
  IoThreads threads{2};
  TmpDir    tmp;

  PgInstance a(kPgBin, tmp() / "a", kPort, PgInstance::Role::kPrimary);
  PgInstance b(kPgBin, tmp() / "b", kPort + 1, PgInstance::Role::kPrimary);
  for (auto* pg : {&a, &b}) {
    BOOST_REQUIRE(pg->init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
    BOOST_REQUIRE(pg->write_config({}));
    BOOST_REQUIRE(pg->write_hba({}));
    BOOST_REQUIRE(pg->start());
  }
  BOOST_REQUIRE(a.wait_ready(kPort));
  BOOST_REQUIRE(b.wait_ready(kPort + 1));

  auto key_a = shm_key(tmp() / "a");
  auto key_b = shm_key(tmp() / "b");
  BOOST_REQUIRE(key_a != 0);
  BOOST_REQUIRE(key_b != 0);
  BOOST_TEST(::shmget(static_cast<key_t>(key_a), 0, 0) >= 0);
  auto sets_b = semaphore_sets(key_b);

  a.kill();
  a.stop(PgInstance::StopMode::Immediate);  // reaps it, and releases its IPC

  BOOST_TEST(::shmget(static_cast<key_t>(key_a), 0, 0) == -1);
#ifdef __APPLE__
  BOOST_TEST(semaphore_sets(key_a) == 0);  // PostgreSQL uses SysV semaphores on macOS
#endif
  // The live one's are untouched.
  BOOST_TEST(::shmget(static_cast<key_t>(key_b), 0, 0) >= 0);
  BOOST_TEST(semaphore_sets(key_b) == sets_b);
  BOOST_TEST(a.release_ipc() == 0);  // nothing left, and b's are not a's

  // a restarts in its directory.
  BOOST_REQUIRE(a.start());
  BOOST_TEST(a.wait_ready(kPort));
  a.stop(PgInstance::StopMode::Fast);
  b.stop(PgInstance::StopMode::Fast);
}
