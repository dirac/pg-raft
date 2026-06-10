#include "src/base/io_threads.h"
#include "src/base/logger.h"
#include "src/process/pg_instance.h"
#include "src/process/process.h"
#include "src/wal_fuse/fuse.h"
#include "src/wal_fuse/wal_fuse_ops.h"

static char const* const kPgBin    = "/opt/pg_raft/postgresql/17/bin";
static constexpr int     kScale    = 10;   // pgbench scale factor (10 → 1M rows)
static constexpr int     kDuration = 30;   // seconds per run
static constexpr int     kClients  = 4;    // concurrent pgbench clients
static constexpr uint16_t kNoFusePort = 15500;
static constexpr uint16_t kFusePort   = 15501;

// ── pgbench process ────────────────────────────────────────────────────────────
//
// Captures the "without initial connection time" (PG14+) or legacy
// "excluding connections establishing" TPS line from pgbench stdout/stderr.

class PgbenchProcess : public Process {
 public:
  using Process::Process;
  double tps() const { return tps_; }

 protected:
  void on_stdout_line(std::string_view line) override { parse_tps(line); }
  void on_stderr_line(std::string_view line) override { parse_tps(line); }

 private:
  void parse_tps(std::string_view line) {
    double     v;
    std::string s{line};
    if (sscanf(s.c_str(), "tps = %lf (without initial connection time)", &v) == 1 ||
        sscanf(s.c_str(), "tps = %lf (excluding connections establishing)", &v) == 1)
      tps_ = v;
  }
  double tps_ = 0.0;
};

// ── pgbench helpers ────────────────────────────────────────────────────────────

static bool pgbench_init(uint16_t port) {
  PgbenchProcess p(std::string{kPgBin} + "/pgbench",
                   {"-h", "127.0.0.1", "-p", std::to_string(port),
                    "-i", "-s", std::to_string(kScale), "postgres"});
  return p.start() && p.wait() == 0;
}

static double pgbench_run(uint16_t port) {
  PgbenchProcess p(std::string{kPgBin} + "/pgbench",
                   {"-h", "127.0.0.1", "-p", std::to_string(port),
                    "-T", std::to_string(kDuration),
                    "-c", std::to_string(kClients),
                    "-j", "2",
                    "postgres"});
  if (!p.start() || p.wait() != 0)
    return 0.0;
  return p.tps();
}

// ── run without FUSE ──────────────────────────────────────────────────────────

static double run_without_fuse() {
  char tmpl[] = "/tmp/pgbench_nofuse_XXXXXX";
  if (!mkdtemp(tmpl)) {
    log(ERROR, "mkdtemp: {}", strerror(errno));
    return 0.0;
  }
  auto root = std::filesystem::path{tmpl};
  auto data = root / "pgdata";
  auto raft = root / "pgraft";
  std::filesystem::create_directory(raft);

  PgInstance pg(kPgBin, data, kNoFusePort, PgInstance::Role::kPrimary);

  if (!pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"})) {
    log(ERROR, "initdb failed");
    std::filesystem::remove_all(root);
    return 0.0;
  }
  pg.write_config({{"listen_addresses", "'127.0.0.1'"}, {"fsync", "on"}});
  pg.write_hba({"host  all  all  127.0.0.1/32  trust"});

  if (!pg.start()) {
    log(ERROR, "pg start failed");
    std::filesystem::remove_all(root);
    return 0.0;
  }
  if (!pg.wait_ready(kNoFusePort)) {
    log(ERROR, "pg not ready");
    pg.stop();
    std::filesystem::remove_all(root);
    return 0.0;
  }
  if (!pgbench_init(kNoFusePort)) {
    log(ERROR, "pgbench init failed");
    pg.stop();
    std::filesystem::remove_all(root);
    return 0.0;
  }

  double tps = pgbench_run(kNoFusePort);
  pg.stop(PgInstance::StopMode::Fast);
  std::filesystem::remove_all(root);
  return tps;
}

// ── run with FUSE ─────────────────────────────────────────────────────────────

static double run_with_fuse() {
  char tmpl[] = "/tmp/pgbench_fuse_XXXXXX";
  if (!mkdtemp(tmpl)) {
    log(ERROR, "mkdtemp: {}", strerror(errno));
    return 0.0;
  }
  auto root        = std::filesystem::path{tmpl};
  auto data        = root / "pgdata";
  auto raft        = root / "pgraft";
  auto raft_pg_wal = raft / "pg_wal";
  std::filesystem::create_directory(raft);

  WalTracker tracker;
  // Fuse must be declared before PgInstance so postgres is stopped (via
  // PgInstance destructor) before the FUSE mount is torn down.
  Fuse       fuse(data / "pg_wal", std::make_unique<WalFuseOps>(tracker, raft_pg_wal));
  PgInstance pg(kPgBin, data, kFusePort, PgInstance::Role::kPrimary);

  if (!pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"})) {
    log(ERROR, "initdb failed");
    std::filesystem::remove_all(root);
    return 0.0;
  }
  if (!pg.create_raft_wal(raft)) {
    log(ERROR, "create_raft_wal failed");
    std::filesystem::remove_all(root);
    return 0.0;
  }
  pg.write_config({{"listen_addresses", "'127.0.0.1'"}, {"fsync", "on"}});
  pg.write_hba({"host  all  all  127.0.0.1/32  trust"});

  if (!fuse.start()) {
    log(ERROR, "FUSE start failed");
    std::filesystem::remove_all(root);
    return 0.0;
  }
  if (!pg.start()) {
    log(ERROR, "pg start failed");
    fuse.stop();
    std::filesystem::remove_all(root);
    return 0.0;
  }
  if (!pg.wait_ready(kFusePort)) {
    log(ERROR, "pg not ready");
    pg.stop();
    fuse.stop();
    std::filesystem::remove_all(root);
    return 0.0;
  }
  if (!pgbench_init(kFusePort)) {
    log(ERROR, "pgbench init failed");
    pg.stop();
    fuse.stop();
    std::filesystem::remove_all(root);
    return 0.0;
  }

  double tps = pgbench_run(kFusePort);
  pg.stop(PgInstance::StopMode::Fast);
  fuse.stop();
  std::filesystem::remove_all(root);
  return tps;
}

// ── main ──────────────────────────────────────────────────────────────────────

int main() {
  if (access((std::string{kPgBin} + "/postgres").c_str(), X_OK) != 0) {
    std::fprintf(stderr, "PostgreSQL not found at %s\n", kPgBin);
    return 1;
  }

  logger::set_level(logger::Level::Warn);
  IoThreads threads{2};

  std::printf("pgbench  scale=%d  duration=%ds  clients=%d\n\n",
              kScale, kDuration, kClients);

  std::printf("running without FUSE ...\n");
  double tps_no_fuse = run_without_fuse();
  std::printf("  tps = %.1f\n\n", tps_no_fuse);

  std::printf("running with FUSE ...\n");
  double tps_fuse = run_with_fuse();
  std::printf("  tps = %.1f\n\n", tps_fuse);

  if (tps_no_fuse > 0.0 && tps_fuse > 0.0) {
    double overhead_pct = (tps_no_fuse - tps_fuse) / tps_no_fuse * 100.0;
    std::printf("%-24s %8.1f TPS\n", "without FUSE:", tps_no_fuse);
    std::printf("%-24s %8.1f TPS\n", "with FUSE:",    tps_fuse);
    std::printf("%-24s %+8.1f%%\n",  "FUSE overhead:", overhead_pct);
  }
  return 0;
}
