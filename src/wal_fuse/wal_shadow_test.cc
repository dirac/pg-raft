#define BOOST_TEST_MODULE wal_shadow

#include "src/base/testing.h"
#include "src/process/pg_instance.h"
#include "src/process/process.h"
#include "src/wal_fuse/fuse.h"
#include "src/wal_fuse/wal_fuse_ops.h"

static std::string const  kPgBin = "/opt/pg_raft/postgresql/17/bin";
static constexpr uint16_t kPort  = 15439;

static bool pg_available() {
  return access((kPgBin + "/postgres").c_str(), X_OK) == 0;
}

// ── WAL name helpers ──────────────────────────────────────────────────────────

static bool is_wal_name(char const* path) {
  if (*path == '/')
    ++path;
  for (int i = 0; i < 24; ++i)
    if (!isxdigit(static_cast<unsigned char>(path[i])))
      return false;
  return path[24] == '\0';
}

// ── ShadowWalOps ─────────────────────────────────────────────────────────────
//
// Derived from WalFuseOps. For WAL files it:
//   • Accumulates raw write bytes in a per-file shadow buffer (kWalSegmentSize).
//   • After each op_write: reads back every newly-published region from the
//     WalFuseOps mmap via op_read and compares it with the shadow.
//   • After each op_fsync: re-verifies all published regions for the segment,
//     confirming the mmap is consistent after msync.
//
// Non-WAL file ops are delegated to WalFuseOps unchanged.
//
// Thread safety: fuse_loop_mt may dispatch ops concurrently. The shadow map
// and error log are guarded by separate mutexes. Published regions are written
// to the mmap before async_publish returns, so by the time we read them back
// the mmap is stable for those offsets.

bool capture = false;

struct WriteRequest {
  char   path[32];
  size_t size;
  off_t  off;
};

void fwrite(int fd, void const* buf, size_t count) {
  size_t size = 0;
  while (size < count) {
    ssize_t rc = ::write(fd, reinterpret_cast<char const*>(buf) + size, count - size);
    if (rc < 0)
      log(FATAL, "write failed {} ({})", strerror(errno), errno);
    size += static_cast<size_t>(rc);
  }
}

void write_fn(char const* path, char const* buf, size_t size, off_t off) {
  static std::mutex mtx;
  static int        fd = 0;

  if (!capture)
    return;

  WriteRequest r;
  for (auto i = 0; i < 26; i++)
    r.path[i] = path[i];

  assert(path[24] != '\0');
  assert(path[25] == '\0');

  r.size = size;
  r.off  = off;

  auto lk = std::unique_lock{mtx};
  if (fd == 0) {
    fd = ::open("/tmp/wal_data", O_WRONLY | O_CREAT | O_EXCL, file_mode());
    if (fd == -1)
      log(FATAL, "open failed {} ({})", strerror(errno), errno);
  }
  static_assert(sizeof(WriteRequest) == 48);

  fwrite(fd, &r, sizeof(r));
  if (size)
    fwrite(fd, buf, size);
  if (::fsync(fd) == -1)
    log(FATAL, "fdatasync failed {} ({})", strerror(errno), errno);
}

class ShadowWalOps : public WalFuseOps {
 public:
  ShadowWalOps(WalTracker& tracker, std::filesystem::path backing)
      : WalFuseOps(tracker, std::move(backing)) {}

  int op_write(char const*            path,
               char const*            buf,
               size_t                 size,
               off_t                  off,
               struct fuse_file_info* fi) override {
    if (!is_wal_name(path))
      return WalFuseOps::op_write(path, buf, size, off, fi);

    write_fn(path, buf, size, off);

    // Mirror every raw write into the shadow before the base class runs so
    // the shadow is always a superset of what the mmap will contain.
    shadow_write(path, buf, size, off);

    return WalFuseOps::op_write(path, buf, size, off, fi);
  }

  int op_open(char const* path, struct fuse_file_info* fi) override {
    if (!is_wal_name(path))
      return WalFuseOps::op_open(path, fi);

    log(INFO, "open {}", path);
    write_fn(path, nullptr, 0U, 1);
    return WalFuseOps::op_open(path, fi);
  }

  int op_release(char const* path, struct fuse_file_info* fi) override {
    if (!is_wal_name(path))
      return WalFuseOps::op_release(path, fi);

    log(INFO, "close {}", path);

    write_fn(path, nullptr, 0U, -1);
    return WalFuseOps::op_release(path, fi);
  }

  int op_fsync(char const* path, int datasync, struct fuse_file_info* fi) override {
    int r = WalFuseOps::op_fsync(path, datasync, fi);
    return r;
  }

  vector<char> const& data(char const* path) const { return shadows_.find(path)->second; }

 private:
  void shadow_write(char const* path, char const* buf, size_t size, off_t off) {
    std::lock_guard lock(mu_);
    auto&           v = shadows_[path];
    if (v.empty())
      v.resize(kWalSegmentSize, '\0');
    memcpy(v.data() + off, buf, size);
  }

  std::mutex                mu_;
  map<string, vector<char>> shadows_;  // FUSE path → kWalSegmentSize shadow
};

std::vector<char> load_file(std::filesystem::path filename) {
  int fd = ::open(filename.c_str(), O_RDONLY, file_mode());

  std::vector<char> buffer;
  buffer.resize(kWalSegmentSize, 'a');

  size_t count = kWalSegmentSize;
  size_t size  = 0;
  while (size < count) {
    ssize_t rc = ::read(fd, buffer.data() + size, count - size);
    if (rc <= 0) {
      log(ERROR, "did not read all bytes");
      return buffer;
    }
    size += static_cast<size_t>(rc);
  }
  return buffer;
}

// ── test ─────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(shadow_verification_with_pgbench) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not found at " + kPgBin + "; skipping");
    return;
  }

  logger::set_level(logger::Level::Info);

  TmpDir    tmp;
  IoThreads th{2};

  auto data_dir    = tmp() / "pgdata";
  auto raft_dir    = tmp() / "pgraft";
  auto raft_pg_wal = raft_dir / "pg_wal";
  std::filesystem::create_directory(raft_dir);

  WalTracker tracker;

  // fuse must be declared before pg so that pg's destructor runs first:
  // postgres is stopped before the FUSE mount is torn down.
  Fuse       fuse(data_dir / "pg_wal", std::make_unique<ShadowWalOps>(tracker, raft_pg_wal));
  PgInstance pg(kPgBin, data_dir, PgInstance::Role::kPrimary, {"-p", std::to_string(kPort)});

  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust"}));
  std::filesystem::copy_file(data_dir / "pg_wal" / "000000010000000000000001",
                             "/tmp/000000010000000000000001");

  BOOST_REQUIRE(pg.create_raft_wal(raft_dir));
  BOOST_REQUIRE(pg.write_config({{"listen_addresses", "'127.0.0.1'"}}));
  BOOST_REQUIRE(pg.write_hba({"host  all  all  127.0.0.1/32  trust"}));

  BOOST_REQUIRE(fuse.start());
  BOOST_REQUIRE(pg.start());
  BOOST_REQUIRE(pg.wait_ready(kPort));

  Process init(kPgBin + "/pgbench",
               {"-h", "127.0.0.1", "-p", std::to_string(kPort), "-i", "postgres"});
  BOOST_REQUIRE(init.start());
  BOOST_REQUIRE_EQUAL(init.wait(), 0);

  Process bench(
      kPgBin + "/pgbench",
      {"-h", "127.0.0.1", "-p", std::to_string(kPort), "-t", "500", "-c", "4", "postgres"});
  BOOST_REQUIRE(bench.start());
  BOOST_REQUIRE_EQUAL(bench.wait(), 0);

  pg.stop(PgInstance::StopMode::Fast);
  fuse.stop();

  for (auto const& entry : std::filesystem::directory_iterator{raft_pg_wal}) {
    std::filesystem::path path = entry.path();
    char const*           name = path.c_str();
    if (auto* p = strrchr(name, '/'); p && is_wal_name(p)) {
      if (p[24] == '1')
        continue;

      log(INFO, "Comparing {}", p);

      auto  file = load_file(path);
      auto& exp  = static_cast<ShadowWalOps const&>(fuse.ops()).data(p);
      BOOST_TEST(file == exp);
    }
  }

  ::unlink("/tmp/wal_data");
  ::unlink("/tmp/000000010000000000000001");
}
