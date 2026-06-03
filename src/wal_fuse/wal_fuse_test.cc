#define BOOST_TEST_MODULE WalFuseTest

#include "src/wal_fuse/wal_fuse.h"

#include "src/base/io_threads.h"
#include "src/base/testing.h"
#include "src/process/pg_instance.h"
#include "src/process/process.h"

static std::string const  kPgBin = "/opt/pg_raft/postgresql/17/bin";
static constexpr uint16_t kPort  = 15436;

static bool pg_available() {
  return access((kPgBin + "/postgres").c_str(), X_OK) == 0;
}

static int count_wal_segments(std::filesystem::path const& dir) {
  DIR* dp = opendir(dir.c_str());
  if (!dp)
    return -1;
  int            n = 0;
  struct dirent* de;
  while ((de = readdir(dp)) != nullptr)
    if (de->d_name[0] != '.' && de->d_type == DT_REG)
      ++n;
  closedir(dp);
  return n;
}

static void run_pgbench() {
  Process init(kPgBin + "/pgbench",
               {"-h", "127.0.0.1", "-p", std::to_string(kPort), "-i", "postgres"});
  BOOST_REQUIRE(init.start());
  BOOST_REQUIRE_EQUAL(init.wait(), 0);

  Process bench(
      kPgBin + "/pgbench",
      {"-h", "127.0.0.1", "-p", std::to_string(kPort), "-t", "200", "-c", "4", "postgres"});
  BOOST_REQUIRE(bench.start());
  BOOST_REQUIRE_EQUAL(bench.wait(), 0);
}

// Write |data| to |path|; returns true on success.
static bool write_file(std::filesystem::path const& path, std::string const& data) {
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0)
    return false;
  ssize_t n = write(fd, data.data(), data.size());
  close(fd);
  return n == static_cast<ssize_t>(data.size());
}

// Read entire |path| into a string.
static std::string read_file(std::filesystem::path const& path) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0)
    return {};
  std::string result;
  char        buf[4096];
  ssize_t     n;
  while ((n = read(fd, buf, sizeof(buf))) > 0)
    result.append(buf, static_cast<size_t>(n));
  close(fd);
  return result;
}

// ── lifecycle ─────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(MountAndUnmount) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());
  BOOST_CHECK(fuse.is_mounted());

  // Mount point's st_dev must differ from its parent while mounted.
  struct stat st_mp{}, st_parent{};
  BOOST_REQUIRE_EQUAL(stat(mount.c_str(), &st_mp), 0);
  BOOST_REQUIRE_EQUAL(stat(tmp().c_str(), &st_parent), 0);
  BOOST_CHECK_NE(st_mp.st_dev, st_parent.st_dev);

  fuse.stop();
  BOOST_CHECK(!fuse.is_mounted());

  // After unmount st_dev should match parent again.
  BOOST_REQUIRE_EQUAL(stat(mount.c_str(), &st_mp), 0);
  BOOST_CHECK_EQUAL(st_mp.st_dev, st_parent.st_dev);
}

BOOST_AUTO_TEST_CASE(DestructorUnmounts) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  {
    WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
    BOOST_REQUIRE(fuse.start());
  }  // destructor runs here

  struct stat st_mp{}, st_parent{};
  BOOST_REQUIRE_EQUAL(stat(mount.c_str(), &st_mp), 0);
  BOOST_REQUIRE_EQUAL(stat(tmp().c_str(), &st_parent), 0);
  BOOST_CHECK_EQUAL(st_mp.st_dev, st_parent.st_dev);
}

BOOST_AUTO_TEST_CASE(StopIdempotent) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());
  fuse.stop();
  fuse.stop();  // must not crash
  BOOST_CHECK(!fuse.is_mounted());
}

// ── passthrough ───────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(BackingFileVisibleThroughMount) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);
  BOOST_REQUIRE(write_file(backing / "hello.txt", "world"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_CHECK_EQUAL(read_file(mount / "hello.txt"), "world");
}

BOOST_AUTO_TEST_CASE(WriteGoesToBackingDir) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_REQUIRE(write_file(mount / "data.bin", "pg-raft"));
  BOOST_CHECK_EQUAL(read_file(backing / "data.bin"), "pg-raft");
}

// ── getattr / fgetattr ────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(GetattrMatchesBacking) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);
  BOOST_REQUIRE(write_file(backing / "f", "abcde"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  struct stat st_m{}, st_b{};
  BOOST_REQUIRE_EQUAL(stat((mount / "f").c_str(), &st_m), 0);
  BOOST_REQUIRE_EQUAL(stat((backing / "f").c_str(), &st_b), 0);
  BOOST_CHECK_EQUAL(st_m.st_size, st_b.st_size);
  BOOST_CHECK_EQUAL(st_m.st_mode, st_b.st_mode);
  // macOS FUSE does not preserve backing-dir inodes across the mount boundary.
  BOOST_CHECK_GE(fuse.ops().n_getattr.load(), 1u);
}

BOOST_AUTO_TEST_CASE(GetattrNonExistentReturnsENOENT) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  struct stat st{};
  BOOST_CHECK_EQUAL(stat((mount / "no_such_file").c_str(), &st), -1);
  BOOST_CHECK_EQUAL(errno, ENOENT);
}

BOOST_AUTO_TEST_CASE(FgetattrOpenFile) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);
  BOOST_REQUIRE(write_file(backing / "f", "hello"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  int fd = open((mount / "f").c_str(), O_RDONLY);
  BOOST_REQUIRE_GE(fd, 0);
  struct stat st{};
  BOOST_REQUIRE_EQUAL(fstat(fd, &st), 0);
  BOOST_CHECK_EQUAL(st.st_size, 5);
  close(fd);

  // Kernel may dispatch fstat to either fgetattr or getattr.
  BOOST_CHECK_GE(fuse.ops().n_fgetattr.load() + fuse.ops().n_getattr.load(), 1u);
}

// ── access ────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(AccessExistingFile) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);
  BOOST_REQUIRE(write_file(backing / "f", "x"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_CHECK_EQUAL(access((mount / "f").c_str(), F_OK), 0);
  BOOST_CHECK_EQUAL(access((mount / "f").c_str(), R_OK), 0);
  BOOST_CHECK_EQUAL(access((mount / "f").c_str(), W_OK), 0);
  BOOST_CHECK_GE(fuse.ops().n_access.load(), 3u);
}

BOOST_AUTO_TEST_CASE(AccessNonExistentReturnsENOENT) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_CHECK_EQUAL(access((mount / "missing").c_str(), F_OK), -1);
  BOOST_CHECK_EQUAL(errno, ENOENT);
}

// ── create / open / read / write ──────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(CreateFile) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  int fd = open((mount / "new.txt").c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  BOOST_REQUIRE_GE(fd, 0);
  close(fd);

  BOOST_CHECK(std::filesystem::exists(backing / "new.txt"));
  BOOST_CHECK_GE(fuse.ops().n_create.load(), 1u);
}

BOOST_AUTO_TEST_CASE(WriteAndReadBack) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  std::string payload(8192, 'X');
  BOOST_REQUIRE(write_file(mount / "wal.seg", payload));
  BOOST_CHECK_EQUAL(read_file(mount / "wal.seg"), payload);
  BOOST_CHECK_GE(fuse.ops().n_write.load(), 1u);
  // macOS VFS may serve reads from the page cache, bypassing op_read.
}

BOOST_AUTO_TEST_CASE(WriteAtOffset) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  int fd = open((mount / "sparse").c_str(), O_WRONLY | O_CREAT, 0644);
  BOOST_REQUIRE_GE(fd, 0);
  BOOST_CHECK_EQUAL(pwrite(fd, "hello", 5, 100), 5);
  close(fd);

  fd = open((backing / "sparse").c_str(), O_RDONLY);
  BOOST_REQUIRE_GE(fd, 0);
  char buf[5]{};
  BOOST_CHECK_EQUAL(pread(fd, buf, 5, 100), 5);
  close(fd);
  BOOST_CHECK_EQUAL(std::string(buf, 5), "hello");
}

// ── truncate ─────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(TruncateFile) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);
  BOOST_REQUIRE(write_file(backing / "f", "hello world"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_REQUIRE_EQUAL(truncate((mount / "f").c_str(), 5), 0);

  struct stat st{};
  BOOST_REQUIRE_EQUAL(stat((backing / "f").c_str(), &st), 0);
  BOOST_CHECK_EQUAL(st.st_size, 5);
  BOOST_CHECK_GE(fuse.ops().n_truncate.load(), 1u);
}

BOOST_AUTO_TEST_CASE(FtruncateFile) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);
  BOOST_REQUIRE(write_file(backing / "f", "hello world"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  int fd = open((mount / "f").c_str(), O_WRONLY);
  BOOST_REQUIRE_GE(fd, 0);
  BOOST_REQUIRE_EQUAL(ftruncate(fd, 3), 0);
  close(fd);

  struct stat st{};
  BOOST_REQUIRE_EQUAL(stat((backing / "f").c_str(), &st), 0);
  BOOST_CHECK_EQUAL(st.st_size, 3);
  BOOST_CHECK_GE(fuse.ops().n_ftruncate.load(), 1u);
}

// ── unlink ────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(UnlinkFile) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);
  BOOST_REQUIRE(write_file(backing / "victim", "data"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_REQUIRE_EQUAL(unlink((mount / "victim").c_str()), 0);
  BOOST_CHECK(!std::filesystem::exists(backing / "victim"));
  BOOST_CHECK_GE(fuse.ops().n_unlink.load(), 1u);
}

BOOST_AUTO_TEST_CASE(UnlinkNonExistentReturnsENOENT) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_CHECK_EQUAL(unlink((mount / "no_such_file").c_str()), -1);
  BOOST_CHECK_EQUAL(errno, ENOENT);
}

// ── rename ────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(RenameFile) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);
  BOOST_REQUIRE(write_file(backing / "old", "content"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_REQUIRE_EQUAL(rename((mount / "old").c_str(), (mount / "new").c_str()), 0);
  BOOST_CHECK(!std::filesystem::exists(backing / "old"));
  BOOST_CHECK_EQUAL(read_file(backing / "new"), "content");
  BOOST_CHECK_GE(fuse.ops().n_rename.load(), 1u);
}

// ── hard link ─────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(HardLink) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);
  BOOST_REQUIRE(write_file(backing / "orig", "data"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_REQUIRE_EQUAL(link((mount / "orig").c_str(), (mount / "linked").c_str()), 0);

  struct stat st_orig{}, st_link{};
  BOOST_REQUIRE_EQUAL(stat((backing / "orig").c_str(), &st_orig), 0);
  BOOST_REQUIRE_EQUAL(stat((backing / "linked").c_str(), &st_link), 0);
  BOOST_CHECK_EQUAL(st_orig.st_ino, st_link.st_ino);
  BOOST_CHECK_GE(fuse.ops().n_link.load(), 1u);
}

// ── symlink / readlink ────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(SymlinkAndReadlink) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);
  BOOST_REQUIRE(write_file(backing / "target", "data"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_REQUIRE_EQUAL(symlink("target", (mount / "link").c_str()), 0);

  char    buf[256]{};
  ssize_t n = readlink((mount / "link").c_str(), buf, sizeof(buf) - 1);
  BOOST_REQUIRE_GT(n, 0);
  BOOST_CHECK_EQUAL(std::string(buf, static_cast<size_t>(n)), "target");
  BOOST_CHECK(std::filesystem::is_symlink(backing / "link"));
  BOOST_CHECK_GE(fuse.ops().n_symlink.load(), 1u);
  BOOST_CHECK_GE(fuse.ops().n_readlink.load(), 1u);
}

// ── chmod ─────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(ChmodFile) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);
  BOOST_REQUIRE(write_file(backing / "f", "x"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_REQUIRE_EQUAL(chmod((mount / "f").c_str(), 0600), 0);

  struct stat st{};
  BOOST_REQUIRE_EQUAL(stat((backing / "f").c_str(), &st), 0);
  BOOST_CHECK_EQUAL(st.st_mode & 0777u, 0600u);
  BOOST_CHECK_GE(fuse.ops().n_chmod.load(), 1u);
}

// ── utimens ───────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(UtimensFile) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);
  BOOST_REQUIRE(write_file(backing / "f", "x"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  struct timespec ts[2];
  ts[0].tv_sec  = 1000000000;
  ts[0].tv_nsec = 0;
  ts[1].tv_sec  = 1000000000;
  ts[1].tv_nsec = 0;
  BOOST_REQUIRE_EQUAL(utimensat(AT_FDCWD, (mount / "f").c_str(), ts, 0), 0);

  struct stat st{};
  BOOST_REQUIRE_EQUAL(stat((backing / "f").c_str(), &st), 0);
  BOOST_CHECK_EQUAL(st.st_mtime, static_cast<time_t>(1000000000));
  BOOST_CHECK_GE(fuse.ops().n_utimens.load(), 1u);
}

// ── mkdir / rmdir ─────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(MkdirAndRmdir) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_REQUIRE_EQUAL(mkdir((mount / "subdir").c_str(), 0755), 0);
  BOOST_CHECK(std::filesystem::is_directory(backing / "subdir"));
  BOOST_CHECK_GE(fuse.ops().n_mkdir.load(), 1u);

  BOOST_REQUIRE_EQUAL(rmdir((mount / "subdir").c_str()), 0);
  BOOST_CHECK(!std::filesystem::exists(backing / "subdir"));
  BOOST_CHECK_GE(fuse.ops().n_rmdir.load(), 1u);
}

// ── readdir ───────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(ReadDirEmpty) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  int n = 0;
  for (auto const& e : std::filesystem::directory_iterator(mount))
    (void)e, ++n;
  BOOST_CHECK_EQUAL(n, 0);
}

BOOST_AUTO_TEST_CASE(ReadDirSmall) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  for (int i = 0; i < 5; ++i)
    BOOST_REQUIRE(write_file(backing / std::format("f{}", i), "x"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  int n = 0;
  for (auto const& e : std::filesystem::directory_iterator(mount))
    if (e.is_regular_file())
      ++n;
  BOOST_CHECK_EQUAL(n, 5);
}

// ── fsync ─────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(FsyncFile) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  int fd = open((mount / "f").c_str(), O_WRONLY | O_CREAT, 0644);
  BOOST_REQUIRE_GE(fd, 0);
  BOOST_REQUIRE_EQUAL(write(fd, "data", 4), 4);
  BOOST_REQUIRE_EQUAL(fsync(fd), 0);
  close(fd);

  BOOST_CHECK_GE(fuse.ops().n_fsync.load(), 1u);
  BOOST_CHECK_EQUAL(read_file(backing / "f"), "data");
}

// ── statfs ────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(StatfsMount) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  struct statvfs st{};
  BOOST_REQUIRE_EQUAL(statvfs(mount.c_str(), &st), 0);
  BOOST_CHECK_GT(st.f_blocks, 0u);
  BOOST_CHECK_GE(fuse.ops().n_statfs.load(), 1u);
}

// ── op counters ───────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(OpCountersIncrement) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  // write: create + write + flush + release
  write_file(mount / "f", "hello");
  BOOST_CHECK_GE(fuse.ops().n_create.load(), 1u);
  BOOST_CHECK_GE(fuse.ops().n_write.load(), 1u);
  BOOST_CHECK_GE(fuse.ops().n_flush.load(), 1u);
  BOOST_CHECK_GE(fuse.ops().n_release.load(), 1u);

  // read: open + release (op_read may be bypassed by macOS VFS page cache)
  read_file(mount / "f");
  BOOST_CHECK_GE(fuse.ops().n_open.load(), 1u);

  // stat → getattr
  struct stat st{};
  stat((mount / "f").c_str(), &st);
  BOOST_CHECK_GE(fuse.ops().n_getattr.load(), 1u);

  // readdir → opendir + readdir + releasedir
  for (auto const& e : std::filesystem::directory_iterator(mount))
    (void)e;
  BOOST_CHECK_GE(fuse.ops().n_opendir.load(), 1u);
  BOOST_CHECK_GE(fuse.ops().n_readdir.load(), 1u);
  BOOST_CHECK_GE(fuse.ops().n_releasedir.load(), 1u);
}

// ── xattr ─────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(SetGetXattr) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);
  BOOST_REQUIRE(write_file(backing / "f", "x"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_REQUIRE_EQUAL(setxattr((mount / "f").c_str(), "user.test", "pg-raft", 7, 0, 0), 0);

  char    buf[64]{};
  ssize_t n = getxattr((mount / "f").c_str(), "user.test", buf, sizeof(buf), 0, 0);
  BOOST_REQUIRE_GE(n, 0);
  BOOST_CHECK_EQUAL(std::string(buf, static_cast<size_t>(n)), "pg-raft");
  BOOST_CHECK_GE(fuse.ops().n_setxattr.load(), 1u);
  BOOST_CHECK_GE(fuse.ops().n_getxattr.load(), 1u);
}

BOOST_AUTO_TEST_CASE(ListXattr) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);
  BOOST_REQUIRE(write_file(backing / "f", "x"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_REQUIRE_EQUAL(setxattr((mount / "f").c_str(), "user.a", "1", 1, 0, 0), 0);
  BOOST_REQUIRE_EQUAL(setxattr((mount / "f").c_str(), "user.b", "2", 1, 0, 0), 0);

  char    buf[512]{};
  ssize_t n = listxattr((mount / "f").c_str(), buf, sizeof(buf), 0);
  BOOST_REQUIRE_GT(n, 0);
  BOOST_CHECK_GE(fuse.ops().n_listxattr.load(), 1u);
}

BOOST_AUTO_TEST_CASE(RemoveXattr) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);
  BOOST_REQUIRE(write_file(backing / "f", "x"));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_REQUIRE_EQUAL(setxattr((mount / "f").c_str(), "user.tmp", "val", 3, 0, 0), 0);
  BOOST_REQUIRE_EQUAL(removexattr((mount / "f").c_str(), "user.tmp", 0), 0);

  char    buf[64]{};
  ssize_t n = getxattr((mount / "f").c_str(), "user.tmp", buf, sizeof(buf), 0, 0);
  BOOST_CHECK_EQUAL(n, -1);
  BOOST_CHECK_EQUAL(errno, ENOATTR);
  BOOST_CHECK_GE(fuse.ops().n_removexattr.load(), 1u);
}

// ── subclass override ─────────────────────────────────────────────────────────

namespace {

struct InterceptOps : public WalFuseOps {
  using WalFuseOps::WalFuseOps;

  mutable std::mutex       mu;
  std::vector<std::string> intercepted_writes;
  std::vector<std::string> intercepted_fsyncs;

  int op_write(char const*            path,
               char const*            buf,
               size_t                 size,
               off_t                  offset,
               struct fuse_file_info* fi) override {
    {
      std::lock_guard lock(mu);
      intercepted_writes.emplace_back(path);
    }
    return WalFuseOps::op_write(path, buf, size, offset, fi);
  }

  int op_fsync(char const* path, int datasync, struct fuse_file_info* fi) override {
    {
      std::lock_guard lock(mu);
      intercepted_fsyncs.emplace_back(path);
    }
    return WalFuseOps::op_fsync(path, datasync, fi);
  }
};

}  // namespace

BOOST_AUTO_TEST_CASE(SubclassInterceptsWriteAndFsync) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<InterceptOps>(backing));
  BOOST_REQUIRE(fuse.start());

  int fd = open((mount / "wal.seg").c_str(), O_WRONLY | O_CREAT, 0644);
  BOOST_REQUIRE_GE(fd, 0);
  BOOST_REQUIRE_EQUAL(write(fd, "waldata", 7), 7);
  BOOST_REQUIRE_EQUAL(fsync(fd), 0);
  close(fd);

  auto const& ops = static_cast<InterceptOps const&>(fuse.ops());
  {
    std::lock_guard lock(ops.mu);
    BOOST_CHECK(!ops.intercepted_writes.empty());
    BOOST_CHECK(!ops.intercepted_fsyncs.empty());
  }
  // Base class must still write through to the backing dir.
  BOOST_CHECK_EQUAL(read_file(backing / "wal.seg"), "waldata");
}

BOOST_AUTO_TEST_CASE(SubclassCanSuppressWrite) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  struct DropWriteOps : public WalFuseOps {
    using WalFuseOps::WalFuseOps;
    int op_write(char const*, char const*, size_t size, off_t,
                 struct fuse_file_info*) override {
      return static_cast<int>(size);  // report success without writing
    }
  };

  WalFuse fuse(mount, std::make_unique<DropWriteOps>(backing));
  BOOST_REQUIRE(fuse.start());

  BOOST_REQUIRE(write_file(mount / "f", "should not appear"));

  // The file either doesn't exist in backing or is empty.
  BOOST_CHECK(!std::filesystem::exists(backing / "f") ||
              std::filesystem::file_size(backing / "f") == 0);
}

// ── concurrency ───────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(ConcurrentWrites) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  static constexpr int kThreads = 8;
  static constexpr int kFiles   = 16;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      for (int f = 0; f < kFiles; ++f) {
        auto name = std::format("t{}f{}", t, f);
        write_file(mount / name, name);
      }
    });
  }
  for (auto& th : threads)
    th.join();

  int count = 0;
  for (auto const& e : std::filesystem::directory_iterator(backing))
    if (e.is_regular_file())
      ++count;
  BOOST_CHECK_EQUAL(count, kThreads * kFiles);
}

BOOST_AUTO_TEST_CASE(ConcurrentReadWrite) {
  TmpDir tmp;
  auto   backing = tmp() / "backing";
  auto   mount   = tmp() / "mount";
  std::filesystem::create_directory(backing);
  std::filesystem::create_directory(mount);

  static constexpr int kFiles = 32;
  for (int i = 0; i < kFiles; ++i)
    BOOST_REQUIRE(write_file(backing / std::format("f{}", i), std::format("init{}", i)));

  WalFuse fuse(mount, std::make_unique<WalFuseOps>(backing));
  BOOST_REQUIRE(fuse.start());

  std::vector<std::thread> threads;
  for (int i = 0; i < kFiles / 2; ++i) {
    threads.emplace_back([&, i] {
      write_file(mount / std::format("f{}", i), std::format("new{}", i));
    });
    threads.emplace_back([&, i] {
      (void)read_file(mount / std::format("f{}", kFiles / 2 + i));
    });
  }
  for (auto& th : threads)
    th.join();

  for (int i = 0; i < kFiles / 2; ++i)
    BOOST_CHECK_EQUAL(read_file(backing / std::format("f{}", i)), std::format("new{}", i));
}

// ── existing PG integration tests ─────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(WalMovedToRaftDir) {
  if (!pg_available()) {
    BOOST_TEST_MESSAGE("PostgreSQL not found; skipping");
    return;
  }
  IoThreads threads{1};
  TmpDir    tmp;
  auto      data_dir = tmp() / "pgdata";
  auto      raft_dir = tmp() / "pgraft";
  std::filesystem::create_directory(raft_dir);

  PgInstance pg(kPgBin, data_dir, raft_dir);
  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));

  int initial = count_wal_segments(data_dir / "pg_wal");
  BOOST_REQUIRE_GT(initial, 0);

  BOOST_TEST(pg.create_raft_wal());
  BOOST_TEST(count_wal_segments(raft_dir / "pg_wal") == initial);
  BOOST_TEST(count_wal_segments(data_dir / "pg_wal") == 0);
}

BOOST_AUTO_TEST_CASE(ReadDirLargeDirectory) {
  TmpDir tmp;
  auto   backing_dir = tmp() / "backing";
  auto   mount_point = tmp() / "mount";
  std::filesystem::create_directory(backing_dir);
  std::filesystem::create_directory(mount_point);

  static constexpr int kFileCount = 4096;
  for (int i = 0; i < kFileCount; ++i) {
    auto path = backing_dir / std::format("file_{:04d}", i);
    int  fd   = open(path.c_str(), O_CREAT | O_WRONLY, 0644);
    BOOST_REQUIRE_GE(fd, 0);
    close(fd);
  }

  WalFuse fuse(mount_point, std::make_unique<WalFuseOps>(backing_dir));
  BOOST_REQUIRE(fuse.start());

  int n = 0;
  for (auto const& entry : std::filesystem::directory_iterator(mount_point)) {
    if (entry.is_regular_file())
      ++n;
  }

  BOOST_CHECK_EQUAL(n, kFileCount);
  log(INFO, "op_readdir {}", fuse.ops().n_readdir.load());
}

BOOST_AUTO_TEST_CASE(WalWritesToBackingDir) {
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

  // fuse declared before pg so pg destructs first (stops postgres before FUSE unmounts).
  WalFuse    fuse(data_dir / "pg_wal", std::make_unique<WalFuseOps>(raft_pg_wal));
  PgInstance pg(kPgBin, data_dir, raft_dir, {"-p", std::to_string(kPort)});

  BOOST_REQUIRE(pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}));
  BOOST_REQUIRE(pg.create_raft_wal());
  BOOST_REQUIRE(pg.write_config({{"listen_addresses", "'127.0.0.1'"}}));
  BOOST_REQUIRE(pg.write_hba({"host  all  all  127.0.0.1/32  trust"}));

  BOOST_REQUIRE_GT(count_wal_segments(raft_pg_wal), 0);
  BOOST_REQUIRE_EQUAL(count_wal_segments(data_dir / "pg_wal"), 0);

  BOOST_REQUIRE(fuse.start());
  BOOST_REQUIRE(pg.start());
  BOOST_REQUIRE(pg.wait_ready(kPort));

  // pg_wal's st_dev must differ from its parent when the FUSE mount is live.
  struct stat st_wal{}, st_data{};
  BOOST_REQUIRE_EQUAL(stat((data_dir / "pg_wal").c_str(), &st_wal), 0);
  BOOST_REQUIRE_EQUAL(stat(data_dir.c_str(), &st_data), 0);
  BOOST_CHECK_NE(st_wal.st_dev, st_data.st_dev);

  run_pgbench();

  int backing_count = count_wal_segments(raft_pg_wal);
  BOOST_CHECK_GT(backing_count, 0);
  // The FUSE view of pg_wal must show the same files as the backing directory.
  BOOST_CHECK_EQUAL(count_wal_segments(data_dir / "pg_wal"), backing_count);
}
