#define BOOST_TEST_MODULE wal_fuse_ops

#include "src/wal_fuse/wal_fuse_ops.h"

#include "src/base/testing.h"

// ── helpers ───────────────────────────────────────────────────────────────────

// FUSE paths for a WAL segment (TLI=1, LOGID=0, LOGSEG=1) and a plain file.
static constexpr char kWalPath[]  = "/000000010000000000000001";
static constexpr char kTextPath[] = "/readme.txt";

// Minimal first-page layout (40-byte XLogLongPageHeader + one 24-byte record):
//   [0..1]   xlp_magic = 0xD116 (little-endian)
//   [2..15]  zeros (xlp_info, xlp_tli)
//   [16..19] xlp_rem_len = 0 (no continuation from prior segment)
//   [20..39] zeros (xlp_seg_size, xlp_xlog_blcksz, padding)
//   [40..43] xl_tot_len = 24 (one minimal XLogRecord, already 8-byte aligned)
//   [44..]   zeros
//
// WalState::update() consumes header(40) + record(24) = 64 bytes and then
// hits zeros at offset 64, which round_up(xl_tot_len=0) = 0, ending the loop.
// async_publish is therefore called with offset=0, size=64.
template <typename C>
static void fill_wal_page(C* page) {
  memset(page, 0, kWalBlockSize);
  page[0]          = 0x16;
  page[1]          = 0xD1;  // xlp_magic = 0xD116 LE
  uint32_t tot_len = 24;
  memcpy(page + 40, &tot_len, 4);  // xl_tot_len of first XLogRecord
}

// Create and pre-allocate a 16 MiB file; return its fd (caller owns).
static int alloc_seg(std::filesystem::path const& path) {
  int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0600);
  if (fd < 0)
    return -1;
  if (::ftruncate(fd, kWalSegmentSize) != 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

struct RecordingTracker : public WalTracker {
  struct Pub {
    off_t  offset;
    size_t size;
  };
  struct Syn {
    unsigned char*  buf;
    size_t size;
  };

  std::vector<Pub> publishes;
  std::vector<Syn> fsyncs;

  void async_publish(shared_ptr<WalSegment>, off_t off, size_t sz) override {
    publishes.push_back({off, sz});
  }
  void async_fsync(std::promise<int> p,
                   shared_ptr<WalSegment>,
                   unsigned char* b,
                   size_t         s) override {
    fsyncs.push_back({b, s});
    p.set_value(0);
  }
};

// Create a WalFuseSegment backed by a fresh 16 MiB file at dir/filename(lsn).
static shared_ptr<WalFuseSegment> make_fuse_segment(std::filesystem::path const& dir,
                                                    LogSeqNumber                  lsn,
                                                    RecordingTracker&             tracker) {
  int fd = alloc_seg(dir / filename(lsn));
  if (fd < 0)
    return nullptr;
  ::close(fd);
  auto seg = make_shared<WalSegment>(dir, lsn);
  if (!seg->open())
    return nullptr;
  return make_shared<WalFuseSegment>(tracker, std::move(seg));
}

// ── WalFuseSegment ────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(wal_segment_zero_page_no_useful_data, * boost::unit_test::timeout(60)) {
  TmpDir           tmp;
  RecordingTracker tracker;

  auto seg = make_fuse_segment(tmp(), LogSeqNumber{1, 0}, tracker);
  BOOST_REQUIRE(seg);

  unsigned char page[kWalBlockSize]{};  // all zeros — xlp_magic == 0
  seg->write(page, kWalBlockSize, 0);

  // No valid WAL bytes: any publish must carry size 0.
  for (auto const& p : tracker.publishes)
    BOOST_CHECK_EQUAL(p.size, 0u);
}

BOOST_AUTO_TEST_CASE(wal_segment_write_one_record, * boost::unit_test::timeout(60)) {
  TmpDir           tmp;
  RecordingTracker tracker;

  auto seg = make_fuse_segment(tmp(), LogSeqNumber{1, 0}, tracker);
  BOOST_REQUIRE(seg);

  unsigned char page[kWalBlockSize]{};
  fill_wal_page(page);
  seg->write(page, kWalBlockSize, 0);

  // header(40) + record(24) = 64 bytes from offset 0.
  BOOST_REQUIRE_EQUAL(tracker.publishes.size(), 1u);
  BOOST_CHECK_EQUAL(tracker.publishes[0].offset, 0);
  BOOST_CHECK_EQUAL(tracker.publishes[0].size, 64u);
}

BOOST_AUTO_TEST_CASE(wal_segment_fsync_calls_tracker, * boost::unit_test::timeout(60)) {
  TmpDir           tmp;
  RecordingTracker tracker;

  auto seg = make_fuse_segment(tmp(), LogSeqNumber{1, 0}, tracker);
  BOOST_REQUIRE(seg);

  unsigned char page[kWalBlockSize]{};
  fill_wal_page(page);
  seg->write(page, kWalBlockSize, 0);

  BOOST_REQUIRE_EQUAL(seg->fsync(), 0);

  BOOST_REQUIRE_EQUAL(tracker.fsyncs.size(), 1u);
  // Size is rounded up to the OS page size.
  BOOST_CHECK_EQUAL(tracker.fsyncs[0].size % static_cast<size_t>(getpagesize()), 0u);
  BOOST_CHECK_GT(tracker.fsyncs[0].size, 0u);
}

BOOST_AUTO_TEST_CASE(wal_segment_fsync_without_write_is_noop, * boost::unit_test::timeout(60)) {
  TmpDir           tmp;
  RecordingTracker tracker;

  auto seg = make_fuse_segment(tmp(), LogSeqNumber{1, 0}, tracker);
  BOOST_REQUIRE(seg);

  // No write → next_==synced_==0 → dirty range size rounds to 0.
  BOOST_REQUIRE_EQUAL(seg->fsync(), 0);
  BOOST_CHECK_EQUAL(tracker.fsyncs.size(), 0u);
}

BOOST_AUTO_TEST_CASE(wal_segment_read, * boost::unit_test::timeout(60)) {
  TmpDir           tmp;
  RecordingTracker tracker;

  auto seg = make_fuse_segment(tmp(), LogSeqNumber{1, 0}, tracker);
  BOOST_REQUIRE(seg);

  unsigned char page[kWalBlockSize]{};
  fill_wal_page(page);
  seg->write(page, kWalBlockSize, 0);

  unsigned char buf[2]{};
  seg->read(buf, 2, 0);
  BOOST_CHECK_EQUAL(static_cast<uint8_t>(buf[0]), 0x16u);
  BOOST_CHECK_EQUAL(static_cast<uint8_t>(buf[1]), 0xD1u);
}

BOOST_AUTO_TEST_CASE(wal_segment_read_at_offset, * boost::unit_test::timeout(60)) {
  TmpDir           tmp;
  RecordingTracker tracker;

  auto seg = make_fuse_segment(tmp(), LogSeqNumber{1, 0}, tracker);
  BOOST_REQUIRE(seg);

  unsigned char page[kWalBlockSize]{};
  fill_wal_page(page);
  seg->write(page, kWalBlockSize, 0);

  // xl_tot_len is at offset 40 in the mmap.
  unsigned char buf[4]{};
  seg->read(buf, 4, 40);
  uint32_t tot_len{};
  memcpy(&tot_len, buf, 4);
  BOOST_CHECK_EQUAL(tot_len, 24u);
}

BOOST_AUTO_TEST_CASE(wal_segment_lsn_accessor, * boost::unit_test::timeout(60)) {
  TmpDir           tmp;
  RecordingTracker tracker;

  LogSeqNumber lsn{3, 42};
  auto         seg = make_fuse_segment(tmp(), lsn, tracker);
  BOOST_REQUIRE(seg);
  BOOST_CHECK_EQUAL(seg->lsn().timeline, 3u);
  BOOST_CHECK_EQUAL(seg->lsn().lsn, 42u);
}

// ── WalFuseOps ────────────────────────────────────────────────────────────────

// Shared fixture: backing dir + one pre-allocated WAL segment file.
struct WalOpsFixture {
  TmpDir                tmp;
  RecordingTracker      tracker;
  std::filesystem::path backing;

  WalOpsFixture() : backing(tmp() / "backing") {
    std::filesystem::create_directory(backing);
    // kWalPath + 1 strips the leading '/'.
    int fd = alloc_seg(backing / (kWalPath + 1));
    BOOST_REQUIRE_GE(fd, 0);
    ::close(fd);
  }
};

BOOST_FIXTURE_TEST_CASE(non_wal_file_passes_through, WalOpsFixture, * boost::unit_test::timeout(60)) {
  int tfd = ::open((backing / (kTextPath + 1)).c_str(), O_WRONLY | O_CREAT, 0644);
  BOOST_REQUIRE_GE(tfd, 0);
  ::close(tfd);

  WalFuseOps ops(tracker, backing);

  struct fuse_file_info fi{};
  fi.flags = O_RDONLY;
  BOOST_REQUIRE_EQUAL(ops.op_open(kTextPath, &fi), 0);

  // FuseOps base-class passthrough: n_open is incremented there, no WAL tracking.
  BOOST_CHECK_EQUAL(ops.n_open.load(), 1u);
  BOOST_CHECK(tracker.publishes.empty());

  ::close(static_cast<int>(fi.fh));
}

BOOST_FIXTURE_TEST_CASE(wal_file_open_increases_counter, WalOpsFixture, * boost::unit_test::timeout(60)) {
  WalFuseOps ops(tracker, backing);

  struct fuse_file_info fi{};
  fi.flags = O_RDWR;
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi), 0);
  BOOST_CHECK_EQUAL(ops.n_open.load(), 1u);

  ops.op_release(kWalPath, &fi);
}

BOOST_FIXTURE_TEST_CASE(wal_file_write_publishes, WalOpsFixture, * boost::unit_test::timeout(60)) {
  WalFuseOps ops(tracker, backing);

  struct fuse_file_info fi{};
  fi.flags = O_RDWR;
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi), 0);

  char page[kWalBlockSize]{};
  fill_wal_page(page);
  ops.op_write(kWalPath, page, kWalBlockSize, 0, &fi);

  BOOST_REQUIRE_EQUAL(tracker.publishes.size(), 1u);
  BOOST_CHECK_EQUAL(tracker.publishes[0].offset, 0);
  BOOST_CHECK_EQUAL(tracker.publishes[0].size, 64u);
  BOOST_CHECK_EQUAL(ops.n_write.load(), 1u);

  ops.op_release(kWalPath, &fi);
}

BOOST_FIXTURE_TEST_CASE(wal_file_fsync_calls_tracker, WalOpsFixture, * boost::unit_test::timeout(60)) {
  WalFuseOps ops(tracker, backing);

  struct fuse_file_info fi{};
  fi.flags = O_RDWR;
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi), 0);

  char page[kWalBlockSize]{};
  fill_wal_page(page);
  ops.op_write(kWalPath, page, kWalBlockSize, 0, &fi);

  BOOST_REQUIRE_EQUAL(ops.op_fsync(kWalPath, 0, &fi), 0);
  BOOST_REQUIRE_EQUAL(tracker.fsyncs.size(), 1u);
  BOOST_CHECK_EQUAL(tracker.fsyncs[0].size % static_cast<size_t>(getpagesize()), 0u);
  BOOST_CHECK_EQUAL(ops.n_fsync.load(), 1u);

  ops.op_release(kWalPath, &fi);
}

BOOST_FIXTURE_TEST_CASE(wal_file_flush_is_noop, WalOpsFixture, * boost::unit_test::timeout(60)) {
  WalFuseOps ops(tracker, backing);

  struct fuse_file_info fi{};
  fi.flags = O_RDWR;
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi), 0);
  BOOST_REQUIRE_EQUAL(ops.op_flush(kWalPath, &fi), 0);
  BOOST_CHECK_EQUAL(ops.n_flush.load(), 1u);
  BOOST_CHECK(tracker.publishes.empty());

  ops.op_release(kWalPath, &fi);
}

BOOST_FIXTURE_TEST_CASE(wal_file_read, WalOpsFixture, * boost::unit_test::timeout(60)) {
  WalFuseOps ops(tracker, backing);

  struct fuse_file_info fi{};
  fi.flags = O_RDWR;
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi), 0);

  char page[kWalBlockSize]{};
  fill_wal_page(page);
  ops.op_write(kWalPath, page, kWalBlockSize, 0, &fi);

  char buf[2]{};
  ops.op_read(kWalPath, buf, 2, 0, &fi);
  BOOST_CHECK_EQUAL(static_cast<uint8_t>(buf[0]), 0x16u);
  BOOST_CHECK_EQUAL(static_cast<uint8_t>(buf[1]), 0xD1u);
  BOOST_CHECK_EQUAL(ops.n_read.load(), 1u);

  ops.op_release(kWalPath, &fi);
}

BOOST_FIXTURE_TEST_CASE(wal_file_release_counts_and_succeeds, WalOpsFixture, * boost::unit_test::timeout(60)) {
  WalFuseOps ops(tracker, backing);

  struct fuse_file_info fi{};
  fi.flags = O_RDWR;
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi), 0);
  BOOST_REQUIRE_EQUAL(ops.op_release(kWalPath, &fi), 0);
  BOOST_CHECK_EQUAL(ops.n_release.load(), 1u);
}

BOOST_FIXTURE_TEST_CASE(two_opens_share_segment, WalOpsFixture, * boost::unit_test::timeout(60)) {
  WalFuseOps ops(tracker, backing);

  struct fuse_file_info fi1{}, fi2{};
  fi1.flags = fi2.flags = O_RDWR;
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi1), 0);
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi2), 0);
  BOOST_CHECK_EQUAL(ops.n_open.load(), 2u);

  // Write through first handle, read back through second — shared mmap.
  char page[kWalBlockSize]{};
  fill_wal_page(page);
  ops.op_write(kWalPath, page, kWalBlockSize, 0, &fi1);

  char buf[2]{};
  ops.op_read(kWalPath, buf, 2, 0, &fi2);
  BOOST_CHECK_EQUAL(static_cast<uint8_t>(buf[0]), 0x16u);

  // One publish regardless of how many handles are open.
  BOOST_CHECK_EQUAL(tracker.publishes.size(), 1u);

  ops.op_release(kWalPath, &fi1);
  ops.op_release(kWalPath, &fi2);
}
