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
//   [8..15]  xlp_pageaddr = |pageaddr|: the page's own LSN (a header with another is not walked)
template <typename C>
static void fill_wal_page(C* page, uint64_t pageaddr = 0) {
  memset(page, 0, kWalBlockSize);
  page[0]          = static_cast<C>(0x16);
  page[1]          = static_cast<C>(0xD1);  // xlp_magic = 0xD116 LE
  memcpy(page + 8, &pageaddr, 8);
  uint32_t tot_len = 24;
  memcpy(page + 40, &tot_len, 4);  // xl_tot_len of first XLogRecord
}

// Create and pre-allocate a 16 MiB file; return its fd (caller owns).
static int alloc_seg(std::filesystem::path const& path) {
  int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
  if (fd < 0) {
    log(ERROR, "Unable to create the file {} error={}", path, strerror(errno));
    return -1;
  }
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
    unsigned char* buf;
    size_t         size;
  };

  std::vector<Pub> publishes;
  std::vector<Syn> fsyncs;

  std::vector<std::pair<uint32_t, std::string>> timelines;

  void async_timeline(uint32_t timeline, std::string const& content) override {
    timelines.emplace_back(timeline, content);
  }

  void async_publish(shared_ptr<WalSegment>, off_t off, size_t sz) override {
    publishes.push_back({off, sz});
  }
  void async_fsync(shared_ptr<WalSegment>            seg,
                   off_t                             offset,
                   size_t                            size,
                   std::function<void(Result<void>)> cb) override {
    fsyncs.push_back({seg->mapping() + offset, size});
    cb(ok());
  }
};

// Create a WalFuseSegment backed by a fresh 16 MiB file at dir/filename(lsn).
static shared_ptr<WalFuseSegment> make_fuse_segment(std::filesystem::path const& dir,
                                                    LogSeqNumber                 lsn,
                                                    RecordingTracker&            tracker) {
  int fd = alloc_seg(dir / filename(lsn));
  if (fd < 0)
    return nullptr;
  ::close(fd);
  auto seg = make_shared<WalSegment>(make_shared<WalSegmentCache>(), dir, lsn);
  if (!seg->open())
    return nullptr;
  return make_shared<WalFuseSegment>(tracker, std::move(seg));
}

// ── WalFuseSegment ────────────────────────────────────────────────────────────

std::string_view wal_data();  // wal_data.cc: real WAL captured from PostgreSQL

// The write boundaries don't matter: real WAL written in odd chunks (cutting page headers and
// xl_tot_len) is stored byte for byte and walked as far as when written at once. A standby's
// walreceiver writes at any byte; before, a cut page header tripped an assertion in
// WalState::update and the bytes past the walk were dropped.
// A page header only partly stored (a write ended 4 bytes into the page) when the segment's last
// handle closes: the next WalFuseSegment re-reads the file, and must not count the half header as
// walked (published). Before, it did, and PostgreSQL's full write of the page then published only
// the bytes that differed: from page + 4. The page's magic never reached raft, and no standby
// could replay past it.
BOOST_AUTO_TEST_CASE(partial_page_header_across_reopen, *boost::unit_test::timeout(60)) {
  TmpDir           tmp;
  RecordingTracker tracker;
  auto const       lsn = LogSeqNumber{1, 0};
  auto             a   = make_fuse_segment(tmp(), lsn, tracker);
  BOOST_REQUIRE(a);

  std::vector<unsigned char> page0(kWalBlockSize), page1(kWalBlockSize, 0);
  fill_wal_page(page0.data());
  uint32_t fill = kWalBlockSize - 40;  // one record filling page 0: the walk goes on to page 1
  memcpy(page0.data() + 40, &fill, 4);
  uint16_t magic = 0xD116, info = 0x0005;
  uint64_t addr  = kWalBlockSize;
  uint32_t len   = 24;
  memcpy(page1.data(), &magic, 2);
  memcpy(page1.data() + 2, &info, 2);
  memcpy(page1.data() + 8, &addr, 8);  // xlp_pageaddr: page 1's LSN
  memcpy(page1.data() + 24, &len, 4);  // a 24-byte record after the short header

  a->write(page0.data(), page0.size(), 0);
  a->write(page1.data(), 4, kWalBlockSize);  // magic and info only
  a.reset();                                 // the last handle closes

  auto map = make_shared<WalSegment>(make_shared<WalSegmentCache>(), tmp(), lsn);
  BOOST_REQUIRE(map->open().has_value());
  auto b = make_shared<WalFuseSegment>(tracker, std::move(map));
  b->write(page1.data(), page1.size(), kWalBlockSize);

  // Some publish covers page 1's first byte.
  bool covered = false;
  for (auto const& p : tracker.publishes)
    covered = covered || (p.offset <= static_cast<off_t>(kWalBlockSize) &&
                          p.offset + static_cast<off_t>(p.size) > static_cast<off_t>(kWalBlockSize));
  BOOST_TEST(covered);
}

BOOST_AUTO_TEST_CASE(wal_segment_any_write_boundaries, *boost::unit_test::timeout(60)) {
  TmpDir           tmp;
  RecordingTracker whole_tracker, chunked_tracker;
  std::filesystem::create_directories(tmp() / "a");
  std::filesystem::create_directories(tmp() / "b");
  // The captured segment's own LSN: its first page header's xlp_pageaddr.
  uint64_t seg_lsn = 0;
  memcpy(&seg_lsn, wal_data().data() + 8, 8);
  auto whole   = make_fuse_segment(tmp() / "a", LogSeqNumber{1, seg_lsn}, whole_tracker);
  auto chunked = make_fuse_segment(tmp() / "b", LogSeqNumber{1, seg_lsn}, chunked_tracker);
  BOOST_REQUIRE(whole);
  BOOST_REQUIRE(chunked);

  auto const* data = reinterpret_cast<unsigned char const*>(wal_data().data());
  auto const  size = std::min<size_t>(wal_data().size(), kWalSegmentSize);
  whole->write(data, size, 0);

  static constexpr std::array<size_t, 7> kChunks = {1, 3, 5, 17, 23, 1000, 8192};
  size_t                                 i       = 0;
  for (size_t off = 0; off < size; ++i) {
    auto n = std::min(kChunks.at(i % kChunks.size()), size - off);
    chunked->write(data + off, n, static_cast<off_t>(off));
    off += n;
  }

  // Stored byte for byte.
  BOOST_TEST(std::memcmp(chunked->mapping(), data, size) == 0);

  // Walked as far, and contiguously.
  auto walked = [](RecordingTracker const& t) {
    off_t end = 0;
    for (auto const& p : t.publishes) {
      BOOST_TEST(p.offset == end);
      end = p.offset + static_cast<off_t>(p.size);
    }
    return end;
  };
  auto const end = walked(whole_tracker);
  BOOST_TEST(end > 0);
  BOOST_TEST(walked(chunked_tracker) == end);

  // fsync covers everything stored.
  BOOST_REQUIRE_EQUAL(chunked->fsync(), 0);
  BOOST_REQUIRE_EQUAL(chunked_tracker.fsyncs.size(), 1u);
  BOOST_TEST(chunked_tracker.fsyncs.at(0).size == size);
}

BOOST_AUTO_TEST_CASE(wal_segment_zero_page_no_useful_data, *boost::unit_test::timeout(60)) {
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

BOOST_AUTO_TEST_CASE(wal_segment_write_one_record, *boost::unit_test::timeout(60)) {
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

BOOST_AUTO_TEST_CASE(wal_segment_fsync_calls_tracker, *boost::unit_test::timeout(60)) {
  TmpDir           tmp;
  RecordingTracker tracker;

  auto seg = make_fuse_segment(tmp(), LogSeqNumber{1, 0}, tracker);
  BOOST_REQUIRE(seg);

  unsigned char page[kWalBlockSize]{};
  fill_wal_page(page);
  seg->write(page, kWalBlockSize, 0);

  BOOST_REQUIRE_EQUAL(seg->fsync(), 0);

  BOOST_REQUIRE_EQUAL(tracker.fsyncs.size(), 1u);
  // Everything written is synced: the walked 64 bytes and the rest of the page.
  BOOST_CHECK_EQUAL(tracker.fsyncs[0].size, kWalBlockSize);
}

BOOST_AUTO_TEST_CASE(wal_segment_fsync_without_write_asks_tracker, *boost::unit_test::timeout(60)) {
  TmpDir           tmp;
  RecordingTracker tracker;

  auto seg = make_fuse_segment(tmp(), LogSeqNumber{1, 0}, tracker);
  BOOST_REQUIRE(seg);

  // No write: nothing new here, but the tracker is still asked (an empty range). It may hold
  // writes of the segment published through a handle since closed, and the flush is the barrier
  // that acknowledges them.
  BOOST_REQUIRE_EQUAL(seg->fsync(), 0);
  BOOST_REQUIRE_EQUAL(tracker.fsyncs.size(), 1u);
  BOOST_CHECK_EQUAL(tracker.fsyncs[0].size, 0u);
}

BOOST_AUTO_TEST_CASE(wal_segment_read, *boost::unit_test::timeout(60)) {
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

BOOST_AUTO_TEST_CASE(wal_segment_read_at_offset, *boost::unit_test::timeout(60)) {
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

BOOST_AUTO_TEST_CASE(wal_segment_lsn_accessor, *boost::unit_test::timeout(60)) {
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

  ~WalOpsFixture() { std::filesystem::remove_all(backing); }
};

BOOST_FIXTURE_TEST_CASE(non_wal_file_passes_through,
                        WalOpsFixture,
                        *boost::unit_test::timeout(60)) {
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

BOOST_FIXTURE_TEST_CASE(wal_file_open_increases_counter,
                        WalOpsFixture,
                        *boost::unit_test::timeout(60)) {
  WalFuseOps ops(tracker, backing);

  struct fuse_file_info fi{};
  fi.flags = O_RDWR;
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi), 0);
  BOOST_CHECK_EQUAL(ops.n_open.load(), 1u);

  ops.op_release(kWalPath, &fi);
}

BOOST_FIXTURE_TEST_CASE(wal_file_write_publishes, WalOpsFixture, *boost::unit_test::timeout(60)) {
  WalFuseOps ops(tracker, backing);

  struct fuse_file_info fi{};
  fi.flags = O_RDWR;
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi), 0);

  char page[kWalBlockSize]{};
  fill_wal_page(page, kWalSegmentSize);  // segment 1 (kWalPath)
  ops.op_write(kWalPath, page, kWalBlockSize, 0, &fi);

  BOOST_REQUIRE_EQUAL(tracker.publishes.size(), 1u);
  BOOST_CHECK_EQUAL(tracker.publishes[0].offset, 0);
  BOOST_CHECK_EQUAL(tracker.publishes[0].size, 64u);
  BOOST_CHECK_EQUAL(ops.n_write.load(), 1u);

  ops.op_release(kWalPath, &fi);
}

BOOST_FIXTURE_TEST_CASE(wal_file_fsync_calls_tracker,
                        WalOpsFixture,
                        *boost::unit_test::timeout(60)) {
  WalFuseOps ops(tracker, backing);

  struct fuse_file_info fi{};
  fi.flags = O_RDWR;
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi), 0);

  char page[kWalBlockSize]{};
  fill_wal_page(page, kWalSegmentSize);  // segment 1 (kWalPath)
  ops.op_write(kWalPath, page, kWalBlockSize, 0, &fi);

  BOOST_REQUIRE_EQUAL(ops.op_fsync(kWalPath, 0, &fi), 0);
  BOOST_REQUIRE_EQUAL(tracker.fsyncs.size(), 1u);
  // Everything written is synced: the walked 64 bytes and the rest of the page.
  BOOST_CHECK_EQUAL(tracker.fsyncs[0].size, kWalBlockSize);
  BOOST_CHECK_EQUAL(ops.n_fsync.load(), 1u);

  ops.op_release(kWalPath, &fi);
}

BOOST_FIXTURE_TEST_CASE(wal_file_flush_is_noop, WalOpsFixture, *boost::unit_test::timeout(60)) {
  WalFuseOps ops(tracker, backing);

  struct fuse_file_info fi{};
  fi.flags = O_RDWR;
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi), 0);
  BOOST_REQUIRE_EQUAL(ops.op_flush(kWalPath, &fi), 0);
  BOOST_CHECK_EQUAL(ops.n_flush.load(), 1u);
  BOOST_CHECK(tracker.publishes.empty());

  ops.op_release(kWalPath, &fi);
}

BOOST_FIXTURE_TEST_CASE(wal_file_read, WalOpsFixture, *boost::unit_test::timeout(60)) {
  WalFuseOps ops(tracker, backing);

  struct fuse_file_info fi{};
  fi.flags = O_RDWR;
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi), 0);

  char page[kWalBlockSize]{};
  fill_wal_page(page, kWalSegmentSize);  // segment 1 (kWalPath)
  ops.op_write(kWalPath, page, kWalBlockSize, 0, &fi);

  char buf[2]{};
  ops.op_read(kWalPath, buf, 2, 0, &fi);
  BOOST_CHECK_EQUAL(static_cast<uint8_t>(buf[0]), 0x16u);
  BOOST_CHECK_EQUAL(static_cast<uint8_t>(buf[1]), 0xD1u);
  BOOST_CHECK_EQUAL(ops.n_read.load(), 1u);

  ops.op_release(kWalPath, &fi);
}

BOOST_FIXTURE_TEST_CASE(wal_file_release_counts_and_succeeds,
                        WalOpsFixture,
                        *boost::unit_test::timeout(60)) {
  WalFuseOps ops(tracker, backing);

  struct fuse_file_info fi{};
  fi.flags = O_RDWR;
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi), 0);
  BOOST_REQUIRE_EQUAL(ops.op_release(kWalPath, &fi), 0);
  BOOST_CHECK_EQUAL(ops.n_release.load(), 1u);
}

BOOST_FIXTURE_TEST_CASE(two_opens_share_segment, WalOpsFixture, *boost::unit_test::timeout(60)) {
  WalFuseOps ops(tracker, backing);

  struct fuse_file_info fi1{}, fi2{};
  fi1.flags = fi2.flags = O_RDWR;
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi1), 0);
  BOOST_REQUIRE_EQUAL(ops.op_open(kWalPath, &fi2), 0);
  BOOST_CHECK_EQUAL(ops.n_open.load(), 2u);

  // Write through first handle, read back through second — shared mmap.
  char page[kWalBlockSize]{};
  fill_wal_page(page, kWalSegmentSize);  // segment 1 (kWalPath)
  ops.op_write(kWalPath, page, kWalBlockSize, 0, &fi1);

  char buf[2]{};
  ops.op_read(kWalPath, buf, 2, 0, &fi2);
  BOOST_CHECK_EQUAL(static_cast<uint8_t>(buf[0]), 0x16u);

  // One publish regardless of how many handles are open.
  BOOST_CHECK_EQUAL(tracker.publishes.size(), 1u);

  ops.op_release(kWalPath, &fi1);
  ops.op_release(kWalPath, &fi2);
}

// ── Timeline history files ────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(history_file_names) {
  BOOST_TEST(history_filename(2) == "00000002.history");
  BOOST_TEST(history_filename(0x1A) == "0000001A.history");

  BOOST_TEST(history_timeline("00000002.history").value() == 2u);
  BOOST_TEST(history_timeline("/0000001A.history").value() == 0x1Au);

  for (auto const* name : {"00000002.history.tmp",
                           "0000002.history",
                           "0000000a.history",
                           "000000010000000000000001",
                           "xlogtemp.123",
                           "000000010000000000000001.partial",
                           "00000002.backup"})
    BOOST_TEST(!history_timeline(name).has_value(), name);
}

BOOST_AUTO_TEST_CASE(history_file_write_read) {
  TmpDir tmp;
  auto   content = std::string{"1\t0/5000000\tno recovery target specified\n"};

  BOOST_REQUIRE(write_history_file(tmp(), 2, content).has_value());
  BOOST_TEST(read_history_file(tmp(), 2).value() == content);
  BOOST_TEST(!read_history_file(tmp(), 3).has_value());

  // Rewriting replaces it, and leaves no temporary file behind.
  BOOST_REQUIRE(write_history_file(tmp(), 2, content + "x").has_value());
  BOOST_TEST(read_history_file(tmp(), 2).value() == content + "x");
  for (auto const& entry : std::filesystem::directory_iterator(tmp()))
    BOOST_TEST(!entry.path().filename().string().ends_with(".tmp"), entry.path());
}

// PostgreSQL installs a history file on promotion by renaming xlogtemp.<pid>: the tracker gets the
// timeline and the content. Other renames don't reach it.
BOOST_FIXTURE_TEST_CASE(history_rename_calls_tracker,
                        WalOpsFixture,
                        *boost::unit_test::timeout(60)) {
  WalFuseOps ops(tracker, backing);
  auto       content = std::string{"1\t0/3000028\tno recovery target specified\n"};

  std::ofstream(backing / "xlogtemp.123") << content;
  BOOST_REQUIRE_EQUAL(ops.op_rename("/xlogtemp.123", "/00000002.history"), 0);
  BOOST_REQUIRE_EQUAL(tracker.timelines.size(), 1u);
  BOOST_TEST(tracker.timelines.at(0).first == 2u);
  BOOST_TEST(tracker.timelines.at(0).second == content);
  BOOST_TEST(std::filesystem::exists(backing / "00000002.history"));

  // A segment installed by rename, and a rename to .partial, are not timelines.
  std::ofstream(backing / "xlogtemp.124") << "x";
  BOOST_REQUIRE_EQUAL(ops.op_rename("/xlogtemp.124", "/000000020000000000000003"), 0);
  BOOST_REQUIRE_EQUAL(
      ops.op_rename("/000000020000000000000003", "/000000020000000000000003.partial"), 0);
  BOOST_TEST(tracker.timelines.size() == 1u);

  // A failed rename reports the error and calls nothing.
  BOOST_TEST(ops.op_rename("/missing", "/00000003.history") == -ENOENT);
  BOOST_TEST(tracker.timelines.size() == 1u);
}
