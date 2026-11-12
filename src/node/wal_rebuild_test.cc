#define BOOST_TEST_MODULE wal_rebuild
#include "src/node/wal_rebuild.h"

#include <fcntl.h>
#include <unistd.h>

#include <fstream>

#include "src/base/testing.h"
#include "src/base/wal_segment.h"

// rebuild_pg_wal: pg_wal ends up with raft/wal's committed WAL only.

namespace fs = std::filesystem;

namespace {

/// A 16 MiB segment file filled with |fill|.
void segment(fs::path const& dir, LogSeqNumber lsn, char fill) {
  fs::create_directories(dir);
  std::ofstream out(dir / filename(lsn), std::ios::binary | std::ios::trunc);
  auto          block = string(1U << 20U, fill);
  for (unsigned i = 0; i < kWalSegmentSize / block.size(); ++i)
    out << block;
}

/// The byte at |off| of a segment, and whether the file exists.
std::optional<char> byte_at(fs::path const& dir, LogSeqNumber lsn, uint64_t off) {
  std::ifstream in(dir / filename(lsn), std::ios::binary);
  if (!in)
    return std::nullopt;
  in.seekg(static_cast<std::streamoff>(off));
  char c = 0;
  in.get(c);
  return c;
}

LogSeqNumber seg(uint32_t tli, uint64_t n) {
  return {.timeline = tli, .lsn = n * kWalSegmentSize};
}

}  // namespace

BOOST_AUTO_TEST_CASE(rebuild) {
  TmpDir tmp;
  auto   pg   = tmp() / "pg_wal";
  auto   raft = tmp() / "raft_wal";

  // raft/wal: timeline 1 up to segment 3, then timeline 2 (segments 3 and 4), and its history.
  // 'R' marks raft's bytes, 'P' the deposed primary's own.
  for (auto s : {seg(1, 2), seg(1, 3), seg(2, 3), seg(2, 4)})
    segment(raft, s, 'R');
  std::ofstream(raft / "00000002.history") << "1\t0/3100000\tno recovery target specified\n";

  // pg_wal: the same, plus what was never committed: timeline 2 past the end (segment 5), a
  // timeline 3 the failed promotion started, a segment raft never had, and leftovers.
  for (auto s : {seg(1, 2), seg(1, 3), seg(2, 3), seg(2, 4), seg(2, 5), seg(3, 4), seg(1, 1)})
    segment(pg, s, 'P');
  std::ofstream(pg / "00000002.history") << "stale";
  std::ofstream(pg / "00000003.history") << "2\t0/4800000\tno recovery target specified\n";
  std::ofstream(pg / "000000010000000000000003.partial") << "x";
  std::ofstream(pg / "xlogtemp.123") << "x";
  std::ofstream(pg / ("." + filename(seg(1, 0)))) << "x";
  fs::create_directories(pg / "archive_status");

  // The committed WAL ends in timeline 2's segment 4, at offset 0x1000.
  auto end = LogSeqNumber{.timeline = 2, .lsn = 4 * kWalSegmentSize + 0x1000};
  BOOST_REQUIRE(rebuild_pg_wal(pg, raft, end).has_value());

  // Earlier timeline, and before the end: raft's copy.
  BOOST_TEST((byte_at(pg, seg(1, 2), 100) == 'R'));
  BOOST_TEST((byte_at(pg, seg(1, 3), kWalSegmentSize - 1) == 'R'));
  BOOST_TEST((byte_at(pg, seg(2, 3), 100) == 'R'));
  // The end's segment: raft's copy up to the end, zeros from it.
  BOOST_TEST((byte_at(pg, seg(2, 4), 0x0FFF) == 'R'));
  BOOST_TEST((byte_at(pg, seg(2, 4), 0x1000) == '\0'));
  BOOST_TEST((byte_at(pg, seg(2, 4), kWalSegmentSize - 1) == '\0'));
  BOOST_TEST(fs::file_size(pg / filename(seg(2, 4))) == kWalSegmentSize);
  // Never committed: gone.
  BOOST_TEST((!byte_at(pg, seg(2, 5), 0).has_value()));
  BOOST_TEST((!byte_at(pg, seg(3, 4), 0).has_value()));
  BOOST_TEST((!byte_at(pg, seg(1, 1), 0).has_value()));
  BOOST_TEST(!fs::exists(pg / "00000003.history"));
  // The committed history is raft's.
  std::ifstream h(pg / "00000002.history");
  auto          history = string{std::istreambuf_iterator<char>(h), {}};
  BOOST_TEST(history == "1\t0/3100000\tno recovery target specified\n");
  // Leftovers gone, directories kept.
  BOOST_TEST(!fs::exists(pg / "000000010000000000000003.partial"));
  BOOST_TEST(!fs::exists(pg / "xlogtemp.123"));
  BOOST_TEST(!fs::exists(pg / ("." + filename(seg(1, 0)))));
  BOOST_TEST(fs::is_directory(pg / "archive_status"));
  // raft/wal is untouched.
  BOOST_TEST((byte_at(raft, seg(2, 4), kWalSegmentSize - 1) == 'R'));
}

// At bootstrap the committed end is the backup's end: its segment is kept up to there.
BOOST_AUTO_TEST_CASE(backup_end) {
  TmpDir tmp;
  auto   pg   = tmp() / "pg_wal";
  auto   raft = tmp() / "raft_wal";
  segment(raft, seg(1, 2), 'R');
  segment(pg, seg(1, 2), 'P');
  segment(pg, seg(2, 2), 'P');  // the failed promotion's timeline 2

  BOOST_REQUIRE(
      rebuild_pg_wal(pg, raft, {.timeline = 1, .lsn = 2 * kWalSegmentSize + 0x120}).has_value());
  BOOST_TEST((byte_at(pg, seg(1, 2), 0x11F) == 'R'));
  BOOST_TEST((byte_at(pg, seg(1, 2), 0x120) == '\0'));
  BOOST_TEST((!byte_at(pg, seg(2, 2), 0).has_value()));
}
