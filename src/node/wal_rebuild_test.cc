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

  BOOST_REQUIRE(rebuild_pg_wal(pg, raft).has_value());

  // Earlier timeline, and before the end: raft's copy.
  BOOST_TEST((byte_at(pg, seg(1, 2), 100) == 'R'));
  BOOST_TEST((byte_at(pg, seg(1, 3), kWalSegmentSize - 1) == 'R'));
  BOOST_TEST((byte_at(pg, seg(2, 3), 100) == 'R'));
  // The last segment: raft's copy whole (raft/wal holds committed WAL only: nothing is cut).
  BOOST_TEST((byte_at(pg, seg(2, 4), 0x1000) == 'R'));
  BOOST_TEST((byte_at(pg, seg(2, 4), kWalSegmentSize - 1) == 'R'));
  BOOST_TEST(fs::file_size(pg / filename(seg(2, 4))) == kWalSegmentSize);
  // Never committed: gone.
  BOOST_TEST((!byte_at(pg, seg(2, 5), 0).has_value()));
  BOOST_TEST((!byte_at(pg, seg(3, 4), 0).has_value()));
  // Older than everything raft/wal holds (compacted history, e.g. a restored backup's): kept.
  BOOST_TEST((byte_at(pg, seg(1, 1), 0) == 'P'));
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

// A newer timeline than the node's applied state knows of (its last checkpoint's, behind
// raft/wal after a crash): its committed segments are kept (cutting them made the standby replay
// the old timeline's partial last record, and PANIC).
BOOST_AUTO_TEST_CASE(newer_timeline_kept) {
  TmpDir tmp;
  auto   pg   = tmp() / "pg_wal";
  auto   raft = tmp() / "raft_wal";
  segment(raft, seg(1, 2), 'A');
  segment(raft, seg(2, 2), 'B');
  std::ofstream(raft / "00000002.history") << "1\t0/2000000\tno recovery target specified\n";
  segment(pg, seg(1, 2), 'P');
  segment(pg, seg(2, 2), 'P');
  BOOST_REQUIRE(rebuild_pg_wal(pg, raft).has_value());
  BOOST_TEST((byte_at(pg, seg(1, 2), 0) == 'A'));
  BOOST_TEST((byte_at(pg, seg(2, 2), 0) == 'B'));
}

// The segment holding a timeline's start point: raft's copy has zeros before it (PostgreSQL copies
// the parent's bytes in through an xlogtemp file, which FUSE doesn't replicate). The rebuilt
// segment gets them from raft's copy of the parent's segment, else from pg_wal's own copy.
BOOST_AUTO_TEST_CASE(timeline_start_prefix) {
  auto const start = uint64_t{3} * kWalSegmentSize + 0x100000;  // timeline 2 starts at 0/3100000
  auto       zero_prefix = [&](fs::path const& dir) {
    // raft's copy of timeline 2's segment 3: zeros up to the start point.
    std::fstream f(dir / filename(seg(2, 3)), std::ios::binary | std::ios::in | std::ios::out);
    auto         zeros = string(start - 3 * kWalSegmentSize, '\0');
    f.write(zeros.data(), static_cast<std::streamsize>(zeros.size()));
  };

  {  // The parent's segment in raft/wal.
    TmpDir tmp;
    auto   pg   = tmp() / "pg_wal";
    auto   raft = tmp() / "raft_wal";
    segment(raft, seg(1, 3), 'A');
    segment(raft, seg(2, 3), 'R');
    zero_prefix(raft);
    segment(raft, seg(2, 4), 'R');
    std::ofstream(raft / "00000002.history") << "1\t0/3100000\tno recovery target specified\n";
    segment(pg, seg(2, 3), 'P');
    segment(pg, seg(2, 4), 'P');
    BOOST_REQUIRE(rebuild_pg_wal(pg, raft).has_value());
    BOOST_TEST((byte_at(pg, seg(2, 3), 0) == 'A'));
    BOOST_TEST((byte_at(pg, seg(2, 3), start - 3 * kWalSegmentSize - 1) == 'A'));
    BOOST_TEST((byte_at(pg, seg(2, 3), start - 3 * kWalSegmentSize) == 'R'));
  }
  {  // The parent's segment compacted away in raft/wal: pg_wal's own copy of the prefix.
    TmpDir tmp;
    auto   pg   = tmp() / "pg_wal";
    auto   raft = tmp() / "raft_wal";
    segment(raft, seg(2, 3), 'R');
    zero_prefix(raft);
    segment(raft, seg(2, 4), 'R');
    std::ofstream(raft / "00000002.history") << "1\t0/3100000\tno recovery target specified\n";
    segment(pg, seg(2, 3), 'P');
    BOOST_REQUIRE(rebuild_pg_wal(pg, raft).has_value());
    BOOST_TEST((byte_at(pg, seg(2, 3), 0) == 'P'));
    BOOST_TEST((byte_at(pg, seg(2, 3), start - 3 * kWalSegmentSize - 1) == 'P'));
    BOOST_TEST((byte_at(pg, seg(2, 3), start - 3 * kWalSegmentSize) == 'R'));
  }
}

// Two timelines starting in one segment (promotions in quick succession): timeline 3's copy of the
// segment needs timeline 1's bytes up to timeline 2's start and timeline 2's up to its own, and
// raft's copy of timeline 2's segment has zeros before timeline 2's start.
BOOST_AUTO_TEST_CASE(two_timeline_starts_in_one_segment) {
  TmpDir     tmp;
  auto       pg    = tmp() / "pg_wal";
  auto       raft  = tmp() / "raft_wal";
  auto const base  = uint64_t{3} * kWalSegmentSize;
  auto const s2    = base + 0x100000;  // timeline 2 starts at 0/3100000
  auto const s3    = base + 0x200000;  // timeline 3 at 0/3200000
  auto       zeros = [](fs::path const& dir, LogSeqNumber lsn, uint64_t n) {
    std::fstream f(dir / filename(lsn), std::ios::binary | std::ios::in | std::ios::out);
    auto         z = string(n, '\0');
    f.write(z.data(), static_cast<std::streamsize>(z.size()));
  };
  segment(raft, seg(1, 3), 'A');
  segment(raft, seg(2, 3), 'B');
  zeros(raft, seg(2, 3), s2 - base);
  segment(raft, seg(3, 3), 'C');
  zeros(raft, seg(3, 3), s3 - base);
  std::ofstream(raft / "00000002.history") << "1\t0/3100000\tno recovery target specified\n";
  std::ofstream(raft / "00000003.history") << "1\t0/3100000\tno recovery target specified\n\n"
                                           << "2\t0/3200000\tno recovery target specified\n";
  segment(pg, seg(3, 3), 'P');

  BOOST_REQUIRE(rebuild_pg_wal(pg, raft).has_value());
  BOOST_TEST((byte_at(pg, seg(3, 3), 0) == 'A'));
  BOOST_TEST((byte_at(pg, seg(3, 3), s2 - base - 1) == 'A'));
  BOOST_TEST((byte_at(pg, seg(3, 3), s2 - base) == 'B'));
  BOOST_TEST((byte_at(pg, seg(3, 3), s3 - base - 1) == 'B'));
  BOOST_TEST((byte_at(pg, seg(3, 3), s3 - base) == 'C'));
}

// A restored backup's WAL from its redo point, which raft has compacted away (raft/wal starts
// later): kept, on a timeline raft knows; an old segment on a timeline raft has no history of is
// removed.
BOOST_AUTO_TEST_CASE(compacted_history_kept) {
  TmpDir tmp;
  auto   pg   = tmp() / "pg_wal";
  auto   raft = tmp() / "raft_wal";
  segment(raft, seg(2, 6), 'R');
  std::ofstream(raft / "00000002.history") << "1\t0/1000000\tno recovery target specified\n";
  segment(pg, seg(2, 4), 'P');  // the backup's WAL: before raft's first segment
  segment(pg, seg(2, 5), 'P');
  segment(pg, seg(5, 4), 'P');  // a timeline raft knows nothing of
  segment(pg, seg(2, 7), 'P');  // past raft's first, not in raft: removed
  BOOST_REQUIRE(rebuild_pg_wal(pg, raft).has_value());
  BOOST_TEST((byte_at(pg, seg(2, 4), 0) == 'P'));
  BOOST_TEST((byte_at(pg, seg(2, 5), 0) == 'P'));
  BOOST_TEST((!byte_at(pg, seg(5, 4), 0).has_value()));
  BOOST_TEST((!byte_at(pg, seg(2, 7), 0).has_value()));
}
