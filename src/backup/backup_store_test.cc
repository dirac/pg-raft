#define BOOST_TEST_MODULE backup_store
#include <sys/stat.h>

#include <fstream>

#include "src/backup/dir_backup_store.h"
#include "src/base/testing.h"

// Backup names, the scheduling rules, and DirBackupStore: put/list/latest/get/remove, the
// atomicity of put, and the pending leases.

namespace fs = std::filesystem;

namespace {

LogSeqNumber lsn(uint32_t timeline, uint64_t l) {
  return LogSeqNumber{.timeline = timeline, .lsn = l};
}

/// A small data directory, with a nested file, an empty directory and a symbolic link.
fs::path make_data_dir(fs::path const& dir, string const& content) {
  fs::create_directories(dir / "base" / "1");
  fs::create_directories(dir / "pg_wal");
  std::ofstream(dir / "PG_VERSION") << "17\n";
  std::ofstream(dir / "base" / "1" / "1259") << content;
  fs::create_symlink("base/1/1259", dir / "link");
  return dir;
}

string read(fs::path const& file) {
  std::ifstream in(file);
  return string{std::istreambuf_iterator<char>(in), {}};
}

}  // namespace

// ── names ────────────────────────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(name_round_trip) {
  auto b = BackupInfo{.lsn = lsn(2, 0x5A1F3B0), .time = 1762956000, .node = RaftNodeId(3)};
  BOOST_TEST(b.name() == "00000002-0000000005A1F3B0-1762956000-3");

  auto p = parse_backup_name(b.name());
  BOOST_REQUIRE(p.has_value());
  BOOST_TEST((*p == b));

  auto big =
      BackupInfo{.lsn = lsn(0xFFFFFFFF, ~0ULL), .time = 9999999999, .node = RaftNodeId(65535)};
  BOOST_TEST((parse_backup_name(big.name()) == big));
}

BOOST_AUTO_TEST_CASE(name_rejects) {
  for (auto const* name : {
           "",
           "00000002-0000000005A1F3B0-1762956000",         // no node
           "00000002-0000000005A1F3B0-1762956000-",        // empty node
           "00000002-0000000005a1f3b0-1762956000-3",       // lower-case hex
           "0000002-0000000005A1F3B0-1762956000-3",        // short timeline
           "00000002-0000000005A1F3B0-176295600-3",        // short time
           "00000002-0000000005A1F3B0-1762956000-3-1",     // extra field
           "00000002-0000000005A1F3B0-1762956000-123456",  // node too long
           ".tmp-00000002-0000000005A1F3B0-1762956000-3",  // a partial copy
       })
    BOOST_TEST(!parse_backup_name(name).has_value(), name);
}

BOOST_AUTO_TEST_CASE(names_sort_like_backups) {
  auto backups = vector<BackupInfo>{
      {.lsn = lsn(1, 0x3000028), .time = 100, .node = RaftNodeId(0)},
      {.lsn = lsn(1, 0x9000000), .time = 50, .node = RaftNodeId(4)},
      {.lsn = lsn(2, 0x1000000), .time = 10, .node = RaftNodeId(1)},
      {.lsn = lsn(2, 0x1000000), .time = 20, .node = RaftNodeId(0)},
      {.lsn = lsn(2, 0x1000000), .time = 20, .node = RaftNodeId(2)},
  };
  for (size_t i = 1; i < backups.size(); ++i) {
    BOOST_TEST((backups.at(i - 1) < backups.at(i)));
    BOOST_TEST(backups.at(i - 1).name() < backups.at(i).name());
  }
}

BOOST_AUTO_TEST_CASE(keep_from_is_the_segment_start) {
  auto b = BackupInfo{.lsn = lsn(2, 0x5A1F3B0), .time = 1, .node = RaftNodeId(0)};
  BOOST_TEST((keep_from(b) == lsn(2, 0x5000000)));
  b.lsn = lsn(1, 0x3000000);
  BOOST_TEST((keep_from(b) == lsn(1, 0x3000000)));
}

// ── scheduling ───────────────────────────────────────────────────────────────────────────────────

// The example of docs/backup.md: 5 hours, 5 GB, 5 nodes.
BOOST_AUTO_TEST_CASE(staggered_deadlines) {
  auto const hour   = std::time_t{3600};
  auto const gb     = uint64_t{1} << 30U;
  auto const policy = BackupPolicy{.interval = std::chrono::hours{5}, .wal_size = 5 * gb};
  auto const t      = std::time_t{1762956000};
  auto const latest = BackupInfo{.lsn = lsn(2, 0x10000000), .time = t, .node = RaftNodeId(0)};
  auto const no_wal = latest.lsn;

  // Time: node i is due at t + 5h + i hours.
  for (unsigned i = 0; i < 5; ++i) {
    auto deadline = t + 5 * hour + static_cast<std::time_t>(i) * hour;
    BOOST_TEST(!backup_due(policy, i, 5, latest, deadline - 1, no_wal), i);
    BOOST_TEST(backup_due(policy, i, 5, latest, deadline, no_wal), i);
  }

  // WAL: node i is due after 5 GB + i GB.
  for (unsigned i = 0; i < 5; ++i) {
    auto threshold = 5 * gb + i * gb;
    BOOST_TEST(!backup_due(policy, i, 5, latest, t, lsn(2, latest.lsn.lsn + threshold - 1)), i);
    BOOST_TEST(backup_due(policy, i, 5, latest, t, lsn(2, latest.lsn.lsn + threshold)), i);
  }

  // No backup yet: init makes the first one.
  BOOST_TEST(!backup_due(policy, 0, 5, std::nullopt, t + 100 * hour, lsn(2, ~0ULL)));

  // WAL behind the backup (a node that hasn't applied up to it yet) counts as none.
  BOOST_TEST(!backup_due(policy, 0, 5, latest, t, lsn(2, 0x1000)));
}

BOOST_AUTO_TEST_CASE(live_leases_block) {
  auto const policy = BackupPolicy{.pending_timeout = std::chrono::minutes{2}};
  auto const now    = std::time_t{1762956000};
  auto const self   = RaftNodeId(1);

  BOOST_TEST(!pending_blocks(policy, {}, self, now));

  // Our own lease never blocks: it is left over from before a restart.
  BOOST_TEST(
      !pending_blocks(policy, {{.node = self, .started = now, .refreshed = now}}, self, now));

  // Another node's lease, refreshed a minute ago, blocks.
  BOOST_TEST(pending_blocks(
      policy, {{.node = RaftNodeId(3), .started = now - 3600, .refreshed = now - 60}}, self, now));

  // A lease not refreshed for 2 minutes is a node that died: it doesn't block.
  BOOST_TEST(!pending_blocks(
      policy, {{.node = RaftNodeId(3), .started = now - 60, .refreshed = now - 120}}, self, now));
}

// ── DirBackupStore ───────────────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(put_list_get_remove) {
  TmpDir tmp;
  auto   store = DirBackupStore{tmp() / "store"};

  BOOST_TEST(store.list().value().empty());
  BOOST_TEST(!store.latest().value().has_value());

  auto data1   = make_data_dir(tmp() / "data1", "one");
  auto pending = store.begin(RaftNodeId(0), 1000).value();
  auto b1      = store.put(pending, data1, lsn(1, 0x3000028)).value();
  BOOST_TEST(b1.name() == "00000001-0000000003000028-0000001000-0");
  BOOST_TEST(store.pending().value().empty());  // put() releases the lease

  auto data2 = make_data_dir(tmp() / "data2", "two");
  pending    = store.begin(RaftNodeId(2), 2000).value();
  auto b2    = store.put(pending, data2, lsn(2, 0x5A1F3B0)).value();

  BOOST_TEST((store.list().value() == vector<BackupInfo>{b1, b2}));
  BOOST_TEST((store.latest().value() == std::optional<BackupInfo>{b2}));

  // get: the files, the empty directory and the link come back.
  auto dest = tmp() / "restore" / "data";
  BOOST_REQUIRE(store.get(b2, dest).has_value());
  BOOST_TEST(read(dest / "base" / "1" / "1259") == "two");
  BOOST_TEST(read(dest / "PG_VERSION") == "17\n");
  BOOST_TEST(fs::is_directory(dest / "pg_wal"));
  BOOST_TEST(fs::is_symlink(dest / "link"));
  BOOST_TEST(fs::read_symlink(dest / "link") == fs::path{"base/1/1259"});

  // get refuses a directory that is not empty, and an unknown backup.
  BOOST_TEST(!store.get(b1, dest).has_value());
  auto missing = BackupInfo{.lsn = lsn(9, 9), .time = 9, .node = RaftNodeId(9)};
  BOOST_TEST(!store.get(missing, tmp() / "nowhere").has_value());

  // A second put under the same name is refused.
  pending = store.begin(RaftNodeId(2), 2000).value();
  BOOST_TEST(!store.put(pending, data2, lsn(2, 0x5A1F3B0)).has_value());
  BOOST_REQUIRE(store.abort(RaftNodeId(2)).has_value());

  BOOST_REQUIRE(store.remove(b2).has_value());
  BOOST_TEST((store.latest().value() == std::optional<BackupInfo>{b1}));
  BOOST_TEST(!store.remove(b2).has_value());
}

BOOST_AUTO_TEST_CASE(partial_copies_are_never_listed) {
  TmpDir tmp;
  auto   store = DirBackupStore{tmp() / "store"};

  // A crashed put() of node 3, and a stray file.
  auto partial = BackupInfo{.lsn = lsn(1, 0x6000000), .time = 500, .node = RaftNodeId(3)};
  make_data_dir(store.root() / (".tmp-" + partial.name()), "partial");
  std::ofstream(store.root() / "README") << "not a backup\n";

  BOOST_TEST(store.list().value().empty());

  // Another node's begin() leaves node 3's partial copy alone; node 3's own begin() removes it.
  BOOST_REQUIRE(store.begin(RaftNodeId(1), 600).has_value());
  BOOST_TEST(fs::exists(store.root() / (".tmp-" + partial.name())));
  BOOST_REQUIRE(store.begin(RaftNodeId(3), 700).has_value());
  BOOST_TEST(!fs::exists(store.root() / (".tmp-" + partial.name())));
}

BOOST_AUTO_TEST_CASE(leases) {
  TmpDir tmp;
  auto   store = DirBackupStore{tmp() / "store"};

  auto p1 = store.begin(RaftNodeId(1), 1000).value();
  auto p3 = store.begin(RaftNodeId(3), 1100).value();
  BOOST_TEST((p1 == PendingBackup{.node = RaftNodeId(1), .started = 1000, .refreshed = 1000}));

  BOOST_TEST((store.pending().value() == vector<PendingBackup>{p1, p3}));

  // refresh moves the refresh time, not the start time.
  BOOST_REQUIRE(store.refresh(p3, 1160).has_value());
  BOOST_TEST(p3.refreshed == 1160);
  BOOST_TEST((store.pending().value().back() ==
              PendingBackup{.node = RaftNodeId(3), .started = 1100, .refreshed = 1160}));

  // A node that restarts begins again: its old lease is replaced, not duplicated.
  auto p1b = store.begin(RaftNodeId(1), 5000).value();
  BOOST_TEST((store.pending().value() == vector<PendingBackup>{p1b, p3}));

  // abort releases the lease.
  BOOST_REQUIRE(store.abort(RaftNodeId(3)).has_value());
  BOOST_TEST((store.pending().value() == vector<PendingBackup>{p1b}));

  // The name of a stored backup carries the lease's start time.
  auto data = make_data_dir(tmp() / "data", "x");
  auto b    = store.put(p1b, data, lsn(1, 0x3000000)).value();
  BOOST_TEST(b.time == 5000);
  BOOST_TEST(INT(b.node) == 1);
  BOOST_TEST(store.pending().value().empty());
}
