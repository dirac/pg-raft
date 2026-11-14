#define BOOST_TEST_MODULE raft_log
#include "src/raft/raft_log.h"

#include "src/base/testing.h"

// Fault injection (every LMDB call fails in turn), and the log operations: appends, entries,
// snapshot and compaction, install_snapshot, term and vote.

namespace {

int err = EIO;

/// Fails the |invocation|th LMDB call when |fail| is set, and checks that every txn and cursor is
/// closed.
struct TestRaftLog : public RaftLog {
  std::filesystem::path term_db() const override { return tmp / std::format("{0}-term.lmdb", idx); }

  /// Logs database file
  std::filesystem::path logs_db() const override { return tmp / std::format("{0}-logs.lmdb", idx); }

  int env_set_mapsize(MDB_env* env, size_t size) override {
    if (fail && invocation-- == 0)
      return err;
    return RaftLog::env_set_mapsize(env, size >> 10U);
  }

  int env_create(MDB_env** env) override {
    if (fail && invocation-- == 0)
      return err;

    return RaftLog::env_create(env);
  }
  int env_open(MDB_env* env, char const* filename, unsigned int flags) override {
    if (fail && invocation-- == 0)
      return err;

    return RaftLog::env_open(env, filename, flags);
  }
  int txn_wbegin(MDB_env* env, MDB_txn** txn) override {
    if (fail && invocation-- == 0)
      return err;

    BOOST_TEST(open_txn == nullptr);

    BOOST_REQUIRE(RaftLog::txn_wbegin(env, txn) == 0);
    open_txn = *txn;
    return 0;
  }
  int txn_rbegin(MDB_env* env, MDB_txn** txn) const override {
    if (fail && invocation-- == 0)
      return err;
    BOOST_TEST(open_txn == nullptr);

    BOOST_REQUIRE(RaftLog::txn_rbegin(env, txn) == 0);
    open_txn = *txn;
    return 0;
  }
  int txn_commit_impl(MDB_txn* txn) const override {
    BOOST_TEST(open_cursor == nullptr);
    BOOST_TEST(open_txn == txn);

    open_txn = nullptr;

    if (fail && invocation-- == 0) {
      RaftLog::txn_abort_impl(txn);
      return err;
    }

    return RaftLog::txn_commit_impl(txn);
  }
  int dbi_open(MDB_txn* txn, char const* name, MDB_dbi* dbi) override {
    if (fail && invocation-- == 0)
      return err;

    return RaftLog::dbi_open(txn, name, dbi);
  }
  int get(MDB_txn* txn, MDB_dbi dbi, MDB_val* key, MDB_val* val) const override {
    if (fail && invocation-- == 0)
      return err;

    return RaftLog::get(txn, dbi, key, val);
  }
  int put(MDB_txn* txn, MDB_dbi dbi, MDB_val* key, MDB_val* val) override {
    if (fail && invocation-- == 0)
      return err;

    return RaftLog::put(txn, dbi, key, val);
  }
  int del(MDB_txn* txn, MDB_dbi dbi, MDB_val* key) override {
    if (fail && invocation-- == 0)
      return err;

    return RaftLog::del(txn, dbi, key);
  }
  int cursor_open(MDB_txn* txn, MDB_dbi dbi, MDB_cursor** cursor) const override {
    if (fail && invocation-- == 0)
      return err;

    BOOST_REQUIRE(RaftLog::cursor_open(txn, dbi, cursor) == 0);
    open_cursor = *cursor;
    return 0;
  }
  int cursor_get(MDB_cursor* cursor, MDB_val* key, MDB_val* val, MDB_cursor_op op) const override {
    if (fail && invocation-- == 0)
      return err;

    return RaftLog::cursor_get(cursor, key, val, op);
  }

  int cursor_del(MDB_cursor* cursor) override {
    if (fail && invocation-- == 0)
      return err;
    return RaftLog::cursor_del(cursor);
  }

  void txn_abort_impl(MDB_txn* txn) const override {
    BOOST_TEST(open_cursor == nullptr);
    BOOST_TEST(open_txn == txn);
    open_txn = nullptr;

    return RaftLog::txn_abort_impl(txn);
  }
  void cursor_close_impl(MDB_cursor* cursor) const override {
    BOOST_TEST(open_cursor == cursor);
    open_cursor = nullptr;

    return RaftLog::cursor_close_impl(cursor);
  }

  mutable MDB_txn*      open_txn    = nullptr;
  mutable MDB_cursor*   open_cursor = nullptr;
  mutable int           invocation{0};
  mutable bool          fail{false};
  std::filesystem::path tmp;
  unsigned const        idx = file_idx++;

  explicit TestRaftLog(std::filesystem::path const& path)
      : RaftLog(path, /*sync_commits=*/false, /*no_sync=*/true), tmp(path) {}

  static unsigned file_idx;
};

unsigned TestRaftLog::file_idx = 0;

struct BaseTest {
  BaseTest() : log(dir()) {}

  /// Runs test_fn with the 1st, 2nd, ... LMDB call failing until it succeeds: every failure is the
  /// injected error and leaves no txn or cursor open.
  template <typename T>
  Result<T> exec(std::function<Result<T>(RaftLog&)> const& test_fn, int max = 50) {
    for (int i = 0; i < max; ++i) {
      log.fail       = true;
      log.invocation = i;

      BOOST_REQUIRE(log.open_txn == nullptr);
      BOOST_REQUIRE(log.open_cursor == nullptr);

      auto r   = test_fn(log);
      log.fail = false;

      BOOST_TEST(log.open_txn == nullptr);
      BOOST_TEST(log.open_cursor == nullptr);

      if (r.has_value())
        return r;

      BOOST_TEST(r.error() == system_error_code(err));
    }
    BOOST_REQUIRE(false);
    return system_error_code(err);
  }

  TmpDir      dir;
  TestRaftLog log;
};

struct Test : public BaseTest {
  Test() {
    BOOST_REQUIRE(!log.open_term_db().has_error());
    BOOST_REQUIRE(!log.open_logs_db().has_error());
  }
};

Entry entry(std::time_t t, uint64_t term, off_t offset = 0) {
  return Entry{
      .time = t,
      .term = RaftTerm{term},
      .cmd =
          WalCommand{
              .flushed_lsn = LogSeqNumber{.timeline = 1, .lsn = u8(offset)},
              .segment     = LogSeqNumber{.timeline = 1, .lsn = 0},
              .command     = WriteOp{.offset = offset, .size = 8},
          },
  };
}

vector<Entry> entries(std::time_t from, size_t n, uint64_t term) {
  vector<Entry> result;
  for (size_t i = 0; i < n; ++i)
    result.emplace_back(entry(from + static_cast<std::time_t>(i), term, static_cast<off_t>(i * 8)));
  return result;
}

}  // namespace

BOOST_FIXTURE_TEST_CASE(open_term_db, BaseTest) {
  auto r = exec<RaftTerm>([](RaftLog& log) -> Result<RaftTerm> { return log.open_term_db(); });
  BOOST_TEST(!r.has_error());
}

BOOST_FIXTURE_TEST_CASE(open_logs_db, BaseTest) {
  auto r = exec<void>([](RaftLog& log) -> Result<void> { return log.open_logs_db(); });
  BOOST_TEST(!r.has_error());
}

BOOST_FIXTURE_TEST_CASE(last_entry, Test) {
  using R = std::tuple<RaftTerm, RaftIndex>;
  // Empty
  {
    auto r = exec<R>([](RaftLog& log) -> Result<R> { return log.last_entry(); });
    BOOST_TEST(RaftIndex(0) == std::get<RaftIndex>(r.value()));
    BOOST_TEST(RaftTerm(0) == std::get<RaftTerm>(r.value()));
  }

  auto r = log.append_entries(RaftIndex(0), RaftTerm(0), entries(1, 2, 1), true);
  BOOST_TEST(r.value());

  // Non-empty
  {
    auto r = exec<R>([](RaftLog& log) -> Result<R> { return log.last_entry(); });
    BOOST_TEST(RaftIndex(2) == std::get<RaftIndex>(r.value()));
    BOOST_TEST(RaftTerm(1) == std::get<RaftTerm>(r.value()));
  }
}

BOOST_FIXTURE_TEST_CASE(append_entries, Test) {
  // The leader appends after its last entry only.
  BOOST_TEST(log.append_entries(RaftIndex(0), RaftTerm(0), entries(1, 5, 1), false).value());
  BOOST_TEST(!log.append_entries(RaftIndex(3), RaftTerm(1), entries(6, 1, 1), false).value());
  BOOST_TEST(log.append_entries(RaftIndex(5), RaftTerm(1), entries(6, 1, 1), false).value());

  // A follower replaces the entries after a matching one.
  BOOST_TEST(!log.append_entries(RaftIndex(3), RaftTerm(2), entries(7, 1, 2), true).value());
  BOOST_TEST(log.append_entries(RaftIndex(3), RaftTerm(1), entries(7, 2, 2), true).value());

  auto last = log.last_entry().value();
  BOOST_TEST(std::get<RaftIndex>(last) == RaftIndex(5));
  BOOST_TEST(std::get<RaftTerm>(last) == RaftTerm(2));

  BOOST_TEST(log.get_term(RaftIndex(3)).value() == std::optional<RaftTerm>{RaftTerm(1)});
  BOOST_TEST(log.get_term(RaftIndex(4)).value() == std::optional<RaftTerm>{RaftTerm(2)});
  BOOST_TEST(!log.get_term(RaftIndex(6)).value().has_value());

  // With failures, nothing is left open and the append succeeds in the end.
  auto r = exec<bool>([](RaftLog& log) -> Result<bool> {
    return log.append_entries(RaftIndex(5), RaftTerm(2), entries(9, 3, 2), true);
  });
  BOOST_TEST(r.value());
  BOOST_TEST(std::get<RaftIndex>(log.last_entry().value()) == RaftIndex(8));
}

BOOST_FIXTURE_TEST_CASE(entries_from, Test) {
  auto e = entries(1, 10, 1);
  BOOST_TEST(log.append_entries(RaftIndex(0), RaftTerm(0), e, false).value());

  auto r = log.entries_from(RaftIndex(3), 4).value();
  BOOST_REQUIRE(r.has_value());
  BOOST_TEST(r->size() == 4U);
  BOOST_TEST((r->front() == e.at(2)));
  BOOST_TEST((r->back() == e.at(5)));

  // Past the end: empty.
  BOOST_TEST(log.entries_from(RaftIndex(11), 4).value()->empty());

  auto [idx, latest] = log.latest_entries(3).value();
  BOOST_TEST(idx == RaftIndex(10));
  BOOST_TEST(latest.size() == 3U);
  BOOST_TEST((latest.front() == e.at(9)));  // The last first.
  BOOST_TEST((latest.back() == e.at(7)));
}

BOOST_FIXTURE_TEST_CASE(snapshot_and_compaction, Test) {
  BOOST_TEST((log.load_snapshot().value() == RaftState{}));

  BOOST_TEST(log.append_entries(RaftIndex(0), RaftTerm(0), entries(1, 100, 1), false).value());

  auto state = RaftState{
      .log_idx     = RaftIndex(90),
      .timestamp   = 90,
      .flushed_lsn = LogSeqNumber{.timeline = 1, .lsn = 712},
      .wal_end     = LogSeqNumber{.timeline = 1, .lsn = 8192},
  };
  auto r = exec<void>(
      [&state](RaftLog& log) -> Result<void> { return log.save_snapshot(state, /*log_size=*/20); },
      /*max=*/500);
  BOOST_TEST(r.has_value());
  BOOST_TEST((log.load_snapshot().value() == state));

  // The entries before log_idx - log_size are gone.
  BOOST_TEST(std::get<RaftIndex>(log.first_entry().value()) == RaftIndex(70));
  BOOST_TEST(!log.entries_from(RaftIndex(69), 1).value().has_value());
  BOOST_TEST(std::get<RaftIndex>(log.last_entry().value()) == RaftIndex(100));

  // The state survives a reopen.
  log.close();
  BOOST_REQUIRE(log.open_term_db().has_value());
  BOOST_REQUIRE(log.open_logs_db().has_value());
  BOOST_TEST((log.load_snapshot().value() == state));
}

BOOST_FIXTURE_TEST_CASE(install_snapshot, Test) {
  BOOST_TEST(log.append_entries(RaftIndex(0), RaftTerm(0), entries(1, 10, 1), false).value());

  auto e     = entries(100, 5, 3);
  auto state = RaftState{.log_idx = RaftIndex(500), .timestamp = 99, .needs_backup = true};

  auto r = exec<void>([&e, &state](RaftLog& log) -> Result<void> {
    return log.install_snapshot(RaftIndex(500), e, state, state);
  });
  BOOST_TEST(r.has_value());

  BOOST_TEST(std::get<RaftIndex>(log.first_entry().value()) == RaftIndex(501));
  BOOST_TEST(std::get<RaftIndex>(log.last_entry().value()) == RaftIndex(505));
  BOOST_TEST((log.entries_from(RaftIndex(501), 10).value().value() == e));
  BOOST_TEST((log.load_snapshot().value() == state));

  // The leader can go on from the last entry.
  BOOST_TEST(log.append_entries(RaftIndex(505), RaftTerm(3), entries(105, 1, 3), true).value());
}

// wal_end is the highest end of the WAL bytes applied (WriteOps and FlushOps): a rewrite of earlier
// offsets does not lower it, and other commands do not move it.
BOOST_AUTO_TEST_CASE(state_wal_end) {
  auto const seg1 = LogSeqNumber{.timeline = 1, .lsn = 0x1000000};
  auto const seg2 = LogSeqNumber{.timeline = 1, .lsn = 0x2000000};

  auto entry = [](std::optional<LogSeqNumber> seg, auto op) {
    return Entry{.time = 1, .term = RaftTerm(1), .cmd = WalCommand{.segment = seg, .command = op}};
  };

  auto state = RaftState{};
  state.apply(RaftIndex(1), entry(seg1, WriteOp{.offset = 0, .size = 16384}));
  BOOST_TEST((state.wal_end == LogSeqNumber{.timeline = 1, .lsn = 0x1000000 + 16384}));

  // A rewrite of the tail page, published again.
  state.apply(RaftIndex(2), entry(seg1, WriteOp{.offset = 8192, .size = 100}));
  BOOST_TEST((state.wal_end == LogSeqNumber{.timeline = 1, .lsn = 0x1000000 + 16384}));

  // A flush over earlier bytes, a tick and a write without a segment don't move it.
  state.apply(RaftIndex(3), entry(seg1, FlushOp{.offset = 0, .size = 16384}));
  state.apply(RaftIndex(4), entry(std::nullopt, std::monostate{}));
  state.apply(RaftIndex(5), entry(std::nullopt, WriteOp{.offset = 0, .size = 1U << 20}));
  BOOST_TEST((state.wal_end == LogSeqNumber{.timeline = 1, .lsn = 0x1000000 + 16384}));

  // The next segment.
  state.apply(RaftIndex(6), entry(seg2, WriteOp{.offset = 0, .size = 8192}));
  BOOST_TEST((state.wal_end == LogSeqNumber{.timeline = 1, .lsn = 0x2000000 + 8192}));

  // A flush that carries new bytes (the node's writes travel with the flush) moves it.
  state.apply(RaftIndex(7), entry(seg2, FlushOp{.offset = 8192, .size = 300}));
  BOOST_TEST((state.wal_end == LogSeqNumber{.timeline = 1, .lsn = 0x2000000 + 8492}));

  // A new timeline is later, even at a lower LSN.
  state.apply(
      RaftIndex(8),
      entry(LogSeqNumber{.timeline = 2, .lsn = 0x1000000}, WriteOp{.offset = 0, .size = 40}));
  BOOST_TEST((state.wal_end == LogSeqNumber{.timeline = 2, .lsn = 0x1000000 + 40}));
}

// compact() deletes the entries and saves the state and the boundary in one transaction: when any
// LMDB call of it fails, nothing of it happened; once it succeeds, all of it did.
BOOST_FIXTURE_TEST_CASE(compact_is_atomic, Test) {
  BOOST_TEST(log.append_entries(RaftIndex(0), RaftTerm(0), entries(1, 100, 1), false).value());
  BOOST_TEST((log.load_boundary().value() == RaftState{}));  // never compacted

  auto const before = log.load_snapshot().value();
  auto const state  = RaftState{.log_idx       = RaftIndex(90),
                                .timestamp     = 90,
                                .first_idx     = RaftIndex(40),
                                .first_segment = LogSeqNumber{.timeline = 1, .lsn = 0x3000000}};
  auto const bound  = RaftState{.log_idx = RaftIndex(39), .timestamp = 39};

  for (int i = 0;; ++i) {
    BOOST_REQUIRE(i < 500);  // about two LMDB calls per deleted entry, then the puts and the commit
    log.fail       = true;
    log.invocation = i;
    auto r         = log.compact(RaftIndex(40), state, bound);
    log.fail       = false;
    BOOST_TEST(log.open_txn == nullptr);
    BOOST_TEST(log.open_cursor == nullptr);

    if (!r) {
      // Nothing happened.
      BOOST_TEST(std::get<RaftIndex>(log.first_entry().value()) == RaftIndex(0));
      BOOST_TEST((log.load_snapshot().value() == before));
      BOOST_TEST((log.load_boundary().value() == RaftState{}));
      continue;
    }
    break;
  }

  // All of it happened.
  BOOST_TEST(std::get<RaftIndex>(log.first_entry().value()) == RaftIndex(40));
  BOOST_TEST(!log.entries_from(RaftIndex(39), 1).value().has_value());
  BOOST_TEST(std::get<RaftIndex>(log.last_entry().value()) == RaftIndex(100));
  BOOST_TEST((log.load_snapshot().value() == state));
  BOOST_TEST((log.load_boundary().value() == bound));

  // They survive a reopen.
  log.close();
  BOOST_REQUIRE(log.open_term_db().has_value());
  BOOST_REQUIRE(log.open_logs_db().has_value());
  BOOST_TEST((log.load_snapshot().value() == state));
  BOOST_TEST((log.load_boundary().value() == bound));
}

BOOST_FIXTURE_TEST_CASE(term_and_vote, Test) {
  BOOST_TEST(log.current_term().value() == RaftTerm(0));

  BOOST_TEST(log.set_current_term(RaftTerm(3)).value() == RaftTerm(3));
  // The term only goes up.
  BOOST_TEST(log.set_current_term(RaftTerm(2)).value() == RaftTerm(3));

  // A vote is for the current term, once.
  BOOST_TEST(!log.vote(RaftTerm(2), RaftNodeId(1)).value());
  BOOST_TEST(log.vote(RaftTerm(3), RaftNodeId(1)).value());
  BOOST_TEST(log.vote(RaftTerm(3), RaftNodeId(1)).value());
  BOOST_TEST(!log.vote(RaftTerm(3), RaftNodeId(2)).value());

  // A new term clears the vote.
  BOOST_TEST(log.set_current_term(RaftTerm(4)).value() == RaftTerm(4));
  BOOST_TEST(log.vote(RaftTerm(4), RaftNodeId(2)).value());

  auto r =
      exec<bool>([](RaftLog& log) -> Result<bool> { return log.vote(RaftTerm(4), RaftNodeId(2)); });
  BOOST_TEST(r.value());
}

using test_types = boost::mpl::list<int16_t, int32_t, int64_t, uint16_t, uint32_t, uint64_t>;

BOOST_AUTO_TEST_CASE_TEMPLATE(mem, T, test_types) {
  set<T> elems{
      std::numeric_limits<T>::min(),
      std::numeric_limits<T>::lowest(),
      static_cast<T>(-42),
      0,
      static_cast<T>(42),
      std::numeric_limits<T>::max(),
  };

  T t = 0;
  for (unsigned i = 1; i < sizeof(T); ++i) {
    t = static_cast<T>(static_cast<std::make_unsigned_t<T>>(t) | (i << i));
    elems.emplace(t);
    elems.emplace(static_cast<T>(-t));
  }

  auto last = to_mem<T>(*elems.begin());
  for (auto t : elems) {
    auto str = to_mem<T>(t);
    BOOST_TEST((last <= str));
    BOOST_TEST(t == std::get<T>(from_mem<T>(str.data())));

    last = str;
  }
}

// Every value of the log carries the CRC-32C of its key and object: verify() finds a value changed
// on disk, though reads, which don't check it, still work.
BOOST_FIXTURE_TEST_CASE(verify_finds_corrupt_value, Test) {
  BOOST_TEST(log.append_entries(RaftIndex(0), RaftTerm(0), entries(1, 300, 1), false).value());
  BOOST_TEST(log.verify().has_value());
  log.close();

  // Entry 151's key (index, term) is in its leaf page, right before its value: flip a byte of the
  // value, past its 4-byte CRC.
  std::array<uint8_t, 16> key{};
  auto const              idx  = ::to_mem<uint64_t>(151);
  auto const              term = ::to_mem<uint64_t>(1);
  std::memcpy(key.data(), idx.data(), 8);
  std::memcpy(key.data() + 8, term.data(), 8);

  auto const file = log.logs_db();
  string     bytes;
  {
    std::ifstream in(file, std::ios::binary);
    bytes.assign(std::istreambuf_iterator<char>(in), {});
  }
  auto const at = bytes.find(string(reinterpret_cast<char const*>(key.data()), key.size()));
  BOOST_REQUIRE(at != string::npos);
  {
    std::fstream f(file, std::ios::in | std::ios::out | std::ios::binary);
    auto const   off = static_cast<std::streamoff>(at + key.size() + 4 + 6);
    f.seekp(off);
    f.put(static_cast<char>(bytes[static_cast<size_t>(off)] ^ 0x5A));
  }

  BOOST_REQUIRE(log.open_logs_db().has_value());
  BOOST_TEST(log.entries_from(RaftIndex(1), 300).has_value());  // reads don't check
  auto r = log.verify();
  BOOST_REQUIRE(!r.has_value());
  BOOST_TEST(r.error() == system_error_code(EBADMSG));
}

// verify() of a sound log: the sentinel alone, entries across a term change, and a compacted log
// (starting at its state's first_idx); and of a log that doesn't start where its state says.
BOOST_FIXTURE_TEST_CASE(verify_of_a_good_log, Test) {
  BOOST_TEST(log.verify().has_value());  // the (0, 0) sentinel only
  BOOST_TEST(log.append_entries(RaftIndex(0), RaftTerm(0), entries(1, 50, 1), false).value());
  BOOST_TEST(log.append_entries(RaftIndex(50), RaftTerm(1), entries(51, 50, 2), false).value());
  BOOST_TEST(log.verify().has_value());

  auto const bound = RaftState{.log_idx = RaftIndex(39), .timestamp = 39};
  BOOST_TEST(
      log.compact(RaftIndex(40),
                  RaftState{.log_idx = RaftIndex(90), .timestamp = 90, .first_idx = RaftIndex(40)},
                  bound)
          .has_value());
  BOOST_TEST(log.verify().has_value());

  // Entries 40..49 deleted, but the state says the log starts at 45: entries are missing.
  BOOST_TEST(
      log.compact(RaftIndex(50),
                  RaftState{.log_idx = RaftIndex(90), .timestamp = 90, .first_idx = RaftIndex(45)},
                  bound)
          .has_value());
  auto r = log.verify();
  BOOST_REQUIRE(!r.has_value());
  BOOST_TEST(r.error() == system_error_code(EBADMSG));
}

// verify() finds a missing entry: the indexes must follow each other, without a gap.
BOOST_FIXTURE_TEST_CASE(verify_finds_a_gap, Test) {
  BOOST_TEST(log.append_entries(RaftIndex(0), RaftTerm(0), entries(1, 20, 1), false).value());
  BOOST_TEST(log.verify().has_value());
  log.close();

  // Delete entry 10 behind the log's back.
  {
    MDB_env* env = nullptr;
    BOOST_REQUIRE(mdb_env_create(&env) == 0);
    BOOST_REQUIRE(mdb_env_set_maxdbs(env, 4) == 0);
    BOOST_REQUIRE(mdb_env_open(env, log.logs_db().c_str(), MDB_NOSUBDIR, 0664) == 0);
    MDB_txn* txn = nullptr;
    BOOST_REQUIRE(mdb_txn_begin(env, nullptr, 0, &txn) == 0);
    MDB_dbi dbi = 0;
    BOOST_REQUIRE(mdb_dbi_open(txn, "logs_", 0, &dbi) == 0);
    std::array<uint8_t, 16> key{};
    auto const              idx  = ::to_mem<uint64_t>(10);
    auto const              term = ::to_mem<uint64_t>(1);
    std::memcpy(key.data(), idx.data(), 8);
    std::memcpy(key.data() + 8, term.data(), 8);
    MDB_val k{.mv_size = key.size(), .mv_data = key.data()};
    BOOST_REQUIRE(mdb_del(txn, dbi, &k, nullptr) == 0);
    BOOST_REQUIRE(mdb_txn_commit(txn) == 0);
    mdb_env_close(env);
  }

  BOOST_REQUIRE(log.open_logs_db().has_value());
  auto r = log.verify();
  BOOST_REQUIRE(!r.has_value());
  BOOST_TEST(r.error() == system_error_code(EBADMSG));
}
