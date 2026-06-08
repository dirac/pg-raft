#define BOOST_TEST_MODULE raft_log
#include "lib/servers/zookeeper/raft_log.hpp"

#include "lib/base/testing.hpp"

namespace banyandb::servers::zookeeper::test {

static int err = EIO;

struct TestRaftLog : public RaftLog {
  std::filesystem::path term_db() const override { return tmp / std::format("{0}-term.lmdb", idx); }

  /// Logs database file
  std::filesystem::path logs_db() const override { return tmp / std::format("{0}-logs.lmdb", idx); }

  int env_set_mapsize(MDB_env* env, size_t size) override {
    if (fail && invocation-- == 0)
      return err;
    return RaftLog::env_set_mapsize(env, size >> 10);
  }

  int env_create(MDB_env** env) override {
    if (fail && invocation-- == 0)
      return err;

    return RaftLog::env_create(env);
  }
  int env_open(MDB_env* env, char const* filename) override {
    if (fail && invocation-- == 0)
      return err;

    return RaftLog::env_open(env, filename);
  }
  int env_copy(MDB_env* env, int fd) const override {
    if (fail && invocation-- == 0)
      return err;

    return RaftLog::env_copy(env, fd);
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
  int txn_commit(MDB_txn* txn) const override {
    BOOST_TEST(open_cursor == nullptr);
    BOOST_TEST(open_txn == txn);

    open_txn = nullptr;

    if (fail && invocation-- == 0)
      return err;

    return RaftLog::txn_commit(txn);
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

  void env_close(MDB_env* env) override { return RaftLog::env_close(env); }

  void txn_abort(MDB_txn* txn) const override {
    BOOST_TEST(open_cursor == nullptr);
    BOOST_TEST(open_txn == txn);
    open_txn = nullptr;

    return RaftLog::txn_abort(txn);
  }
  void cursor_close(MDB_cursor* cursor) const override {
    BOOST_TEST(open_cursor == cursor);
    open_cursor = nullptr;

    return RaftLog::cursor_close(cursor);
  }

  mutable MDB_txn*      open_txn    = nullptr;
  mutable MDB_cursor*   open_cursor = nullptr;
  mutable int           invocation;
  mutable bool          fail{false};
  std::filesystem::path tmp;
  unsigned const        idx;

  TestRaftLog(std::filesystem::path path) : RaftLog(path), tmp(path), idx(file_idx++) {}

  static unsigned file_idx;
};

unsigned TestRaftLog::file_idx = 0;

struct BaseTest {
  BaseTest() : dir(), log(dir()) {}

  template <typename T>
  Result<T> exec(std::function<Result<T>(RaftLog&)> test_fn, int max = 50) {
    for (int i = 0; i < max; ++i) {
      log.fail       = true;
      log.invocation = i;

      BOOST_REQUIRE(log.open_txn == nullptr);
      BOOST_REQUIRE(log.open_cursor == nullptr);

      log.open_txn    = nullptr;
      log.open_cursor = nullptr;

      auto cleanup = boost::scope::scope_exit([this]() noexcept { log.fail = false; });

      auto r = test_fn(log);

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

  auto r = log.append_entries(
      RaftIndex(0),
      RaftTerm(0),
      vector{Entry{.term = RaftTerm{1}, .time = 1}, Entry{.term = RaftTerm{1}, .time = 2}}, true);
  BOOST_TEST(!r.has_error());

  // Non-empty
  {
    auto r = exec<R>([](RaftLog& log) -> Result<R> { return log.last_entry(); });
    BOOST_TEST(RaftIndex(2) == std::get<RaftIndex>(r.value()));
    BOOST_TEST(RaftTerm(1) == std::get<RaftTerm>(r.value()));
  }
}

typedef boost::mpl::list<int16_t, int32_t, int64_t, uint16_t, uint32_t, uint64_t> test_types;

BOOST_AUTO_TEST_CASE_TEMPLATE(mem, T, test_types) {
  set<T> elems{std::numeric_limits<T>::min(),
               std::numeric_limits<T>::lowest(),
               static_cast<T>(-42),
               0,
               static_cast<T>(42),
               std::numeric_limits<T>::max()};

  T t = 0;
  for (unsigned i = 1; i < sizeof(T); ++i) {
    t = t | static_cast<T>(i << i);
    elems.emplace(t);
    elems.emplace(-t);
  }

  auto last = to_mem<T>(*elems.begin());
  for (auto t : elems) {
    auto str = to_mem<T>(t);
    BOOST_TEST(last <= str);
    BOOST_TEST(t == std::get<T>(from_mem<T>(str.data())));

    last = str;
  }
}

}  // namespace banyandb::servers::zookeeper::test
