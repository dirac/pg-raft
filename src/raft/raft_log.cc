#include "src/raft/raft_log.h"

#include "src/base/error_code.h"
#include "src/base/logger.h"
#include "src/base/xxhash.h"

namespace {

/// The MDB_txn/MDB_cursor currently open on this thread, tracked by the txn_*/cursor_*
/// wrappers below. Used by ScopedHandleCheck to catch a leaked handle at the exact call
/// that leaked it, instead of it silently wedging the environment's writer lock.
thread_local MDB_txn*    tl_txn    = nullptr;
thread_local MDB_cursor* tl_cursor = nullptr;

/// Placed on entry to every public RaftLog method. Asserts no txn/cursor survived past the
/// end of the previous public call on this thread, and none survives past the end of this
/// one either.
struct ScopedHandleCheck {
  ScopedHandleCheck() {
    assert(tl_txn == nullptr && "RaftLog: entered a public method with a txn left open");
    assert(tl_cursor == nullptr && "RaftLog: entered a public method with a cursor left open");
  }
  ~ScopedHandleCheck() {
    assert(tl_txn == nullptr && "RaftLog: leaving a public method without closing its txn");
    assert(tl_cursor == nullptr && "RaftLog: leaving a public method without closing its cursor");
  }
};

/// Runs the function on destruction, unless reset.
struct Cleanup {
  explicit Cleanup(std::function<void()> f) : f_(std::move(f)) {}
  ~Cleanup() {
    if (f_)
      f_();
  }
  void reset() { f_ = nullptr; }

 private:
  std::function<void()> f_;
};

template <typename T>
string to_encoded_string(T const& t) {
  std::ostringstream              stream;
  boost::archive::binary_oarchive oa(stream,
                                     boost::archive::no_header | boost::archive::no_codecvt);
  oa << t;
  return stream.str();
}

template <typename T>
Result<T> from_encoded_string(string_view str) {
  try {
    T                               t;
    std::istringstream              stream{string{str}};
    boost::archive::binary_iarchive ia(stream,
                                       boost::archive::no_header | boost::archive::no_codecvt);
    ia >> t;
    return t;
  } catch (std::exception const&) {
    return system_error_code(EBADMSG);
  }
}

// Term db.
MDB_val term_key() {
  static char* data = ::strdup("term");

  assert(strlen(data) == 4);
  return MDB_val{.mv_size = 5, .mv_data = data};
}

MDB_val vote_key() {
  static char* data = ::strdup("vote");

  assert(strlen(data) == 4);
  return MDB_val{.mv_size = 5, .mv_data = data};
}

// Logs db.
MDB_val state_key() {
  static char* data = ::strdup("state");

  assert(strlen(data) == 5);
  return MDB_val{.mv_size = 6, .mv_data = data};
}

// The state at the entry before the first one (see RaftLog::compact).
MDB_val boundary_key() {
  static char* data = ::strdup("boundary");

  assert(strlen(data) == 8);
  return MDB_val{.mv_size = 9, .mv_data = data};
}

template <typename T>
std::array<uint8_t, 8U + sizeof(T)> to_mem(std::array<uint8_t, 8> const& prefix, T t) {
  auto array = ::to_mem<T>(t);

  std::array<uint8_t, 8U + sizeof(T)> result;

  std::memcpy(result.data(), prefix.data(), prefix.size());
  std::memcpy(result.data() + 8, array.data(), array.size());

  return result;
}

std::array<uint8_t, 16> to_mem(RaftIndex idx, RaftTerm term) {
  return to_mem(::to_mem<RaftIndex::value_type>(INT(idx)), term.value());
}

template <typename A, typename B>
std::tuple<A, B> from_mem(uint8_t const* data);

template <>
std::tuple<RaftIndex, RaftTerm> from_mem<RaftIndex, RaftTerm>(uint8_t const* data) {
  uint64_t idx;
  uint64_t term;

  std::tie(idx, data)  = ::from_mem<uint64_t>(data);
  std::tie(term, data) = ::from_mem<uint64_t>(data);

  return {RaftIndex{idx}, RaftTerm{term}};
}

void checksum_fn(MDB_val const* src, MDB_val* dst, MDB_val const*) {
  uint32_t v = xxhash32(src->mv_data, src->mv_size);

  std::memset(dst->mv_data, '\0', dst->mv_size);
  std::memcpy(dst->mv_data, &v, std::min<size_t>(sizeof(v), dst->mv_size));
}

// Scan thro all the records/pages to validate checksum.
int scan(MDB_txn* txn, MDB_dbi dbi) {
  MDB_cursor* cursor;
  MDB_val     key;
  MDB_val     data;

  int rc = mdb_cursor_open(txn, dbi, &cursor);
  if (rc != MDB_SUCCESS)
    return rc;

  Cleanup c{[cursor] { mdb_cursor_close(cursor); }};

  rc = mdb_cursor_get(cursor, &key, &data, MDB_FIRST);
  while (rc == 0)
    rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT);

  return (rc != MDB_NOTFOUND) ? rc : 0;
}

}  // namespace

RaftLog::RaftLog(std::filesystem::path dir, bool sync_commits, bool no_sync)
    : dir_(std::move(dir)), sync_commits_(sync_commits && !no_sync), no_sync_(no_sync) {
  std::error_code ec;
  std::filesystem::create_directories(dir_, ec);

  if (ec) {
    log(FATAL, "Unable to create directories {0}: {1}", dir_, ec.message());
    std::abort();
  }
}

void RaftLog::start_sync() {
  assert(!sync_thread_);

  // Every commit is synced by itself, or nothing is.
  if (sync_commits_ || no_sync_)
    return;

  sync_thread_ = std::make_unique<std::thread>([this] {
    do {
      (void)sync_sem_.try_acquire_for(std::chrono::milliseconds(1000));
      std::this_thread::sleep_for(std::chrono::milliseconds(20));

      while (sync_sem_.try_acquire())
        ;
      if (int rc = env_sync(logs_env_); rc != 0) {
        log(ERROR, "sync failed : {}", rc);
        sync_failed_.test_and_set();
      }
    } while (!sync_stop_.test() && !sync_failed_.test());
  });
}

void RaftLog::stop_sync() {
  sync_stop_.test_and_set();
  sync_sem_.release();
  if (sync_thread_)
    sync_thread_->join();
  sync_thread_.reset();
  sync_stop_.clear();
}

void RaftLog::close() {
  ScopedHandleCheck const check;

  stop_sync();

  {
    std::unique_lock<std::mutex>        wlk{wmtx_};
    std::unique_lock<std::shared_mutex> rlk{rmtx_};
    if (logs_env_ != nullptr)
      env_close(logs_env_);

    logs_env_ = nullptr;
  }
  {
    std::unique_lock<std::mutex> lk{mtx_};
    if (term_env_ != nullptr)
      env_close(term_env_);
    term_env_ = nullptr;
  }
}

Result<RaftTerm> RaftLog::open_term_db() {
  ScopedHandleCheck const check;

  auto term_file = term_db();

  MDB_txn* txn = nullptr;

  if (int rc = env_create(&term_env_); rc != 0)
    return elog(system_error_code(rc), "env_create failed : {}", strerror(rc));

  if (int rc = env_set_maxdbs(term_env_, 2); rc != 0)
    return elog(system_error_code(rc), "env_set_maxdbs failed : {}", strerror(rc));

  if (int rc = env_set_checksum(term_env_, checksum_fn, sizeof(uint32_t)); rc != 0)
    return elog(system_error_code(rc),
                "env_set_checksum for file {0} failed : {1}",
                term_file,
                strerror(rc));

  if (int rc = env_open(term_env_, term_file.c_str(), no_sync_ ? MDB_NOSYNC : 0U); rc != 0)
    return elog(
        system_error_code(rc), "env_open for file {0} failed : {1}", term_file, strerror(rc));

  if (int rc = txn_wbegin(term_env_, &txn); rc != 0)
    return elog(system_error_code(rc), "txn_wbegin failed : {}", strerror(rc));

  if (int rc = dbi_open(txn, "main", &term_); rc != 0) {
    txn_abort(txn);
    return elog(system_error_code(rc), "dbi_open failed : {}", strerror(rc));
  }

  if (int rc = scan(txn, term_); rc != 0) {
    txn_abort(txn);
    return elog(system_error_code(rc), "scan term failed : {}", strerror(rc));
  }

  RaftTerm term{0};

  MDB_val key = term_key();
  MDB_val val;

  int rc = get(txn, term_, &key, &val);
  if (rc != 0 && rc != MDB_NOTFOUND) {
    txn_abort(txn);
    return elog(system_error_code(rc), "get failed : {}", strerror(rc));
  }
  if (rc == MDB_NOTFOUND) {
    // Initialize the system with a zero RaftTerm.
    auto val_str = ::to_mem(INT(term));

    val = MDB_val{.mv_size = val_str.size(), .mv_data = val_str.data()};

    if (rc = put(txn, term_, &key, &val); rc != 0) {
      txn_abort(txn);
      return elog(system_error_code(rc), "put failed : {}", strerror(rc));
    }
  } else {
    term = RaftTerm{std::get<RaftTerm::value_type>(
        ::from_mem<RaftTerm::value_type>(reinterpret_cast<uint8_t const*>(val.mv_data)))};
  }

  if (int rc = txn_commit(txn); rc != 0)
    return elog(system_error_code(rc), "txn_commit failed : {}", strerror(rc));

  return term;
}

Result<void> RaftLog::open_logs_db(unsigned factor) {
  auto logs_file   = logs_db();
  bool file_exists = std::filesystem::exists(logs_file);

  if (auto r = open_logs_db(factor, logs_file); !r) {
    if (file_exists) {
      std::error_code ec;
      std::filesystem::remove(logs_file, ec);
      if (ec)
        return ec;
      return open_logs_db(factor, logs_file);
    }
    return r;
  }
  return ok();
}

Result<void> RaftLog::open_logs_db(unsigned factor, std::filesystem::path const& logs_file) {
  ScopedHandleCheck const check;

  MDB_txn* txn = nullptr;

  if (std::filesystem::exists(logs_file))
    mmap_size_ = std::max(mmap_size_, factor * std::filesystem::file_size(logs_file));

  // When the log is not opened, what has been opened by then is closed, once.
  Cleanup on_error{[this] {
    stop_sync();
    if (logs_env_ != nullptr)
      env_close(logs_env_);
    logs_env_ = nullptr;
  }};

  if (int rc = env_create(&logs_env_); rc != 0)
    return elog(system_error_code(rc), "env_create failed : {}", strerror(rc));

  if (int rc = env_set_maxdbs(logs_env_, 4); rc != 0)
    return elog(system_error_code(rc), "env_set_maxdbs failed : {}", strerror(rc));

  if (int rc = env_set_mapsize(logs_env_, mmap_size_); rc != 0)
    return elog(
        system_error_code(rc), "env_set_mapsize to {0} failed : {1}", mmap_size_, strerror(rc));

  if (int rc = env_set_pagesize(logs_env_, kLmdbPageSize); rc != 0)
    return elog(
        system_error_code(rc), "env_set_pagesize to {0} failed : {1}", kLmdbPageSize, strerror(rc));

  if (int rc = env_set_checksum(logs_env_, checksum_fn, sizeof(uint32_t)); rc != 0)
    return elog(system_error_code(rc),
                "env_set_checksum for file {0} failed : {1}",
                logs_file,
                strerror(rc));

  // With sync_commits_ every commit syncs, otherwise the sync thread syncs the commits.
  if (int rc = env_open(logs_env_, logs_file.c_str(), sync_commits_ ? 0U : MDB_NOSYNC); rc != 0)
    return elog(
        system_error_code(rc), "env_open for file {0} failed : {1}", logs_file, strerror(rc));

  start_sync();

  if (int rc = txn_wbegin(logs_env_, &txn); rc != 0)
    return elog(system_error_code(rc), "txn_wbegin failed : {}", strerror(rc));

  for (auto [dbi, nm] :
       vector<std::tuple<MDB_dbi*, char const*>>{{&logs_, "logs_"}, {&meta_, "meta_"}}) {
    if (int rc = dbi_open(txn, nm, dbi); rc != 0) {
      txn_abort(txn);
      return elog(system_error_code(rc), "dbi_open {0} failed : {1}", nm, strerror(rc));
    }

    if (int rc = scan(txn, *dbi); rc != 0) {
      txn_abort(txn);
      return elog(system_error_code(rc), "scan {0} failed : {1}", nm, strerror(rc));
    }
  }

  /// Add one element in logs database if empty.
  bool empty = false;
  {
    MDB_cursor* cursor;
    if (int rc = cursor_open(txn, logs_, &cursor); rc != 0) {
      txn_abort(txn);
      return elog(system_error_code(rc), "cursor_open failed : {}", strerror(rc));
    }

    MDB_val key;
    MDB_val val;
    if (auto rc = cursor_get(cursor, &key, &val, MDB_FIRST); rc != 0) {
      if (rc == MDB_NOTFOUND) {
        empty = true;

        auto key_str = to_mem(RaftIndex{0}, RaftTerm{0});
        auto val_str = to_encoded_string(Entry{
            .time = 0,
            .term = RaftTerm{0},
            .cmd  = {},
        });

        key.mv_size = key_str.size();
        key.mv_data = key_str.data();

        val.mv_size = val_str.size();
        val.mv_data = val_str.data();

        if (rc = put(txn, logs_, &key, &val); rc != 0) {
          cursor_close(cursor);
          txn_abort(txn);
          return elog(system_error_code(rc), "put(0,0) failed : {}", strerror(rc));
        }
      } else {
        cursor_close(cursor);
        txn_abort(txn);
        return elog(system_error_code(rc), "cursor_get failed {}", strerror(rc));
      }
    }
    cursor_close(cursor);
  }

  if (int rc = txn_commit(txn); rc != 0)
    return elog(system_error_code(rc), "txn_commit failed : {}", strerror(rc));

  if (empty) {
    std::unique_lock<std::mutex> lk{wmtx_};
    if (auto r = save_meta(RaftState{}); !r)
      return r;
  }

  on_error.reset();
  return ok();
}

Result<void> RaftLog::save_snapshot(RaftState const& state, unsigned log_size) {
  ScopedHandleCheck const check;

  std::unique_lock<std::mutex> lk{wmtx_};
  return save_snapshot_locked(state, log_size);
}

Result<void> RaftLog::save_snapshot_locked(RaftState const& state, unsigned log_size) {
  assert(wmtx_.try_lock() == false);

  if (auto r = save_meta(state); !r)
    return r;

  // Delete older log entries.
  if (INT(state.log_idx) < log_size)
    return ok();

  return delete_older_entries(state.log_idx - log_size);
}

Result<RaftState> RaftLog::load_snapshot() const {
  ScopedHandleCheck const check;

  std::shared_lock<std::shared_mutex> lk{rmtx_};
  return read_meta();
}

Result<void> RaftLog::install_snapshot(RaftIndex            log_idx,
                                       vector<Entry> const& entries,
                                       RaftState            state,
                                       RaftState const&     boundary) {
  ScopedHandleCheck const check;

  assert(!entries.empty());

  std::unique_lock<std::mutex> lk{wmtx_};

  if (auto r = delete_all_entries(); !r)
    return elog(r.error(), "delete_all_entries failed");

  if (auto r = append_entries(log_idx + 1U, entries); !r)
    return elog(r.error(), "append entries failed");

  return save_meta(state, &boundary);
}

Result<void> RaftLog::compact(RaftIndex first, RaftState const& state, RaftState const& boundary) {
  ScopedHandleCheck const check;

  std::unique_lock<std::mutex> lk{wmtx_};

  MDB_txn* txn;
  if (auto rc = txn_wbegin(logs_env_, &txn); rc != 0)
    return system_error_code(rc);

  if (auto rc = delete_older_entries(txn, first); rc != 0) {
    txn_abort(txn);
    return system_error_code(rc);
  }
  if (auto rc = put_meta(txn, state, &boundary); rc != 0) {
    txn_abort(txn);
    if (rc == MDB_MAP_FULL) {
      if (auto r = resize_logs(); !r)
        return r;
      lk.unlock();
      return compact(first, state, boundary);
    }
    return system_error_code(rc);
  }

  if (int rc = txn_commit(txn); rc != 0)
    return system_error_code(rc);
  return ok();
}

Result<RaftState> RaftLog::load_boundary() const {
  ScopedHandleCheck const check;

  std::shared_lock<std::shared_mutex> lk{rmtx_};

  MDB_txn* txn;
  if (int rc = txn_rbegin(logs_env_, &txn); rc != 0)
    return elog(system_error_code(rc), "txn_begin failed : {}", strerror(rc));

  MDB_val key = boundary_key();
  MDB_val val;

  int rc = get(txn, meta_, &key, &val);
  if (rc == MDB_NOTFOUND) {
    txn_abort(txn);
    return RaftState{};  // never compacted: the state at the (0, 0) sentinel
  }
  if (rc != 0) {
    txn_abort(txn);
    return elog(system_error_code(rc), "get(boundary) failed : {}", strerror(rc));
  }

  auto r = from_encoded_string<RaftState>(
      string_view{reinterpret_cast<char const*>(val.mv_data), val.mv_size});
  txn_abort(txn);
  return r;
}

int RaftLog::put_meta(MDB_txn* txn, RaftState const& state, RaftState const* boundary) {
  auto    val_str = to_encoded_string(state);
  MDB_val key     = state_key();
  MDB_val val{.mv_size = val_str.size(), .mv_data = val_str.data()};
  if (auto rc = put(txn, meta_, &key, &val); rc != 0)
    return rc;

  if (boundary) {
    auto    b_str = to_encoded_string(*boundary);
    MDB_val b_key = boundary_key();
    MDB_val b_val{.mv_size = b_str.size(), .mv_data = b_str.data()};
    if (auto rc = put(txn, meta_, &b_key, &b_val); rc != 0)
      return rc;
  }
  return 0;
}

Result<void> RaftLog::save_meta(RaftState const& state, RaftState const* boundary) {
  assert(wmtx_.try_lock() == false);

  MDB_txn* txn;
  if (auto rc = txn_wbegin(logs_env_, &txn); rc != 0)
    return system_error_code(rc);

  if (auto rc = put_meta(txn, state, boundary); rc != 0) {
    txn_abort(txn);

    if (rc == MDB_MAP_FULL) {
      if (auto r = resize_logs(); !r)
        return r;

      return save_meta(state, boundary);
    }
    return system_error_code(rc);
  }

  if (int rc = txn_commit(txn); rc != 0)
    return system_error_code(rc);

  return ok();
}

Result<RaftState> RaftLog::read_meta() const {
  MDB_txn* txn;
  if (int rc = txn_rbegin(logs_env_, &txn); rc != 0)
    return elog(system_error_code(rc), "txn_begin failed : {}", strerror(rc));

  MDB_val key = state_key();
  MDB_val val;

  int rc = get(txn, meta_, &key, &val);
  if (rc == MDB_NOTFOUND) {
    txn_abort(txn);
    return RaftState{};
  }
  if (rc != 0) {
    txn_abort(txn);
    return elog(system_error_code(rc), "get(state) failed : {}", strerror(rc));
  }

  auto r = from_encoded_string<RaftState>(
      string_view{reinterpret_cast<char const*>(val.mv_data), val.mv_size});
  txn_abort(txn);
  return r;
}

Result<void> RaftLog::delete_older_entries(RaftIndex idx) {
  assert(wmtx_.try_lock() == false);

  MDB_txn* txn;
  if (auto rc = txn_wbegin(logs_env_, &txn); rc != 0)
    return system_error_code(rc);

  if (auto rc = delete_older_entries(txn, idx); rc != 0) {
    txn_abort(txn);
    return system_error_code(rc);
  }

  if (auto rc = txn_commit(txn); rc != 0)
    return system_error_code(rc);

  return ok();
}

int RaftLog::delete_older_entries(MDB_txn* txn, RaftIndex idx) {
  // In |txn|: the caller commits or aborts it.
  MDB_cursor* cursor;
  if (auto rc = cursor_open(txn, logs_, &cursor); rc != 0)
    return rc;

  auto key_bytes = to_mem(idx, RaftTerm{0});

  MDB_val key;
  MDB_val val;

  MDB_cursor_op op = MDB_FIRST;

  int rc;
  while ((rc = cursor_get(cursor, &key, &val, op)) == 0) {
    if (std::memcmp(key.mv_data, key_bytes.data(), key_bytes.size()) < 0) {
      if (rc = cursor_del(cursor); rc != 0) {
        cursor_close(cursor);
        return rc;
      }
    } else {
      break;
    }
    op = MDB_NEXT;
  }
  cursor_close(cursor);

  return rc == MDB_NOTFOUND ? 0 : rc;
}

Result<void> RaftLog::delete_all_entries() {
  assert(wmtx_.try_lock() == false);

  MDB_txn* txn;
  if (auto rc = txn_wbegin(logs_env_, &txn); rc != 0)
    return system_error_code(rc);

  MDB_cursor* cursor;
  if (auto rc = cursor_open(txn, logs_, &cursor); rc != 0) {
    txn_abort(txn);
    return system_error_code(rc);
  }

  MDB_val key;
  MDB_val val;

  int rc;
  while ((rc = cursor_get(cursor, &key, &val, MDB_FIRST)) == 0) {
    if (rc = cursor_del(cursor); rc != 0) {
      cursor_close(cursor);
      txn_abort(txn);
      return system_error_code(rc);
    }
  }
  cursor_close(cursor);

  if (rc != MDB_NOTFOUND) {
    txn_abort(txn);
    return system_error_code(rc);
  }

  if (rc = txn_commit(txn); rc != 0)
    return system_error_code(rc);

  return ok();
}

Result<void> RaftLog::resize_logs() {
  assert(wmtx_.try_lock() == false);

  if (int rc = env_set_mapsize(logs_env_, mmap_size_ << 2U); rc != 0)
    return system_error_code(rc);
  mmap_size_ <<= 2U;

  return ok();
}

Result<bool> RaftLog::reset_entries(RaftIndex prev_log_idx, RaftTerm prev_log_term) {
  assert(wmtx_.try_lock() == false);

  MDB_txn* txn;
  if (auto rc = txn_wbegin(logs_env_, &txn); rc != 0)
    return elog(system_error_code(rc), "begin_txn failed {}", strerror(rc));

  MDB_cursor* cursor;
  if (auto rc = cursor_open(txn, logs_, &cursor); rc != 0) {
    txn_abort(txn);
    return elog(system_error_code(rc), "cursor_open failed {}", strerror(rc));
  }

  auto    key_bytes = to_mem(prev_log_idx, RaftTerm{0});
  MDB_val key{.mv_size = key_bytes.size(), .mv_data = key_bytes.data()};
  MDB_val val;

  if (auto rc = cursor_get(cursor, &key, &val, MDB_SET_RANGE); rc != 0) {
    cursor_close(cursor);
    txn_abort(txn);

    if (rc == MDB_NOTFOUND)
      return false;
    return elog(system_error_code(rc), "cursor_get failed {}", strerror(rc));
  }

  auto [idx, term] = from_mem<RaftIndex, RaftTerm>(reinterpret_cast<uint8_t const*>(key.mv_data));
  if (idx != prev_log_idx || term != prev_log_term) {
    log(ERROR,
        "idx ({} vs {}) or term ({} vs {})",
        idx.value(),
        prev_log_idx.value(),
        term.value(),
        prev_log_term.value());
    cursor_close(cursor);
    txn_abort(txn);
    return false;
  }

  // Delete all elements after prev_log_idx.
  int rc;
  while ((rc = cursor_get(cursor, &key, &val, MDB_NEXT)) != MDB_NOTFOUND) {
    if (rc != 0) {
      cursor_close(cursor);
      txn_abort(txn);
      return system_error_code(rc);
    }

    if (rc = cursor_del(cursor); rc != 0) {
      cursor_close(cursor);
      txn_abort(txn);
      return system_error_code(rc);
    }
  }
  cursor_close(cursor);

  if (rc = txn_commit(txn); rc != 0)
    return system_error_code(rc);

  return true;
}

Result<bool> RaftLog::append_entries(RaftIndex            prev_log_idx,
                                     RaftTerm             prev_log_term,
                                     vector<Entry> const& entries,
                                     bool                 delete_entries) {
  ScopedHandleCheck const check;

  std::unique_lock<std::mutex> lk{wmtx_};

  if (delete_entries) {
    auto r = reset_entries(prev_log_idx, prev_log_term);
    if (!r)
      return elog(r.error(), "reset_entries failed");
    if (!r.value()) {
      log(ERROR, "The prev did not match idx={}/term={}", prev_log_idx, prev_log_term);
      return false;
    }
  } else {
    auto r = entry(MDB_LAST);
    if (!r)
      return elog(r.error(), "last_entry failed");
    if (r.value() != std::make_tuple(prev_log_term, prev_log_idx)) {
      log(ERROR,
          "last entry did not match: ({}, {}) vs ({}, {})",
          prev_log_term,
          prev_log_idx,
          std::get<RaftTerm>(r.value()),
          std::get<RaftIndex>(r.value()));
      return false;
    }
  }

  if (auto r = append_entries(prev_log_idx + 1U, entries); !r)
    return elog(r.error(), "append entries failed");
  return true;
}

Result<void> RaftLog::append_entries(RaftIndex const idx, vector<Entry> const& entries) {
  assert(wmtx_.try_lock() == false);

  MDB_txn* txn;
  if (auto rc = txn_wbegin(logs_env_, &txn); rc != 0)
    return elog(system_error_code(rc), "txn_begin failed {}", strerror(rc));

  size_t n = 0;
  for (Entry const& entry : entries) {
    auto new_key_bytes  = to_mem(idx + (n++), entry.term);
    auto new_val_string = to_encoded_string(entry);

    MDB_val key{.mv_size = new_key_bytes.size(), .mv_data = new_key_bytes.data()};
    MDB_val val{.mv_size = new_val_string.size(), .mv_data = new_val_string.data()};

    int rc = put(txn, logs_, &key, &val);

    if (rc == MDB_MAP_FULL) {
      txn_abort(txn);
      if (auto r = resize_logs(); !r)
        return r;

      return append_entries(idx, entries);
    }

    if (rc == MDB_TXN_FULL) {
      assert(entries.size() != 1);

      // abort this transaction, Split the entries into two vectors and try again.
      txn_abort(txn);

      auto mid = entries.begin() + static_cast<vector<Entry>::difference_type>(entries.size() / 2);

      vector<Entry> left{entries.begin(), mid};
      vector<Entry> right{mid, entries.end()};

      assert(entries.size() == (left.size() + right.size()));

      if (auto r = append_entries(idx, left); !r)
        return r;
      return append_entries(idx + left.size(), right);
    }

    if (rc != 0) {
      txn_abort(txn);
      return elog(system_error_code(rc), "put failed {}", strerror(rc));
    }
  }

  if (int rc = txn_commit(txn); rc != 0)
    return elog(system_error_code(rc), "txn_commit failed {}", strerror(rc));

  return ok();
}

Result<RaftTerm> RaftLog::set_current_term(RaftTerm term) {
  ScopedHandleCheck const check;

  std::unique_lock<std::mutex> lk{mtx_};

  MDB_txn* txn;
  if (int rc = txn_wbegin(term_env_, &txn); rc != 0)
    return elog(system_error_code(rc), "txn_begin failed : {}", strerror(rc));

  auto term_bytes = ::to_mem(INT(term));

  MDB_val tkey = term_key();
  MDB_val vkey = vote_key();
  MDB_val val;

  if (int rc = get(txn, term_, &tkey, &val); rc != 0) {
    txn_abort(txn);
    return elog(system_error_code(rc), "get(term) failed : {}", strerror(rc));
  }

  if (std::memcmp(val.mv_data, term_bytes.data(), term_bytes.size()) < 0) {
    val.mv_size = term_bytes.size();
    val.mv_data = term_bytes.data();
    if (int rc = put(txn, term_, &tkey, &val); rc != 0) {
      txn_abort(txn);
      return elog(system_error_code(rc), "put(term) failed : {}", strerror(rc));
    }
    if (int rc = del(txn, term_, &vkey); rc != 0 && rc != MDB_NOTFOUND) {
      txn_abort(txn);
      return elog(system_error_code(rc), "del(vote) failed : {}", strerror(rc));
    }
    if (int rc = txn_commit(txn); rc != 0)
      return elog(system_error_code(rc), "txn_commit(txn) failed : {}", strerror(rc));
    return term;
  }
  txn_abort(txn);

  return RaftTerm{std::get<RaftTerm::value_type>(
      ::from_mem<RaftTerm::value_type>(reinterpret_cast<uint8_t const*>(val.mv_data)))};
}

Result<bool> RaftLog::vote(RaftTerm term, RaftNodeId node) {
  ScopedHandleCheck const check;

  std::unique_lock<std::mutex> lk{mtx_};

  MDB_txn* txn;
  if (int rc = txn_wbegin(term_env_, &txn); rc != 0)
    return elog(system_error_code(rc), "txn_begin failed : {}", strerror(rc));

  auto term_bytes = ::to_mem(INT(term));
  auto node_bytes = ::to_mem(INT(node));

  MDB_val tkey = term_key();
  MDB_val vkey = vote_key();
  MDB_val val;

  if (int rc = get(txn, term_, &tkey, &val); rc != 0) {
    txn_abort(txn);
    return elog(system_error_code(rc), "get(term) failed : {}", strerror(rc));
  }

  if (std::memcmp(val.mv_data, term_bytes.data(), term_bytes.size()) != 0) {
    log(ERROR,
        "The term does not match {} vs {}",
        INT(term),
        std::get<RaftTerm::value_type>(
            ::from_mem<RaftTerm::value_type>(reinterpret_cast<uint8_t*>(val.mv_data))));

    txn_abort(txn);
    return false;
  }

  int rc = get(txn, term_, &vkey, &val);
  if (rc != 0 && rc != MDB_NOTFOUND) {
    txn_abort(txn);
    return elog(system_error_code(rc), "get(vote) failed : {}", strerror(rc));
  }

  if (rc != MDB_NOTFOUND) {
    bool result = std::memcmp(val.mv_data, node_bytes.data(), node_bytes.size()) == 0;

    // Close the transaction after reading the value.
    txn_abort(txn);
    return result;
  }

  val.mv_size = node_bytes.size();
  val.mv_data = node_bytes.data();

  if (int rc = put(txn, term_, &vkey, &val); rc != 0) {
    txn_abort(txn);
    return elog(system_error_code(rc), "put(vote) failed : {}", strerror(rc));
  }

  if (int rc = txn_commit(txn); rc != 0)
    return elog(system_error_code(rc), "txn_commit(txn) failed : {}", strerror(rc));
  return true;
}

Result<RaftTerm> RaftLog::current_term() const {
  ScopedHandleCheck const check;

  MDB_txn* txn;
  if (int rc = txn_rbegin(term_env_, &txn); rc != 0)
    return elog(system_error_code(rc), "txn_begin failed : {}", strerror(rc));

  MDB_val tkey = term_key();
  MDB_val val;

  if (int rc = get(txn, term_, &tkey, &val); rc != 0) {
    txn_abort(txn);
    return elog(system_error_code(rc), "get(term) failed : {}", strerror(rc));
  }
  auto result = RaftTerm{std::get<RaftTerm::value_type>(
      ::from_mem<RaftTerm::value_type>(reinterpret_cast<uint8_t const*>(val.mv_data)))};

  txn_abort(txn);
  return result;
}

Result<std::tuple<RaftTerm, RaftIndex>> RaftLog::entry(MDB_cursor_op op) const {
  std::shared_lock<std::shared_mutex> lk{rmtx_};

  MDB_txn* txn;
  if (int rc = txn_rbegin(logs_env_, &txn); rc != 0)
    return elog(system_error_code(rc), "txn_begin failed : {}", strerror(rc));

  MDB_cursor* cursor;
  if (int rc = cursor_open(txn, logs_, &cursor); rc != 0) {
    txn_abort(txn);
    return elog(system_error_code(rc), "cursor_open(logs) failed : {}", strerror(rc));
  }

  MDB_val key;
  MDB_val val;

  if (int rc = cursor_get(cursor, &key, &val, op); rc != 0) {
    cursor_close(cursor);
    txn_abort(txn);

    /// MDB_NOTFOUND is not possible.
    return elog(system_error_code(rc), "cursor_get(logs) failed : {}", strerror(rc));
  }

  auto [idx, term] = from_mem<RaftIndex, RaftTerm>(reinterpret_cast<uint8_t const*>(key.mv_data));

  cursor_close(cursor);
  txn_abort(txn);
  return std::make_tuple(term, idx);
}

Result<std::tuple<RaftTerm, RaftIndex>> RaftLog::last_entry() const {
  ScopedHandleCheck const check;
  return entry(MDB_LAST);
}

Result<std::tuple<RaftTerm, RaftIndex>> RaftLog::first_entry() const {
  ScopedHandleCheck const check;
  return entry(MDB_FIRST);
}

Result<std::optional<vector<Entry>>> RaftLog::entries_from(RaftIndex const idx,
                                                           size_t          count) const {
  ScopedHandleCheck const check;
  return get_entries(idx, count, MDB_NEXT);
}

Result<std::tuple<RaftIndex, vector<Entry>>> RaftLog::latest_entries(size_t count) const {
  ScopedHandleCheck const check;

  auto lk = std::shared_lock<std::shared_mutex>{rmtx_};

  RaftIndex idx;
  if (auto r = entry(MDB_LAST); !r)
    return r.error();
  else
    idx = std::get<RaftIndex>(r.value());

  auto r = get_entries(idx, count, MDB_PREV);
  if (!r)
    return r.error();
  if (!r.value())
    return elog(make_error_code(ErrorCode::kProgramBug), "The last entry is not in the log");

  return std::make_tuple(idx, std::move(r.value().value()));
}

Result<std::optional<vector<Entry>>> RaftLog::get_entries(RaftIndex const idx,
                                                          size_t          count,
                                                          MDB_cursor_op   op) const {
  assert(op == MDB_NEXT || op == MDB_PREV);

  auto lk = std::shared_lock<std::shared_mutex>{rmtx_};

  MDB_txn* txn;
  if (int rc = txn_rbegin(logs_env_, &txn); rc != 0)
    return elog(system_error_code(rc), "txn_begin failed : {}", strerror(rc));

  MDB_cursor* cursor;
  if (int rc = cursor_open(txn, logs_, &cursor); rc != 0) {
    txn_abort(txn);
    return elog(system_error_code(rc), "cursor_open(logs) failed : {}", strerror(rc));
  }

  auto key_bytes = to_mem(idx, RaftTerm{0});

  MDB_val key{.mv_size = key_bytes.size(), .mv_data = key_bytes.data()};
  MDB_val val;

  int rc = 0;
  if (rc = cursor_get(cursor, &key, &val, MDB_SET_RANGE); rc != 0 && rc != MDB_NOTFOUND) {
    cursor_close(cursor);
    txn_abort(txn);
    return elog(system_error_code(rc), "cursor_get(logs) failed : {}", strerror(rc));
  }

  if (rc == MDB_NOTFOUND || std::get<RaftIndex>(from_mem<RaftIndex, RaftTerm>(
                                reinterpret_cast<uint8_t const*>(key.mv_data))) != idx) {
    cursor_close(cursor);
    txn_abort(txn);
    if (rc == MDB_NOTFOUND)
      return vector<Entry>{};
    return std::nullopt;
  }

  auto result = vector<Entry>{};
  do {
    auto [index, _] = from_mem<RaftIndex, RaftTerm>(reinterpret_cast<uint8_t const*>(key.mv_data));
    (void)index;
    assert(op != MDB_NEXT || index == (idx + result.size()));
    assert(op != MDB_PREV || index == (idx - result.size()));

    if (auto r = from_encoded_string<Entry>(
            string_view{reinterpret_cast<char const*>(val.mv_data), val.mv_size});
        r) {
      result.emplace_back(std::move(r.value()));
    } else {
      cursor_close(cursor);
      txn_abort(txn);
      return r.error();
    }

    rc = cursor_get(cursor, &key, &val, op);
  } while ((result.size() < count) && (rc == 0));

  cursor_close(cursor);
  txn_abort(txn);
  return result;
}

Result<std::optional<RaftTerm>> RaftLog::get_term(RaftIndex idx) const {
  ScopedHandleCheck const check;

  auto lk = std::shared_lock<std::shared_mutex>{rmtx_};

  MDB_txn* txn;
  if (int rc = txn_rbegin(logs_env_, &txn); rc != 0)
    return elog(system_error_code(rc), "txn_begin failed : {}", strerror(rc));

  MDB_cursor* cursor;
  if (int rc = cursor_open(txn, logs_, &cursor); rc != 0) {
    txn_abort(txn);
    return elog(system_error_code(rc), "cursor_open(logs) failed : {}", strerror(rc));
  }

  auto key_bytes = to_mem(idx, RaftTerm{0});

  MDB_val key{.mv_size = key_bytes.size(), .mv_data = key_bytes.data()};
  MDB_val val;

  int rc = 0;
  if (rc = cursor_get(cursor, &key, &val, MDB_SET_RANGE); rc != 0 && rc != MDB_NOTFOUND) {
    cursor_close(cursor);
    txn_abort(txn);
    return elog(system_error_code(rc), "cursor_get(logs) failed : {}", strerror(rc));
  }

  if (rc == MDB_NOTFOUND) {
    cursor_close(cursor);
    txn_abort(txn);
    return std::optional<RaftTerm>{std::nullopt};
  }

  auto [index, term] = from_mem<RaftIndex, RaftTerm>(reinterpret_cast<uint8_t const*>(key.mv_data));

  cursor_close(cursor);
  txn_abort(txn);

  return index == idx ? std::optional<RaftTerm>{term} : std::optional<RaftTerm>{std::nullopt};
}

char const* RaftLog::strerror(int rc) const {
  return mdb_strerror(rc);
}
int RaftLog::env_set_checksum(MDB_env* env, MDB_sum_func* func, unsigned int size) {
  return mdb_env_set_checksum(env, func, size);
}
int RaftLog::env_set_mapsize(MDB_env* env, size_t size) {
  return mdb_env_set_mapsize(env, size);
}
int RaftLog::env_set_pagesize(MDB_env* env, size_t size) {
  return mdb_env_set_pagesize(env, i4(size));
}
int RaftLog::env_set_maxdbs(MDB_env* env, unsigned dbs) {
  return mdb_env_set_maxdbs(env, dbs);
}
int RaftLog::env_create(MDB_env** env) {
  *env = nullptr;
  return mdb_env_create(env);
}
int RaftLog::env_open(MDB_env* env, char const* filename, unsigned int flags) {
  return mdb_env_open(env, filename, MDB_NOSUBDIR | flags, 0664);
}
void RaftLog::env_close(MDB_env* env) {
  return mdb_env_close(env);
}
int RaftLog::env_sync(MDB_env* env) {
  return mdb_env_sync(env, 1);
}
int RaftLog::dbi_open(MDB_txn* txn, char const* name, MDB_dbi* dbi) {
  return mdb_dbi_open(txn, name, MDB_CREATE, dbi);
}
int RaftLog::txn_rbegin(MDB_env* env, MDB_txn** txn) const {
  assert(tl_txn == nullptr && "RaftLog: txn_rbegin with a txn already open on this thread");
  int rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, txn);
  if (rc == 0)
    tl_txn = *txn;
  return rc;
}
int RaftLog::txn_wbegin(MDB_env* env, MDB_txn** txn) {
  if (sync_failed_.test())
    return MDB_PANIC;

  assert(tl_txn == nullptr && "RaftLog: txn_wbegin with a txn already open on this thread");
  int rc = mdb_txn_begin(env, nullptr, 0, txn);
  if (rc == 0)
    tl_txn = *txn;
  return rc;
}
int RaftLog::txn_commit(MDB_txn* txn) const {
  assert(tl_txn == txn && "RaftLog: txn_commit on a txn that isn't the one open on this thread");
  // Commit consumes the txn whether it succeeds or fails, so clear the tracking before
  // dispatching to the (possibly overridden) impl.
  tl_txn = nullptr;
  auto r = txn_commit_impl(txn);
  if (!sync_commits_ && !no_sync_)
    sync_sem_.release();
  return r;
}
int RaftLog::txn_commit_impl(MDB_txn* txn) const {
  return mdb_txn_commit(txn);
}
void RaftLog::txn_abort(MDB_txn* txn) const {
  assert(tl_txn == txn && "RaftLog: txn_abort on a txn that isn't the one open on this thread");
  tl_txn = nullptr;
  txn_abort_impl(txn);
}
void RaftLog::txn_abort_impl(MDB_txn* txn) const {
  mdb_txn_abort(txn);
}
int RaftLog::get(MDB_txn* txn, MDB_dbi dbi, MDB_val* key, MDB_val* val) const {
  return mdb_get(txn, dbi, key, val);
}
int RaftLog::put(MDB_txn* txn, MDB_dbi dbi, MDB_val* key, MDB_val* val) {
  return mdb_put(txn, dbi, key, val, 0);
}
int RaftLog::del(MDB_txn* txn, MDB_dbi dbi, MDB_val* key) {
  return mdb_del(txn, dbi, key, nullptr);
}
int RaftLog::cursor_open(MDB_txn* txn, MDB_dbi dbi, MDB_cursor** cursor) const {
  assert(tl_cursor == nullptr && "RaftLog: cursor_open with a cursor already open on this thread");
  int rc = mdb_cursor_open(txn, dbi, cursor);
  if (rc == 0)
    tl_cursor = *cursor;
  return rc;
}
int RaftLog::cursor_get(MDB_cursor* cursor, MDB_val* key, MDB_val* val, MDB_cursor_op op) const {
  return mdb_cursor_get(cursor, key, val, op);
}
int RaftLog::cursor_del(MDB_cursor* cursor) {
  return mdb_cursor_del(cursor, 0);
}
void RaftLog::cursor_close(MDB_cursor* cursor) const {
  assert(tl_cursor == cursor && "RaftLog: cursor_close on a cursor not open on this thread");
  tl_cursor = nullptr;
  cursor_close_impl(cursor);
}
void RaftLog::cursor_close_impl(MDB_cursor* cursor) const {
  mdb_cursor_close(cursor);
}
