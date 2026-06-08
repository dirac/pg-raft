#include "src/raft/raft_log.h"

#include "src/base/string.h"

namespace {

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
MDB_val index_key() {
  static char* data = ::strdup("index");

  assert(strlen(data) == 5);
  return MDB_val{.mv_size = 6, .mv_data = data};
}

MDB_val timestamp_key() {
  static char* data = ::strdup("timestamp");

  assert(strlen(data) == 9);
  return MDB_val{.mv_size = 10, .mv_data = data};
}

MDB_val counter_key() {
  static char* data = ::strdup("counter");

  assert(strlen(data) == 7);
  return MDB_val{.mv_size = 8, .mv_data = data};
}

template <typename T>
std::array<uint8_t, 8U + sizeof(T)> to_mem(std::array<uint8_t, 8> const& prefix, T t) {
  auto array = banyandb::servers::zookeeper::to_mem<T>(t);

  std::array<uint8_t, 8U + sizeof(T)> result;

  std::memcpy(result.data(), prefix.data(), prefix.size());
  std::memcpy(result.data() + 8, array.data(), array.size());

  return result;
}

std::array<uint8_t, 16> to_mem(RaftIndex idx, RaftTerm term) {
  auto str =
      to_mem(banyandb::servers::zookeeper::to_mem<RaftIndex::value_type>(INT(idx)), term.value());
  return str;
}

template <typename A, typename B>
std::tuple<A, B> from_mem(uint8_t const* data);

template <>
std::tuple<RaftIndex, RaftTerm> from_mem<RaftIndex, RaftTerm>(uint8_t const* data) {
  uint64_t idx;
  uint64_t term;

  std::tie(idx, data)  = banyandb::servers::zookeeper::from_mem<uint64_t>(data);
  std::tie(term, data) = banyandb::servers::zookeeper::from_mem<uint64_t>(data);

  return {RaftIndex{idx}, RaftTerm{term}};
}

vector<uint8_t> to_mem(std::array<uint8_t, 8> const& prefix, ZkState::Path const& path) {
  vector<uint8_t> result;
  result.reserve(prefix.size() + path.size() * sizeof(ZkState::PathId));

  result.insert_range(result.end(), prefix);
  for (auto id : path) {
    auto e = banyandb::servers::zookeeper::to_mem(id);
    result.insert_range(result.end(), e);
  }
  return result;
}

ZkState::Path path_from_mem(uint8_t const* data, size_t size) {
  ZkState::Path p;
  p.reserve(size / sizeof(ZkState::PathId));

  ZkState::PathId id;
  while (size >= sizeof(ZkState::PathId)) {
    std::tie(id, data) = banyandb::servers::zookeeper::from_mem<ZkState::PathId>(data);
    p.emplace_back(id);

    size -= sizeof(ZkState::PathId);
  }
  return p;
}

string to_val(string_view str) {
  return string{str};
}

string to_val(ZkState::NodeData const& v) {
  return to_encoded_string(v);
}

string to_val(ZkState::Session<ZkState::Path> const& v) {
  return to_encoded_string(v);
}

std::array<uint8_t, 8> to_val(std::time_t t) {
  return banyandb::servers::zookeeper::to_mem<uint64_t>(u8(t));
}

}  // namespace

RaftLog::RaftLog(std::filesystem::path dir) : dir_(dir) {
  std::error_code ec;
  std::filesystem::create_directories(dir_, ec);

  if (ec)
    ABORT(fn::format("Unable to create directories {0}: {1}", dir_.c_str(), ec.message()));
}

Result<RaftTerm> RaftLog::open_term_db() {
  auto term_file = term_db();

  MDB_txn* txn = nullptr;

  if (int rc = env_create(&term_env_); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("env_create failed : {}", strerror(rc)));

  if (int rc = env_set_maxdbs(term_env_, 2); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("env_set_maxdbs failed : {}", strerror(rc)));

  if (int rc = env_open(term_env_, term_file.c_str()); rc != 0)
    return ERROR_MSG(
        system_error_code(rc),
        fn::format("env_open for file {0} failed : {1}", term_file.c_str(), strerror(rc)));

  if (int rc = txn_wbegin(term_env_, &txn); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("txn_wbegin failed : {}", strerror(rc)));

  if (int rc = dbi_open(txn, "main", &term_); rc != 0) {
    txn_abort(txn);
    return ERROR_MSG(system_error_code(rc), fn::format("dbi_open failed : {}", strerror(rc)));
  }

  RaftTerm term{0};

  MDB_val key = term_key();
  MDB_val val;

  if (int rc = get(txn, term_, &key, &val); rc != 0 && rc != MDB_NOTFOUND) {
    txn_abort(txn);
    return ERROR_MSG(system_error_code(rc), fn::format("get failed : {}", strerror(rc)));
  } else if (rc == MDB_NOTFOUND) {
    // Initialize the system with a zero RaftTerm.
    auto val_str = to_mem(INT(term));

    val = MDB_val{.mv_size = val_str.size(), .mv_data = val_str.data()};

    if (rc = put(txn, term_, &key, &val); rc != 0) {
      txn_abort(txn);
      return ERROR_MSG(system_error_code(rc), fn::format("put failed : {}", strerror(rc)));
    }
  } else
    term = RaftTerm{std::get<RaftTerm::value_type>(
        from_mem<RaftTerm::value_type>(reinterpret_cast<uint8_t const*>(val.mv_data)))};

  if (int rc = txn_commit(txn); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("txn_commit failed : {}", strerror(rc)));

  return term;
}

Result<void> RaftLog::open_logs_db(unsigned factor) {
  auto logs_file = logs_db();

  MDB_txn* txn = nullptr;

  if (std::filesystem::exists(logs_file))
    mmap_size_ = std::max(mmap_size_, factor * std::filesystem::file_size(logs_file));

  if (int rc = env_create(&logs_env_); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("env_create failed : {}", strerror(rc)));

  if (int rc = env_set_maxdbs(logs_env_, 7); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("env_set_maxdbs failed : {}", strerror(rc)));

  if (int rc = env_set_mapsize(logs_env_, mmap_size_); rc != 0)
    return ERROR_MSG(system_error_code(rc),
                     fn::format("env_set_mapsize to {0} failed : {1}", mmap_size_, strerror(rc)));

  if (int rc = env_open(logs_env_, logs_file.c_str()); rc != 0)
    return ERROR_MSG(
        system_error_code(rc),
        fn::format("env_open for file {0} failed : {1}", logs_file.c_str(), strerror(rc)));

  if (int rc = txn_wbegin(logs_env_, &txn); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("txn_wbegin failed : {}", strerror(rc)));

#define E(m) \
  {          \
    &m, #m   \
  }
  for (auto [dbi, nm] : vector<std::tuple<MDB_dbi*, char const*>>{
           E(logs_), E(meta_), E(strings_), E(nodes_), E(ttl_nodes_), E(sessions_)}) {
    if (int rc = dbi_open(txn, nm, dbi); rc != 0) {
      txn_abort(txn);
      return ERROR_MSG(system_error_code(rc),
                       fn::format("dbi_open {0} failed : {1}", nm, strerror(rc)));
    }
  }

  /// Add one element in logs database if empty.
  bool empty = false;
  {
    MDB_cursor* cursor;
    if (int rc = cursor_open(txn, logs_, &cursor); rc != 0) {
      txn_abort(txn);
      return ERROR_MSG(system_error_code(rc), fn::format("cursor_open failed : {}", strerror(rc)));
    }

    MDB_val key;
    MDB_val val;
    if (auto rc = cursor_get(cursor, &key, &val, MDB_FIRST); rc != 0) {
      if (rc == MDB_NOTFOUND) {
        empty = true;

        auto key_str = to_mem(RaftIndex{0}, RaftTerm{0});
        auto val_str = to_encoded_string(Entry{.term = RaftTerm{0}, .time = 0, .cmd = {}});

        key.mv_size = key_str.size();
        key.mv_data = key_str.data();

        val.mv_size = val_str.size();
        val.mv_data = val_str.data();

        if (rc = put(txn, logs_, &key, &val); rc != 0) {
          cursor_close(cursor);
          txn_abort(txn);
          return ERROR_MSG(system_error_code(rc), fn::format("put(0,0) failed : {}", strerror(rc)));
        }
      } else {
        cursor_close(cursor);
        txn_abort(txn);
        return ERROR_MSG(system_error_code(rc), fn::format("cursor_get failed {}", strerror(rc)));
      }
    }
    cursor_close(cursor);
  }

  if (int rc = txn_commit(txn); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("txn_commit failed : {}", strerror(rc)));
  return empty ? save_snapshot(ZkState{}) : ok();
}

Result<void> RaftLog::apply_db_snapshot(std::filesystem::path filename) {
  std::unique_lock<std::mutex>        wlk{wmtx_};
  std::unique_lock<std::shared_mutex> rlk{rmtx_};

  env_close(logs_env_);

  std::error_code ec;
  std::filesystem::rename(filename, logs_db(), ec);

  if (ec)
    return ERROR_MSG(ec,
                     fn::format("Unable to rename {} to {}", filename.c_str(), logs_db().c_str()));

  return open_logs_db(/* factor= */ 2U);
}

Result<void> RaftLog::take_db_snapshot(std::filesystem::path filename) const {
  std::shared_lock<std::shared_mutex> rlk{rmtx_};

  int fd = ::open(filename.c_str(), O_CREAT | O_EXCL | O_RDWR, file_mode());
  if (fd == -1) {
    return ERROR_MSG(system_error_code(errno),
                     fn::format("Unable to open file {0}", filename.c_str()));
  }

  if (int rc = env_copy(logs_env_, fd); rc != 0) {
    ::close(fd);
    return ERROR_MSG(system_error_code(rc),
                     fn::format("env_copy to {0} failed : {1}", filename.c_str(), strerror(rc)));
  }

  if (int rc = ::close(fd); rc != 0) {
    return ERROR_MSG(system_error_code(errno),
                     fn::format("Unable to close file {0}", filename.c_str()));
  }
  return ok();
}

Result<void> RaftLog::save_snapshot(ZkState snapshot, unsigned log_size) {
  std::unique_lock<std::mutex> lk{wmtx_};

  std::array<uint8_t, 8> const prefix = to_mem(INT(snapshot.curr_zxid()));

  if (auto r = save_strings(prefix, snapshot, 0U, snapshot.num_strings()); !r)
    return r;

  if (auto r = save_nodes(prefix, snapshot, 0U, snapshot.num_nodes()); !r)
    return r;

  if (auto r = save_ttl_nodes(prefix, snapshot, 0U, snapshot.num_ttl_nodes()); !r)
    return r;

  if (auto r = save_sessions(prefix, snapshot, 0U, snapshot.num_sessions()); !r)
    return r;

  if (auto r = save_meta(prefix, snapshot); !r)
    return r;

  for (auto dbi : {strings_, nodes_, ttl_nodes_, sessions_}) {
    if (auto r = erase_all_except(dbi, prefix); !r)
      return r;
  }

  // Delete older log entries.
  if (INT(snapshot.curr_zxid()) < log_size)
    return ok();

  return delete_older_entries(snapshot.curr_zxid() - log_size);
}

Result<ZkState> RaftLog::load_snapshot() const {
  std::shared_lock<std::shared_mutex> lk{rmtx_};

  Zxid        zxid;
  std::time_t timestamp;
  uint64_t    counter;

  if (auto r = read_meta(); !r)
    return ERROR(r.error());
  else
    std::tie(zxid, timestamp, counter) = r.value();

  auto prefix = to_mem(INT(zxid));

  ZkState state{zxid, timestamp, counter};

  if (auto r = load_strings(prefix, state); !r)
    return r.error();
  else if (r.value() == false)
    return system_error_code(EPROTO);

  if (auto r = load_nodes(prefix, state); !r)
    return r.error();
  else if (r.value() == false)
    return system_error_code(EPROTO);

  if (auto r = load_ttl_nodes(prefix, state); !r)
    return r.error();
  else if (r.value() == false)
    return system_error_code(EPROTO);

  if (auto r = load_sessions(prefix, state); !r)
    return r.error();
  else if (r.value() == false)
    return system_error_code(EPROTO);

  return state;
}

Result<bool> RaftLog::load(std::array<uint8_t, 8> const&                 prefix,
                           MDB_dbi                                       dbi,
                           std::function<Result<bool>(MDB_val, MDB_val)> func) const {
  assert(rmtx_.try_lock() == false);

  MDB_txn* txn;
  if (auto rc = txn_rbegin(logs_env_, &txn); rc != 0)
    return system_error_code(rc);

  MDB_cursor* cursor;
  if (int rc = cursor_open(txn, dbi, &cursor); rc != 0) {
    txn_abort(txn);
    return system_error_code(rc);
  }

  MDB_val key{.mv_size = prefix.size(), .mv_data = const_cast<uint8_t*>(prefix.data())};
  MDB_val val;

  int rc;
  if (rc = cursor_get(cursor, &key, &val, MDB_SET_RANGE); rc != 0 && rc != MDB_NOTFOUND) {
    cursor_close(cursor);
    txn_abort(txn);
    return system_error_code(rc);
  }

  while (rc == 0 && std::memcmp(prefix.data(), key.mv_data, prefix.size()) == 0) {
    if (!func(key, val)) {
      cursor_close(cursor);
      txn_abort(txn);
      return false;
    }

    rc = cursor_get(cursor, &key, &val, MDB_NEXT);
  }

  if (rc != MDB_NOTFOUND) {
    cursor_close(cursor);
    txn_abort(txn);
    return system_error_code(rc);
  }

  cursor_close(cursor);
  txn_abort(txn);

  return true;
}

template <typename K, typename V>
Result<void> RaftLog::save(std::array<uint8_t, 8> const& prefix,
                           MDB_dbi                       dbi,
                           ZkState const&                snapshot,
                           size_t const                  offset,
                           size_t const                  limit) {
  assert(wmtx_.try_lock() == false);

  MDB_txn* txn;
  if (auto rc = txn_wbegin(logs_env_, &txn); rc != 0)
    return system_error_code(rc);

  size_t idx = 0;
  size_t n   = 0;
  int    rc  = 0;

  snapshot.for_each<K, V>([&](K const& k, V const& v) {
    if (rc == 0 && idx >= offset && n < limit) {
      auto key_str = to_mem(prefix, k);
      auto val_str = to_val(v);

      MDB_val key{.mv_size = key_str.size(), .mv_data = key_str.data()};
      MDB_val val{.mv_size = val_str.size(), .mv_data = val_str.data()};

      rc = put(txn, dbi, &key, &val);
      ++n;
    }
    ++idx;
  });

  if (rc == 0) {
    if (rc = txn_commit(txn); rc != 0)
      return system_error_code(rc);
    return ok();
  }

  if (rc == MDB_MAP_FULL) {
    txn_abort(txn);
    if (auto r = resize_logs(); !r)
      return r;
    return save<K, V>(prefix, dbi, snapshot, offset, limit);
  }

  if (rc == MDB_TXN_FULL) {
    txn_abort(txn);

    if (auto r = save<K, V>(prefix, dbi, snapshot, offset, limit >> 1); !r)
      return r;
    return save<K, V>(prefix, dbi, snapshot, offset + (limit >> 1), (limit + 1) >> 1);
  }
  return system_error_code(rc);
}

Result<void> RaftLog::save_strings(std::array<uint8_t, 8> const& prefix,
                                   ZkState const&                snapshot,
                                   size_t const                  offset,
                                   size_t const                  limit) {
  assert(wmtx_.try_lock() == false);
  return save<ZkState::PathId, string_view>(prefix, strings_, snapshot, offset, limit);
}

Result<bool> RaftLog::load_strings(std::array<uint8_t, 8> const& prefix, ZkState& state) const {
  assert(rmtx_.try_lock() == false);

  auto func = [&state](MDB_val key, MDB_val val) -> Result<bool> {
    auto id = std::get<ZkState::PathId>(
        from_mem<ZkState::PathId>(reinterpret_cast<uint8_t const*>(key.mv_data) + 8U));
    auto str = string_view{reinterpret_cast<char*>(val.mv_data), val.mv_size};

    return state.add_string(id, str);
  };
  return load(prefix, strings_, func);
}

Result<void> RaftLog::save_nodes(std::array<uint8_t, 8> const& prefix,
                                 ZkState const&                snapshot,
                                 size_t const                  offset,
                                 size_t const                  limit) {
  assert(wmtx_.try_lock() == false);
  return save<ZkState::Path, ZkState::NodeData>(prefix, nodes_, snapshot, offset, limit);
}

Result<bool> RaftLog::load_nodes(std::array<uint8_t, 8> const& prefix, ZkState& state) const {
  assert(rmtx_.try_lock() == false);

  auto func = [&state](MDB_val key, MDB_val val) -> Result<bool> {
    auto path =
        path_from_mem(reinterpret_cast<uint8_t const*>(key.mv_data) + 8U, key.mv_size - 8ULL);
    auto data = string_view{reinterpret_cast<char*>(val.mv_data), val.mv_size};

    if (auto r = from_encoded_string<ZkState::NodeData>(data); !r)
      return r.error();
    else
      return state.add_node(std::move(path), std::move(r.value()));
  };
  return load(prefix, nodes_, func);
}

Result<void> RaftLog::save_ttl_nodes(std::array<uint8_t, 8> const& prefix,
                                     ZkState const&                snapshot,
                                     size_t const                  offset,
                                     size_t const                  limit) {
  assert(wmtx_.try_lock() == false);
  return save<ZkState::Path, std::time_t>(prefix, ttl_nodes_, snapshot, offset, limit);
}

Result<bool> RaftLog::load_ttl_nodes(std::array<uint8_t, 8> const& prefix, ZkState& state) const {
  assert(rmtx_.try_lock() == false);

  auto func = [&state](MDB_val key, MDB_val val) -> Result<bool> {
    auto path =
        path_from_mem(reinterpret_cast<uint8_t const*>(key.mv_data) + 8U, key.mv_size - 8ULL);
    auto time =
        std::get<uint64_t>(from_mem<uint64_t>(reinterpret_cast<uint8_t const*>(val.mv_data)));

    return state.add_ttl_node(std::move(path), static_cast<std::time_t>(time));
  };
  return load(prefix, ttl_nodes_, func);
}

Result<void> RaftLog::save_sessions(std::array<uint8_t, 8> const& prefix,
                                    ZkState const&                snapshot,
                                    size_t const                  offset,
                                    size_t const                  limit) {
  assert(wmtx_.try_lock() == false);
  return save<uint64_t, ZkState::Session<ZkState::Path>>(
      prefix, sessions_, snapshot, offset, limit);
}

Result<bool> RaftLog::load_sessions(std::array<uint8_t, 8> const& prefix, ZkState& state) const {
  assert(rmtx_.try_lock() == false);

  auto func = [&state](MDB_val key, MDB_val val) -> Result<bool> {
    auto session_id =
        std::get<uint64_t>(from_mem<uint64_t>(reinterpret_cast<uint8_t const*>(key.mv_data) + 8U));

    auto data = string_view{reinterpret_cast<char*>(val.mv_data), val.mv_size};

    if (auto r = from_encoded_string<ZkState::Session<ZkState::Path>>(data); !r)
      return r.error();
    else
      return state.add_session(session_id, std::move(r.value()));
  };
  return load(prefix, sessions_, func);
}

Result<void> RaftLog::save_meta(std::array<uint8_t, 8> prefix, ZkState const& snapshot) {
  assert(wmtx_.try_lock() == false);

  MDB_txn* txn;
  if (auto rc = txn_wbegin(logs_env_, &txn); rc != 0)
    return system_error_code(rc);

  // Insert the curr_zxid
  {
    assert(prefix == to_mem(INT(snapshot.curr_zxid())));

    MDB_val key = index_key();
    MDB_val val{.mv_size = prefix.size(), .mv_data = prefix.data()};

    if (auto rc = put(txn, meta_, &key, &val); rc != 0) {
      txn_abort(txn);

      if (rc == MDB_MAP_FULL) {
        if (auto r = resize_logs(); !r)
          return r;

        return save_meta(prefix, snapshot);
      }
      return system_error_code(rc);
    }
  }

  // Insert the timestamp
  {
    auto val_str = to_mem(u8(snapshot.timestamp()));

    MDB_val key = timestamp_key();
    MDB_val val{.mv_size = val_str.size(), .mv_data = val_str.data()};

    if (auto rc = put(txn, meta_, &key, &val); rc != 0) {
      txn_abort(txn);
      if (rc == MDB_MAP_FULL) {
        if (auto r = resize_logs(); !r)
          return r;

        return save_meta(prefix, snapshot);
      }
      return system_error_code(rc);
    }
  }

  // Insert the counter
  {
    auto val_str = to_mem(u8(snapshot.counter()));

    MDB_val key = counter_key();
    MDB_val val{.mv_size = val_str.size(), .mv_data = val_str.data()};

    if (auto rc = put(txn, meta_, &key, &val); rc != 0) {
      txn_abort(txn);
      if (rc == MDB_MAP_FULL) {
        if (auto r = resize_logs(); !r)
          return r;

        return save_meta(prefix, snapshot);
      }
      return system_error_code(rc);
    }
  }

  if (auto rc = txn_commit(txn); rc != 0)
    return system_error_code(rc);

  return ok();
}

Result<std::tuple<Zxid, std::time_t, uint64_t>> RaftLog::read_meta() const {
  assert(rmtx_.try_lock() == false);

  Zxid        zxid;
  std::time_t time;
  uint64_t    counter;

  MDB_txn* txn;
  if (auto rc = txn_rbegin(logs_env_, &txn); rc != 0)
    return system_error_code(rc);

  // Read the curr_zxid
  {
    MDB_val key = index_key();
    MDB_val val;

    if (auto rc = get(txn, meta_, &key, &val); rc != 0) {
      txn_abort(txn);
      return system_error_code(rc);
    }
    zxid = Zxid{std::get<Zxid::value_type>(
        from_mem<Zxid::value_type>(reinterpret_cast<uint8_t const*>(val.mv_data)))};
  }

  // Read the timestamp
  {
    MDB_val key = timestamp_key();
    MDB_val val;

    if (auto rc = get(txn, meta_, &key, &val); rc != 0) {
      txn_abort(txn);
      return system_error_code(rc);
    }
    time = static_cast<std::time_t>(
        std::get<uint64_t>(from_mem<uint64_t>(reinterpret_cast<uint8_t const*>(val.mv_data))));
  }

  // Read the counter
  {
    MDB_val key = counter_key();
    MDB_val val;

    if (auto rc = get(txn, meta_, &key, &val); rc != 0) {
      txn_abort(txn);
      return system_error_code(rc);
    }
    counter = std::get<uint64_t>(from_mem<uint64_t>(reinterpret_cast<uint8_t const*>(val.mv_data)));
  }
  txn_abort(txn);

  return std::make_tuple(zxid, time, counter);
}

Result<void> RaftLog::erase_all_except(MDB_dbi dbi, std::array<uint8_t, 8> const& prefix) {
  assert(wmtx_.try_lock() == false);

  MDB_txn* txn;
  if (auto rc = txn_wbegin(logs_env_, &txn); rc != 0)
    return system_error_code(rc);

  MDB_cursor* cursor;
  if (auto rc = cursor_open(txn, dbi, &cursor); rc != 0) {
    txn_abort(txn);
    return system_error_code(rc);
  }

  MDB_val key;
  MDB_val val;

  MDB_cursor_op op = MDB_FIRST;

  int rc;
  while ((rc = cursor_get(cursor, &key, &val, op)) == 0) {
    if (key.mv_size < 8 || std::memcmp(key.mv_data, prefix.data(), prefix.size()) != 0) {
      if (rc = cursor_del(cursor); rc != 0) {
        txn_abort(txn);
        return system_error_code(rc);
      }
    }
    op = MDB_NEXT;
  }

  if (rc != 0 && rc != MDB_NOTFOUND) {
    txn_abort(txn);
    return system_error_code(rc);
  }

  cursor_close(cursor);
  if (rc = txn_commit(txn); rc != 0)
    return system_error_code(rc);

  return ok();
}

Result<void> RaftLog::delete_older_entries(RaftIndex idx) {
  assert(wmtx_.try_lock() == false);

  MDB_txn* txn;
  if (auto rc = txn_wbegin(logs_env_, &txn); rc != 0)
    return system_error_code(rc);

  MDB_cursor* cursor;
  if (auto rc = cursor_open(txn, logs_, &cursor); rc != 0) {
    txn_abort(txn);
    return system_error_code(rc);
  }

  auto key_bytes = to_mem(idx, RaftTerm{0});

  MDB_val key;
  MDB_val val;

  MDB_cursor_op op = MDB_FIRST;

  int rc;
  while ((rc = cursor_get(cursor, &key, &val, op)) == 0) {
    if (std::memcmp(key.mv_data, key_bytes.data(), key_bytes.size()) < 0) {
      if (rc = cursor_del(cursor); rc != 0) {
        cursor_close(cursor);
        txn_abort(txn);
        return system_error_code(rc);
      }
    } else
      break;
    op = MDB_NEXT;
  }
  cursor_close(cursor);

  if (rc != 0 && rc != MDB_NOTFOUND) {
    txn_abort(txn);
    return system_error_code(rc);
  }

  if (rc = txn_commit(txn); rc != 0)
    return system_error_code(rc);

  return ok();
}

Result<void> RaftLog::resize_logs() {
  assert(wmtx_.try_lock() == false);

  if (int rc = env_set_mapsize(logs_env_, mmap_size_ << 2); rc != 0)
    return system_error_code(rc);
  mmap_size_ <<= 2;

  return ok();
}

Result<bool> RaftLog::reset_entries(RaftIndex prev_log_idx, RaftTerm prev_log_term) {
  assert(wmtx_.try_lock() == false);

  MDB_txn* txn;
  if (auto rc = txn_wbegin(logs_env_, &txn); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("being_txn failed {}", strerror(rc)));

  MDB_cursor* cursor;
  if (auto rc = cursor_open(txn, logs_, &cursor); rc != 0) {
    txn_abort(txn);
    return ERROR_MSG(system_error_code(rc), fn::format("cursor_open failed {}", strerror(rc)));
  }

  auto    key_bytes = to_mem(prev_log_idx, RaftTerm{0});
  MDB_val key{.mv_size = key_bytes.size(), .mv_data = key_bytes.data()};
  MDB_val val;

  if (auto rc = cursor_get(cursor, &key, &val, MDB_SET_RANGE); rc != 0) {
    cursor_close(cursor);
    txn_abort(txn);

    if (rc == MDB_NOTFOUND)
      return false;
    return ERROR_MSG(system_error_code(rc), fn::format("cursor_get failed {}", strerror(rc)));
  }

  auto [idx, term] = from_mem<RaftIndex, RaftTerm>(reinterpret_cast<uint8_t const*>(key.mv_data));
  if (idx != prev_log_idx || term != prev_log_term) {
    LOG_ERROR << "idx  (" << idx.value() << " vs " << prev_log_idx.value() << ") or term  ("
              << term.value() << " vs " << prev_log_term.value() << ")";
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
  std::unique_lock<std::mutex> lk{wmtx_};

  if (delete_entries) {
    if (auto r = reset_entries(prev_log_idx, prev_log_term); !r) {
      return ERROR_MSG(r.error(), "reset_entries failed");
    } else if (r.value() == false) {
      LOG_ERROR << "The prev did not match";
      return false;
    }
  } else {
    if (auto r = last_entry(); !r)
      return ERROR_MSG(r.error(), "last_entry failed");
    else if (r.value() != std::make_tuple(prev_log_term, prev_log_idx))
      return false;
  }

  if (auto r = append_entries(prev_log_idx + 1U, entries); !r)
    return ERROR_MSG(r.error(), "append entries failed");
  return true;
}

Result<void> RaftLog::append_entries(RaftIndex const idx, vector<Entry> const& entries) {
  assert(wmtx_.try_lock() == false);

  MDB_txn* txn;
  if (auto rc = txn_wbegin(logs_env_, &txn); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("txn_begin failed {}", strerror(rc)));

  size_t n = 0;
  for (Entry const& entry : entries) {
    auto new_key_bytes  = to_mem(idx + (n++), entry.term);
    auto new_val_string = to_encoded_string(entry);

    MDB_val key{.mv_size = new_key_bytes.size(), .mv_data = new_key_bytes.data()};
    MDB_val val{.mv_size = new_val_string.size() + 1U, .mv_data = new_val_string.data()};

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

      auto mid = entries.begin() + entries.size() / 2;

      vector<Entry> left{entries.begin(), mid};
      vector<Entry> right{mid, entries.end()};

      assert(entries.size() == (left.size() + right.size()));

      if (auto r = append_entries(idx, left); !r)
        return r;
      return append_entries(idx + left.size(), right);
    }

    if (rc != 0) {
      txn_abort(txn);
      return ERROR_MSG(system_error_code(rc), fn::format("put failed {}", strerror(rc)));
    }
  }

  if (int rc = txn_commit(txn); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("txn_commit failed {}", strerror(rc)));

  return ok();
}

Result<RaftTerm> RaftLog::set_current_term(RaftTerm term) {
  std::unique_lock<std::mutex> lk{mtx_};

  MDB_txn* txn;
  if (int rc = txn_wbegin(term_env_, &txn); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("txn_begin failed : {}", strerror(rc)));

  auto term_bytes = to_mem(INT(term));

  MDB_val tkey = term_key();
  MDB_val vkey = vote_key();
  MDB_val val;

  if (int rc = get(txn, term_, &tkey, &val); rc != 0) {
    txn_abort(txn);
    return ERROR_MSG(system_error_code(rc), fn::format("get(term) failed : {}", strerror(rc)));
  }

  if (std::memcmp(val.mv_data, term_bytes.data(), term_bytes.size()) < 0) {
    val.mv_size = term_bytes.size();
    val.mv_data = term_bytes.data();
    if (int rc = put(txn, term_, &tkey, &val); rc != 0) {
      txn_abort(txn);
      return ERROR_MSG(system_error_code(rc), fn::format("put(term) failed : {}", strerror(rc)));
    }
    if (int rc = del(txn, term_, &vkey); rc != 0 && rc != MDB_NOTFOUND) {
      txn_abort(txn);
      return ERROR_MSG(system_error_code(rc), fn::format("del(vote) failed : {}", strerror(rc)));
    }
    if (int rc = txn_commit(txn); rc != 0) {
      return ERROR_MSG(system_error_code(rc),
                       fn::format("txn_commit(txn) failed : {}", strerror(rc)));
    }
    return term;
  }
  txn_abort(txn);

  return RaftTerm{std::get<RaftTerm::value_type>(
      from_mem<RaftTerm::value_type>(reinterpret_cast<uint8_t const*>(val.mv_data)))};
}

Result<bool> RaftLog::vote(RaftTerm term, RaftNodeId node) {
  std::unique_lock<std::mutex> lk{mtx_};

  MDB_txn* txn;
  if (int rc = txn_wbegin(term_env_, &txn); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("txn_begin failed : {}", strerror(rc)));

  auto term_bytes = to_mem(INT(term));
  auto node_bytes = to_mem(INT(node));

  MDB_val tkey = term_key();
  MDB_val vkey = vote_key();
  MDB_val val;

  if (int rc = get(txn, term_, &tkey, &val); rc != 0) {
    txn_abort(txn);
    return ERROR_MSG(system_error_code(rc), fn::format("get(term) failed : {}", strerror(rc)));
  }

  if (std::memcmp(val.mv_data, term_bytes.data(), term_bytes.size()) != 0) {
    LOG_ERROR << "The term does not match " << INT(term) << " vs "
              << std::get<RaftTerm::value_type>(
                     from_mem<RaftTerm::value_type>(reinterpret_cast<uint8_t*>(val.mv_data)));

    txn_abort(txn);
    return false;
  }

  int rc = get(txn, term_, &vkey, &val);
  if (rc != 0 && rc != MDB_NOTFOUND) {
    txn_abort(txn);
    return ERROR_MSG(system_error_code(rc), fn::format("get(vote) failed : {}", strerror(rc)));
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
    return ERROR_MSG(system_error_code(rc), fn::format("put(vote) failed : {}", strerror(rc)));
  }

  if (int rc = txn_commit(txn); rc != 0) {
    return ERROR_MSG(system_error_code(rc),
                     fn::format("txn_commit(txn) failed : {}", strerror(rc)));
  }
  return true;
}

Result<RaftTerm> RaftLog::current_term() const {
  MDB_txn* txn;
  if (int rc = txn_rbegin(term_env_, &txn); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("txn_begin failed : {}", strerror(rc)));

  MDB_val tkey = term_key();
  MDB_val val;

  if (int rc = get(txn, term_, &tkey, &val); rc != 0) {
    txn_abort(txn);
    return ERROR_MSG(system_error_code(rc), fn::format("get(term) failed : {}", strerror(rc)));
  }
  auto result = RaftTerm{std::get<RaftTerm::value_type>(
      from_mem<RaftTerm::value_type>(reinterpret_cast<uint8_t const*>(val.mv_data)))};

  txn_abort(txn);
  return result;
}

Result<std::tuple<RaftTerm, RaftIndex>> RaftLog::entry(MDB_cursor_op op) const {
  std::shared_lock<std::shared_mutex> lk{rmtx_};

  MDB_txn* txn;
  if (int rc = txn_rbegin(logs_env_, &txn); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("txn_begin failed : {}", strerror(rc)));

  MDB_cursor* cursor;
  if (int rc = cursor_open(txn, logs_, &cursor); rc != 0) {
    txn_abort(txn);
    return ERROR_MSG(system_error_code(rc),
                     fn::format("cursor_open(logs) failed : {}", strerror(rc)));
  }

  MDB_val key;
  MDB_val val;

  if (int rc = cursor_get(cursor, &key, &val, op); rc != 0) {
    cursor_close(cursor);
    txn_abort(txn);

    /// MDB_NOTFOUND is not possible.
    return ERROR_MSG(system_error_code(rc),
                     fn::format("cursor_get(logs) failed : {}", strerror(rc)));
  }

  auto [idx, term] = from_mem<RaftIndex, RaftTerm>(reinterpret_cast<uint8_t const*>(key.mv_data));

  cursor_close(cursor);
  txn_abort(txn);
  return std::make_tuple(term, idx);
}

Result<std::optional<vector<Entry>>> RaftLog::entries_from(RaftIndex const idx,
                                                           size_t          count) const {
  auto lk = std::shared_lock<std::shared_mutex>{rmtx_};

  MDB_txn* txn;
  if (int rc = txn_rbegin(logs_env_, &txn); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("txn_begin failed : {}", strerror(rc)));

  MDB_cursor* cursor;
  if (int rc = cursor_open(txn, logs_, &cursor); rc != 0) {
    txn_abort(txn);
    return ERROR_MSG(system_error_code(rc),
                     fn::format("cursor_open(logs) failed : {}", strerror(rc)));
  }

  auto key_bytes = to_mem(idx, RaftTerm{0});

  MDB_val key{.mv_size = key_bytes.size(), .mv_data = key_bytes.data()};
  MDB_val val;

  int rc = 0;
  if (rc = cursor_get(cursor, &key, &val, MDB_SET_RANGE); rc != 0 && rc != MDB_NOTFOUND) {
    cursor_close(cursor);
    txn_abort(txn);
    return ERROR_MSG(system_error_code(rc),
                     fn::format("cursor_get(logs) failed : {}", strerror(rc)));
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
    assert(index == (idx + result.size()));

    if (auto r = from_encoded_string<Entry>(
            string{reinterpret_cast<char const*>(val.mv_data), val.mv_size});
        r)
      result.emplace_back(std::move(r.value()));
    else {
      cursor_close(cursor);
      txn_abort(txn);
      return r.error();
    }

    rc = cursor_get(cursor, &key, &val, MDB_NEXT);
  } while ((result.size() < count) && (rc == 0));

  cursor_close(cursor);
  txn_abort(txn);
  return result;
}

Result<std::optional<RaftTerm>> RaftLog::get_term(RaftIndex idx) const {
  auto lk = std::shared_lock<std::shared_mutex>{rmtx_};

  MDB_txn* txn;
  if (int rc = txn_rbegin(logs_env_, &txn); rc != 0)
    return ERROR_MSG(system_error_code(rc), fn::format("txn_begin failed : {}", strerror(rc)));

  MDB_cursor* cursor;
  if (int rc = cursor_open(txn, logs_, &cursor); rc != 0) {
    txn_abort(txn);
    return ERROR_MSG(system_error_code(rc),
                     fn::format("cursor_open(logs) failed : {}", strerror(rc)));
  }

  auto key_bytes = to_mem(idx, RaftTerm{0});

  MDB_val key{.mv_size = key_bytes.size(), .mv_data = key_bytes.data()};
  MDB_val val;

  int rc = 0;
  if (rc = cursor_get(cursor, &key, &val, MDB_SET_RANGE); rc != 0 && rc != MDB_NOTFOUND) {
    cursor_close(cursor);
    txn_abort(txn);
    return ERROR_MSG(system_error_code(rc),
                     fn::format("cursor_get(logs) failed : {}", strerror(rc)));
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
int RaftLog::env_set_mapsize(MDB_env* env, size_t size) {
  return mdb_env_set_mapsize(env, size);
}
int RaftLog::env_set_maxdbs(MDB_env* env, unsigned dbs) {
  return mdb_env_set_maxdbs(env, dbs);
}
int RaftLog::env_create(MDB_env** env) {
  return mdb_env_create(env);
}
int RaftLog::env_open(MDB_env* env, char const* filename) {
  return mdb_env_open(env, filename, MDB_NOSUBDIR, 0664);
}
void RaftLog::env_close(MDB_env* env) {
  return mdb_env_close(env);
}
int RaftLog::env_copy(MDB_env* env, int fd) const {
  return mdb_env_copyfd2(env, fd, MDB_CP_COMPACT);
}
int RaftLog::dbi_open(MDB_txn* txn, char const* name, MDB_dbi* dbi) {
  return mdb_dbi_open(txn, name, MDB_CREATE, dbi);
}
int RaftLog::txn_rbegin(MDB_env* env, MDB_txn** txn) const {
  return mdb_txn_begin(env, nullptr, MDB_RDONLY, txn);
}
int RaftLog::txn_wbegin(MDB_env* env, MDB_txn** txn) {
  return mdb_txn_begin(env, nullptr, 0, txn);
}
int RaftLog::txn_commit(MDB_txn* txn) const {
  return mdb_txn_commit(txn);
}
void RaftLog::txn_abort(MDB_txn* txn) const {
  return mdb_txn_abort(txn);
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
  return mdb_cursor_open(txn, dbi, cursor);
}
int RaftLog::cursor_get(MDB_cursor* cursor, MDB_val* key, MDB_val* val, MDB_cursor_op op) const {
  return mdb_cursor_get(cursor, key, val, op);
}
int RaftLog::cursor_del(MDB_cursor* cursor) {
  return mdb_cursor_del(cursor, 0);
}
void RaftLog::cursor_close(MDB_cursor* cursor) const {
  return mdb_cursor_close(cursor);
}

