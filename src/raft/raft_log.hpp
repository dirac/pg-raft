#ifndef LIB_SERVERS_ZOOKEEPER_RAFT_LOG_HPP_
#define LIB_SERVERS_ZOOKEEPER_RAFT_LOG_HPP_

#include "lib/base/base.hpp"
#include "lib/servers/zookeeper/raft_types.hpp"
#include "lib/servers/zookeeper/zk_state.hpp"

namespace banyandb::servers::zookeeper {

class RaftLog {
 public:
  RaftLog(std::filesystem::path dir);

  virtual ~RaftLog() { close(); }

  /// Initialize term database. It stores the current term and voted for information.
  Result<RaftTerm> open_term_db();

  /// Initialize logs database.
  Result<void> open_logs_db(unsigned factor = 1U);

  /// Close
  void close() {
    {
      std::unique_lock<std::mutex>        wlk{wmtx_};
      std::unique_lock<std::shared_mutex> rlk{rmtx_};
      if (logs_env_)
        env_close(logs_env_);

      logs_env_ = nullptr;
    }
    {
      std::unique_lock<std::mutex> lk{mtx_};
      if (term_env_)
        env_close(term_env_);
      term_env_ = nullptr;
    }
  }

  /// Delete all entries after the index and append the new entries.
  Result<bool> append_entries(RaftIndex            prev_log_idx,
                              RaftTerm             prev_log_term,
                              vector<Entry> const& entries,
                              bool                 delete_entries);

  /// The term and index of the last log entry.
  Result<std::tuple<RaftTerm, RaftIndex>> last_entry() const { return entry(MDB_LAST); }

  /// The term and index of the first log entry.
  Result<std::tuple<RaftTerm, RaftIndex>> first_entry() const { return entry(MDB_FIRST); }

  /// Returns atmost count entries starting at the index.
  Result<std::optional<vector<Entry>>> entries_from(RaftIndex idx, size_t count) const;

  /// The term of the entry at the index.
  Result<std::optional<RaftTerm>> get_term(RaftIndex idx) const;

  /// Copy the state and truncate the old logs.
  Result<void> save_snapshot(ZkState snapshot, unsigned log_size = 4096);

  /// Restore snapshot.
  Result<ZkState> load_snapshot() const;

  /// Create an LMDB copy.
  Result<void> take_db_snapshot(std::filesystem::path filename) const;

  /// Apply snapshot
  Result<void> apply_db_snapshot(std::filesystem::path filename);

  /// The current term
  Result<RaftTerm> current_term() const;

  /// Update the term, stored in the filesystem.
  Result<RaftTerm> set_current_term(RaftTerm term);

  /// Vote for a node for the term and set the current term. The vote is stored in the filestsytem.
  Result<bool> vote(RaftTerm term, RaftNodeId node);

  /// Term database file
  virtual std::filesystem::path term_db() const { return dir_ / "term.lmdb"; }

  /// Logs database file
  virtual std::filesystem::path logs_db() const { return dir_ / "logs.lmdb"; }

 protected:
  Result<std::tuple<RaftTerm, RaftIndex>> entry(MDB_cursor_op op) const;

  virtual char const* strerror(int rc) const;

  virtual int  env_set_mapsize(MDB_env* env, size_t size);
  virtual int  env_set_maxdbs(MDB_env* env, unsigned dbs);
  virtual int  env_create(MDB_env** env);
  virtual int  env_open(MDB_env* env, char const* filename);
  virtual void env_close(MDB_env* env);
  virtual int  env_copy(MDB_env* env, int fd) const;
  virtual int  txn_wbegin(MDB_env* env, MDB_txn** txn);
  virtual int  txn_rbegin(MDB_env* env, MDB_txn** txn) const;
  virtual int  txn_commit(MDB_txn* txn) const;
  virtual void txn_abort(MDB_txn* txn) const;
  virtual int  dbi_open(MDB_txn* txn, char const* name, MDB_dbi* dbi);
  virtual int  get(MDB_txn* txn, MDB_dbi dbi, MDB_val* key, MDB_val* val) const;
  virtual int  put(MDB_txn* txn, MDB_dbi dbi, MDB_val* key, MDB_val* val);
  virtual int  del(MDB_txn* txn, MDB_dbi dbi, MDB_val* key);
  virtual int  cursor_open(MDB_txn* txn, MDB_dbi dbi, MDB_cursor** cursor) const;
  virtual int  cursor_get(MDB_cursor* cursor, MDB_val* key, MDB_val* val, MDB_cursor_op op) const;
  virtual int  cursor_del(MDB_cursor* cursor);
  virtual void cursor_close(MDB_cursor* cursor) const;

 private:
  /// Delete all entries after prev_log_idx, if the term matches.
  Result<bool> reset_entries(RaftIndex prev_log_idx, RaftTerm prev_log_term);

  Result<bool> load(std::array<uint8_t, 8> const&                 prefix,
                    MDB_dbi                                       dbi,
                    std::function<Result<bool>(MDB_val, MDB_val)> func) const;

  template <typename K, typename V>
  Result<void> save(std::array<uint8_t, 8> const& prefix,
                    MDB_dbi                       dbi,
                    ZkState const&                snapshot,
                    size_t const                  offset,
                    size_t const                  limit);

  Result<void> save_strings(std::array<uint8_t, 8> const& prefix,
                            ZkState const&                snapshot,
                            size_t                        offset,
                            size_t                        limit);
  Result<bool> load_strings(std::array<uint8_t, 8> const& prefix, ZkState& state) const;

  Result<void> save_nodes(std::array<uint8_t, 8> const& prefix,
                          ZkState const&                snapshot,
                          size_t                        offset,
                          size_t                        limit);
  Result<bool> load_nodes(std::array<uint8_t, 8> const& prefix, ZkState& state) const;

  Result<void> save_ttl_nodes(std::array<uint8_t, 8> const& prefix,
                              ZkState const&                snapshot,
                              size_t                        offset,
                              size_t                        limit);
  Result<bool> load_ttl_nodes(std::array<uint8_t, 8> const& prefix, ZkState& state) const;

  Result<void> save_sessions(std::array<uint8_t, 8> const& prefix,
                             ZkState const&                snapshot,
                             size_t                        offset,
                             size_t                        limit);
  Result<bool> load_sessions(std::array<uint8_t, 8> const& prefix, ZkState& state) const;

  Result<void> save_meta(std::array<uint8_t, 8> prefix, ZkState const& snapshot);

  Result<std::tuple<Zxid, std::time_t, uint64_t>> read_meta() const;

  Result<void> append_entries(RaftIndex const idx, vector<Entry> const& entries);

  Result<void> resize_logs();

  /// Delete log entries before the index.
  Result<void> delete_older_entries(RaftIndex idx);

  /// Erase all entries where the key doesn't start with the prefix.
  Result<void> erase_all_except(MDB_dbi dbi, std::array<uint8_t, 8> const& prefix);

  /// Directory to store the databases.
  std::filesystem::path dir_;

  // Mutexes for logs database
  std::mutex                wmtx_;
  mutable std::shared_mutex rmtx_;

  // Mutex for term database.
  mutable std::mutex mtx_;

  /// Database to store current term and voted for information.
  /// Term database is very small with just two entries, so MDB_* errors are not possible.
  MDB_env* term_env_ = nullptr;
  MDB_dbi  term_;

  /// Database for logs and snapshot.
  MDB_env* logs_env_ = nullptr;

  /// Log db
  MDB_dbi logs_;  // {RaftIndex, RaftTerm} -> Entry

  /// Snapshot databases.
  MDB_dbi meta_;       // Meta data from snapshot.
  MDB_dbi strings_;    // PathId -> value
  MDB_dbi nodes_;      // Path -> NodeData
  MDB_dbi ttl_nodes_;  // Path -> time
  MDB_dbi sessions_;   // session_id -> Session

  size_t mmap_size_{0x1ULL << 30};
};

template <typename V>
std::array<uint8_t, sizeof(V)> to_mem(V v) {
  static_assert(std::is_integral<V>::value);

  if constexpr (std::is_signed_v<V>) {
    using U = std::make_unsigned_t<V>;

    // Flip the highest bit.
    U m = (U{1} << (sizeof(U) * 8 - 1));
    U u = static_cast<U>(v);

    return to_mem<U>(u ^ m);
  }
  std::array<uint8_t, sizeof(V)> result;
  for (unsigned i = 0; i < sizeof(V); ++i, v = v >> 8)
    result.at(sizeof(V) - i - 1U) = static_cast<uint8_t>(v & 0xFF);

  return result;
}

template <typename V>
std::tuple<V, uint8_t const*> from_mem(uint8_t const* ptr) {
  static_assert(std::is_integral<V>::value);

  if constexpr (std::is_signed_v<V>) {
    using U = std::make_unsigned_t<V>;

    auto [u, p] = from_mem<U>(ptr);

    // Flip the highest bit.
    U m = (U{1} << (sizeof(U) * 8 - 1));

    return {static_cast<V>(u ^ m), p};
  }
  V v = 0;
  for (unsigned i = 0; i < sizeof(V); ++i) {
    v = static_cast<V>(v << 8);
    v |= static_cast<V>(*ptr);
    ++ptr;
  }

  return {v, ptr};
}

}  // namespace banyandb::servers::zookeeper

#endif
