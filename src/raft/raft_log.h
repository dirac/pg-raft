#pragma once

#include "src/base/base.h"
#include "src/raft/raft_state.h"
#include "src/raft/raft_types.h"

// Ported from sparrowdb lib/servers/zqlite/raft_log.hpp. The snapshot is the RaftState (saved in
// the meta database) instead of the ZqState databases; install_snapshot() replaces the log with
// the entries of an InstallSnapshot.
class RaftLog {
 public:
  constexpr static size_t kLmdbPageSize = 16384U;

  /// With sync_commits every commit of the logs database is synced to the disk, otherwise a thread
  /// syncs the commits in the background (within a second).
  RaftLog(std::filesystem::path dir, bool sync_commits = false, bool no_sync = false);

  virtual ~RaftLog() { close(); }

  /// Initialize term database. It stores the current term and voted for information.
  Result<RaftTerm> open_term_db();

  /// Initialize logs database, on error it will delete existing database and create a new one.
  Result<void> open_logs_db(unsigned factor = 2U);

  /// Initialize the logs database
  Result<void> open_logs_db(unsigned factor, std::filesystem::path const& logs_file);

  /// Close
  void close();

  /// Delete all entries after the index and append the new entries.
  Result<bool> append_entries(RaftIndex            prev_log_idx,
                              RaftTerm             prev_log_term,
                              vector<Entry> const& entries,
                              bool                 delete_entries);

  /// The term and index of the last log entry.
  Result<std::tuple<RaftTerm, RaftIndex>> last_entry() const;

  /// The term and index of the first log entry.
  Result<std::tuple<RaftTerm, RaftIndex>> first_entry() const;

  /// Returns atmost count entries starting at the index; std::nullopt if the index is before the
  /// first entry.
  Result<std::optional<vector<Entry>>> entries_from(RaftIndex idx, size_t count) const;

  /// Returns atmost count entries from the back (the last first), and the index of the last.
  Result<std::tuple<RaftIndex, vector<Entry>>> latest_entries(size_t count) const;

  /// The term of the entry at the index.
  Result<std::optional<RaftTerm>> get_term(RaftIndex idx) const;

  /// Save the state and truncate the logs before state.log_idx - log_size.
  Result<void> save_snapshot(RaftState const& state, unsigned log_size = 4096);

  /// The saved state.
  Result<RaftState> load_snapshot() const;

  /// Replace the log with the entries (log_idx, log_idx + entries.size()] and save the state.
  Result<void> install_snapshot(RaftIndex log_idx, vector<Entry> const& entries, RaftState state);

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

  Result<std::optional<vector<Entry>>> get_entries(RaftIndex     idx,
                                                   size_t        count,
                                                   MDB_cursor_op op) const;

  virtual char const* strerror(int rc) const;

  virtual int  env_set_checksum(MDB_env* env, MDB_sum_func* func, unsigned int size);
  virtual int  env_set_mapsize(MDB_env* env, size_t size);
  virtual int  env_set_pagesize(MDB_env* env, size_t size);
  virtual int  env_set_maxdbs(MDB_env* env, unsigned dbs);
  virtual int  env_create(MDB_env** env);
  virtual int  env_open(MDB_env* env, char const* filename, unsigned int flags);
  virtual void env_close(MDB_env* env);
  virtual int  env_sync(MDB_env* env);
  virtual int  txn_wbegin(MDB_env* env, MDB_txn** txn);
  virtual int  txn_rbegin(MDB_env* env, MDB_txn** txn) const;

  /// Non-virtual: always clears the thread-local open-txn tracking before dispatching to
  /// txn_commit_impl(), so a subclass overriding the impl (e.g. to inject a failure) can't
  /// bypass the bookkeeping and make ScopedHandleCheck see a leaked txn that was actually
  /// handed off here.
  int          txn_commit(MDB_txn* txn) const;
  virtual int  txn_commit_impl(MDB_txn* txn) const;
  void         txn_abort(MDB_txn* txn) const;
  virtual void txn_abort_impl(MDB_txn* txn) const;

  virtual int dbi_open(MDB_txn* txn, char const* name, MDB_dbi* dbi);
  virtual int get(MDB_txn* txn, MDB_dbi dbi, MDB_val* key, MDB_val* val) const;
  virtual int put(MDB_txn* txn, MDB_dbi dbi, MDB_val* key, MDB_val* val);
  virtual int del(MDB_txn* txn, MDB_dbi dbi, MDB_val* key);
  virtual int cursor_open(MDB_txn* txn, MDB_dbi dbi, MDB_cursor** cursor) const;
  virtual int cursor_get(MDB_cursor* cursor, MDB_val* key, MDB_val* val, MDB_cursor_op op) const;
  virtual int cursor_del(MDB_cursor* cursor);

  void         cursor_close(MDB_cursor* cursor) const;
  virtual void cursor_close_impl(MDB_cursor* cursor) const;

 private:
  void start_sync();
  void stop_sync();

  /// save_snapshot() with the mutex of the writes held.
  Result<void> save_snapshot_locked(RaftState const& state, unsigned log_size);

  Result<void> save_meta(RaftState const& state);

  Result<RaftState> read_meta() const;

  /// Delete all entries after prev_log_idx, if the term matches.
  Result<bool> reset_entries(RaftIndex prev_log_idx, RaftTerm prev_log_term);

  /// Delete all entries.
  Result<void> delete_all_entries();

  Result<void> append_entries(RaftIndex idx, vector<Entry> const& entries);

  Result<void> resize_logs();

  /// Delete log entries before the index.
  Result<void> delete_older_entries(RaftIndex idx);

  /// Directory to store the databases.
  std::filesystem::path dir_;

  /// Sync every commit of the logs database.
  bool const sync_commits_;
  bool const no_sync_;  // Testing only: nothing is synced.

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

  MDB_dbi logs_;  // {RaftIndex, RaftTerm} -> Entry
  MDB_dbi meta_;  // "state" -> RaftState

  size_t mmap_size_{0x1ULL << 30U};

  // Sync threads
  mutable std::atomic_flag          sync_stop_;    // Asks the sync thread to exit.
  mutable std::atomic_flag          sync_failed_;  // A sync failed; writes are refused.
  mutable std::counting_semaphore<> sync_sem_{0};
  std::unique_ptr<std::thread>      sync_thread_;
};

template <typename V>
std::array<uint8_t, sizeof(V)> to_mem(V v) {
  static_assert(std::is_integral_v<V>);

  if constexpr (std::is_signed_v<V>) {
    using U = std::make_unsigned_t<V>;

    // Flip the highest bit.
    U m = (U{1} << ((sizeof(U) * 8) - 1));
    U u = static_cast<U>(v);

    return to_mem<U>(u ^ m);
  } else {
    std::array<uint8_t, sizeof(V)> result;
    for (unsigned i = 0; i < sizeof(V); ++i, v = static_cast<V>(v >> 8U))
      result.at(sizeof(V) - i - 1U) = static_cast<uint8_t>(v & 0xFFU);

    return result;
  }
}

template <typename V>
std::tuple<V, uint8_t const*> from_mem(uint8_t const* ptr) {
  static_assert(std::is_integral_v<V>);

  if constexpr (std::is_signed_v<V>) {
    using U = std::make_unsigned_t<V>;

    auto [u, p] = from_mem<U>(ptr);

    // Flip the highest bit.
    U m = (U{1} << ((sizeof(U) * 8) - 1));

    return {static_cast<V>(u ^ m), p};
  } else {
    V v = 0;
    for (unsigned i = 0; i < sizeof(V); ++i) {
      v = static_cast<V>(v << 8U);
      v |= static_cast<V>(*ptr);
      ++ptr;
    }

    return {v, ptr};
  }
}
