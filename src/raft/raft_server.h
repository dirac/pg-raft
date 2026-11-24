#pragma once

#include "src/base/config.h"
#include "src/base/wal_segment.h"
#include "src/net/server.h"
#include "src/raft/match_indexes.h"
#include "src/raft/options.h"
#include "src/raft/peer_connection_mgr.h"
#include "src/raft/raft_client_connection.h"
#include "src/raft/raft_follower_connection.h"
#include "src/raft/ledger.h"
#include "src/raft/raft_peer_connection.h"
#include "src/raft/raft_state.h"
#include "src/raft/raft_types.h"
#include "src/raft/vote_tally.h"

// The raft server:
//
//  - The leader's own WAL interception generates the entries: exec(WalCommand, handler), whose
//    handler is called when the entry is committed.
//  - The state machine is RaftState (see raft_state.h); a RaftObserver is told of each applied
//    entry (e.g. to advance the WalSender to the committed flushed_lsn) and of the role changes
//    (to promote or demote PostgreSQL).
//  - A node without a usable PostgreSQL (RaftState::needs_backup) doesn't start elections.
//  - The log is the ledger (docs/ledger.md): each entry is a record with its WAL bytes, created by
//    the leader and copied byte for byte to the followers. wal_dir is derived from it: every node
//    writes an entry's WAL bytes into its segment when it applies the entry, so wal_dir holds
//    committed WAL only. A checkpoint thread syncs those writes and saves the applied state in
//    |dir|/meta; after a crash the entries after it are applied again. The term and vote are in
//    |dir|/term.
//  - A leader compacts its own log (stepping down to compact would be a PostgreSQL failover).
//  - InstallSnapshot sends the latest entries and their segments; the node then needs a PostgreSQL
//    backup (RaftState::needs_backup), which it fetches over the client channel (backup()).

/// Told of the state machine and role changes, with the server lock held: it must not call the
/// server, and must not block.
struct RaftObserver {
  virtual ~RaftObserver() = default;

  /// The entry at idx was applied; state is the state after it.
  virtual void applied(RaftIndex idx, Entry const& entry, RaftState const& state) {
    (void)idx;
    (void)entry;
    (void)state;
  }

  /// The log was replaced by an InstallSnapshot.
  virtual void installed(RaftState const& state) { (void)state; }

  /// Full leadership in |term|: the node has committed (and applied) every entry it inherited, so
  /// its WAL holds every byte a previous primary acknowledged. Fires once per term.
  virtual void became_leader(RaftTerm term) { (void)term; }

  /// The leader of |term| became a follower (it lost leadership, stepped down or is closing). Its
  /// pending exec() handlers have already been failed with kNotLeader.
  virtual void stepped_down(RaftTerm term) { (void)term; }
};

/// Called when an entry is committed (with its index), or with ErrorCode::kNotLeader.
using ExecHandler = std::function<void(Result<RaftIndex>)>;

class RaftServer : public ClientConnectionApi,
                   public FollowerConnectionApi,
                   public PeerConnectionApi,
                   public Server {
 public:
  /// |dir| holds the ledger, the term and meta files, |wal_dir| the WAL segments (raft/wal in a
  /// node).
  RaftServer(RaftNodeId                     id,
             boost::asio::ip::tcp::acceptor acceptor,
             Config const&                  config,
             std::filesystem::path          dir,
             std::filesystem::path          wal_dir,
             Options                        options,
             shared_ptr<RaftObserver>       observer = nullptr);

  ~RaftServer() override;

  /// Start the server.
  void start() override;
  void close();

  RaftNodeId node_id() const { return self_id_; }

  void on_shutdown() override;

  std::shared_lock<std::shared_mutex> usage_lock() const override {
    auto lk = std::shared_lock<std::shared_mutex>{usage_lock_};
    if (!closed_.test())
      return lk;

    log(INFO, "{}returning null lock", log_prefix_);
    return std::shared_lock<std::shared_mutex>{};
  }

  void periodic();
  void gather_terms();

  /// Function to support stepping down as a leader. A fast way to pass the baton without election
  /// timeouts.
  // 1. Add a tick entry and send an append entries to all.
  // 2. When the server processes AppendEntries response, it checks if the remote node has received
  //    the last tick entry.
  // 3. If so then the first node to respond is sent an election request.
  void stepdown();

  /// Set or clear RaftState::needs_backup, and persist it. While it is set the node doesn't start
  /// elections (nor take one from a RequestElection): it has no PostgreSQL to promote. It still
  /// votes and replicates.
  void set_needs_backup(bool needs_backup);

  /// The node restored a backup that ends at |end|: clears needs_backup, under the server lock, if
  /// the backup reaches the log, i.e. its last segment is at or after first_segment (the standby
  /// replays the backup's WAL, then streams from that segment, which wal_dir has). An
  /// InstallSnapshot during the restore may have moved first_segment past it: then needs_backup
  /// stays set and false is returned, and the node restores a newer backup.
  bool restored_backup(LogSeqNumber end);

  /// The start of the segment that holds the latest backup's end LSN (docs/backup.md §5): from
  /// then on, compaction keeps every entry whose segment is at or after it (and always the last
  /// applied entry), instead of keep_old_logs, so that a node restoring that backup gets the rest
  /// of the WAL through AppendEntries. Set by the node whenever it sees a newer backup.
  void set_wal_keep_from(LogSeqNumber keep_from);

  /// The local PostgreSQL's redo point (its last checkpoint or restartpoint): the WAL from there on
  /// is what it replays after a crash, so wal_gc keeps the segments from its segment on, even
  /// those before the first entry's segment. 0: none.
  void set_wal_gc_floor(uint64_t redo_lsn) {
    if (wal_gc_floor_.exchange(redo_lsn) != redo_lsn) {
      {
        auto lk  = std::unique_lock{gc_mtx_};
        gc_wake_ = true;
      }
      gc_cv_.notify_all();
    }
  }

  /// Connection from client and other peers.
  void new_connection(string peer, socket_type socket) override;

  Config const& config() const { return config_; }

  /// Number of nodes
  size_t nodes() const { return config().servers.size(); }

  /// Does the server think it is the leader?
  bool is_leader() const override { return type_.load() == NodeType::kLeader; }
  bool resigning() const override { return stepping_down(false); }

  /// The leader's PostgreSQL is the primary and accepts connections (set by the node).
  bool serving() const override { return is_leader() && serving_.test(); }
  void set_serving(bool serving) {
    if (serving)
      serving_.test_and_set();
    else
      serving_.clear();
  }

  RaftIndex applied_idx() const override { return applied_idx_.load(std::memory_order_relaxed); }

  /// The PostgreSQL status, polled by the node (see status/PgClient).
  void set_pg_status(std::optional<uint64_t> primary_flushed_lsn,
                     std::optional<uint64_t> standby_applied_lsn) {
    auto lk              = std::unique_lock{pg_status_mtx_};
    primary_flushed_lsn_ = primary_flushed_lsn;
    standby_applied_lsn_ = standby_applied_lsn;
  }
  std::optional<uint64_t> primary_flushed_lsn() const override {
    auto lk = std::unique_lock{pg_status_mtx_};
    return primary_flushed_lsn_;
  }
  std::optional<uint64_t> standby_applied_lsn() const override {
    auto lk = std::unique_lock{pg_status_mtx_};
    return standby_applied_lsn_;
  }

  /// The backups of the PostgreSQL instance (set by the node); without one, every backup fails.
  void set_backup_provider(std::function<void(SnapshotRequest, BackupHandler)> provider) {
    auto lk          = std::unique_lock{backup_mtx_};
    backup_provider_ = std::move(provider);
  }
  void backup(SnapshotRequest request, BackupHandler handler) override;

  /// Append the command to the log (leader only), with |wal|, the WAL bytes it names
  /// (wal_bytes(cmd): a WriteOp's or a FlushOp's, from PostgreSQL's segment; none otherwise); the
  /// record keeps its own copy of them. The handler is called when it is committed, with the
  /// server lock held, or with ErrorCode::kNotLeader.
  void exec(WalCommand cmd, std::span<uint8_t const> wal, ExecHandler handler);
  void exec(WalCommand cmd, ExecHandler handler) { exec(std::move(cmd), {}, std::move(handler)); }

  /// For the tests: a checkpoint now (docs/ledger.md §9.2).
  void checkpoint_for_test() { checkpoint(); }

  /// The WAL segment in wal_dir, created (zero filled) if it doesn't exist.
  Result<shared_ptr<WalSegment>> segment(LogSeqNumber const& lsn) const;

  /// A copy of the state, published under its own mutex after every change, so that a peer
  /// connection can read it without the server lock.
  RaftState applied_state() const override;

  RaftResponse handle_peer_request(RaftNodeId node_id, RaftTerm term, std::monostate) override;

  RaftResponse handle_peer_request(RaftNodeId  candidate,
                                   RaftTerm    term,
                                   RequestVote request) override;

  RaftResponse handle_peer_request(RaftNodeId     candidate,
                                   RaftTerm       term,
                                   RequestPreVote request) override;

  RaftResponse handle_peer_request(RaftNodeId      leader,
                                   RaftTerm        term,
                                   InstallSnapshot request,
                                   Bytes const&    bytes) override;

  RaftResponse handle_peer_request(RaftNodeId    leader,
                                   RaftTerm      term,
                                   AppendEntries request,
                                   Bytes const&  bytes) override;

  RaftResponse handle_peer_request(RaftNodeId      candidate,
                                   RaftTerm        term,
                                   RequestElection request) override;

  void handle_response(shared_ptr<RaftPeerConnection> conn,
                       RaftTerm                       req_term,
                       RaftTerm                       peer_term,
                       std::monostate) override;

  void handle_response(shared_ptr<RaftPeerConnection> conn,
                       RaftTerm                       req_term,
                       RaftTerm                       peer_term,
                       RequestVoteResponse            response) override;

  void handle_response(shared_ptr<RaftPeerConnection> conn,
                       RaftTerm                       req_term,
                       RaftTerm                       peer_term,
                       InstallSnapshotResponse        response) override;

  void handle_response(shared_ptr<RaftPeerConnection> conn,
                       RaftTerm                       req_term,
                       RaftTerm                       peer_term,
                       AppendEntriesResponse          response) override;

  void handle_response(shared_ptr<RaftPeerConnection> conn,
                       RaftTerm                       req_term,
                       RaftTerm                       peer_term,
                       RequestElectionResponse        response) override;

  void handle_response(shared_ptr<RaftPeerConnection> conn,
                       RaftTerm                       req_term,
                       RaftTerm                       peer_term,
                       RequestPreVoteResponse         response) override;

  RaftIndex commit_idx() const { return commit_idx_.load(std::memory_order_relaxed); }

  Ledger const& ledger() const { return ledger_; }

  unsigned random_election_timeout() const {
    return static_cast<unsigned>(random_election_timeout_(rd_));
  }

  /// A file system error. The only sane thing to do is exit.
  [[noreturn]] void system_error(std::error_code ec) const override;

  RaftTerm current_term() const { return current_term_.load(std::memory_order_relaxed); }
  RaftTerm leadership_term() const { return leadership_term_.load(std::memory_order_relaxed); }

  bool has_votes() const {
    auto [votes, _] = votes_.votes_tally();
    return votes > (nodes() >> 1U);
  }
  bool has_lost_election() const {
    auto [votes, responses] = votes_.votes_tally();
    return (responses - votes) > (nodes() >> 1U);
  }

  RaftIndex apply_entries(bool check_delay);

  /// For the tests: compact the log at idx, as periodic() does.
  void compact_for_test(RaftIndex idx) { compact(idx, unique_lock()); }

  /// For the tests: fn(state, ledger, commit_idx) with the lock held, so that nothing is appended
  /// or applied meanwhile.
  template <typename Fn>
  auto inspect_for_test(Fn&& fn) const {
    auto lk = unique_lock();
    return std::forward<Fn>(fn)(state_, ledger_, commit_idx());
  }

 protected:
  /// Create the connection for a newly accepted peer or client socket.
  shared_ptr<Connection> create_connection(string peer, socket_type socket);

 private:
  using UniqueLock = std::unique_lock<std::mutex>;

  // Acquire a lock.
  UniqueLock unique_lock() const { return std::unique_lock<std::mutex>{mtx_}; }

  UniqueLock try_unique_lock() const {
    return std::unique_lock<std::mutex>{mtx_, std::try_to_lock_t{}};
  }

  void on_connect(shared_ptr<RaftPeerConnection> const& conn);

  // We can vote only after receiving the term from majority of the nodes.
  bool can_vote() const { return can_vote_.test(); }

  bool is_candidate() const { return type_.load() == NodeType::kCandidate; }
  bool is_follower() const { return type_.load() == NodeType::kFollower; }

  // Every tick_seconds we add a tick entry if we have not appended a real entry.
  // Returns true if an tick entry was appended.
  bool tick(bool force = false);

  /// |here|: on the calling thread (exec(): its caller waits for the commit anyway, and posting to
  /// the io threads cost a thread wake-up per commit); otherwise posted to the io threads.
  unsigned send_append_entries(bool check_sync_flag, bool here = false);

  unsigned send_request_votes(RaftTerm term);

  RaftTerm update_current_term(RaftTerm term);

  void make_follower(std::source_location loc = std::source_location::current());
  void log_untriggered_election(unsigned now) const;
  void make_candidate(unsigned inc = 1);

  /// The election timeout passed: a PreVote round for current_term() + 1 (make_candidate() once a
  /// majority, this node included, would vote for it).
  void start_pre_vote();

  /// Whether this node heard from a leader of its current term within the minimum election
  /// timeout: it then refuses PreVotes.
  bool hears_leader() const;

  std::atomic<uint64_t> wal_gc_floor_{0};

  // The PreVote round in flight: the proposed term (0: none) and who granted it.
  std::mutex       pre_vote_mtx_;
  RaftTerm         pre_vote_term_{0};
  std::set<RaftNodeId> pre_votes_;

  void make_leader(UniqueLock lk);
  void make_follower(UniqueLock lk, std::source_location loc = std::source_location::current());
  void make_candidate(unsigned inc, UniqueLock lk);

  void set_commit_idx(RaftIndex idx) {
    auto curr = commit_idx();
    while (curr < idx && !commit_idx_.compare_exchange_strong(curr, idx, std::memory_order_relaxed))
      ;
  }

  void update_match_index(RaftNodeId node_id, RaftIndex idx);

  /// The ledger's sync thread: the entries up to |idx| are on this node's disk.
  void on_durable(RaftIndex idx);

  /// Number of entries.
  unsigned count(RaftIndex idx, RaftTerm term) const;

  // 2. When the server processes AppendEntries response, it checks if the remote node has received
  //    the last tick entry.
  bool stepping_down(bool acquire_lock) const;

  // 3. If so then the first response results in sending an election request to the node.
  bool should_send_election_request();

  /// A server that has applied compaction_lag entries since the last compaction saves the state
  /// and truncates the log, if it holds the lock.
  void compact(RaftIndex idx, UniqueLock lk);

  /// Compaction, with the server lock held, without I/O beyond a few small record reads: sets the
  /// state (first_idx, first_segment), detaches the ledger files before the new first entry, and
  /// hands the rest to the WAL deletion thread (DropJob): it saves the meta file, then unlinks the
  /// files (a restart in between keeps them, or deletes them with the saved first index), then
  /// deletes the WAL segments. Holding the lock for those took 250–360 ms in the chaos run (the
  /// leader's heartbeats stopped: an election).
  void compact_log();

  /// The history files of |s|'s timeline and of its ancestors (each the lines of the latest one
  /// before its own) into wal_dir, where missing: at startup and after an install, the TimelineOps
  /// before the state are not applied again (compacted, or never received).
  void write_history(RaftState const& s);

  /// The WAL deletion thread: finishes the compactions (DropJob), then deletes the segments of
  /// wal_dir wholly before first_segment (by address: the parent timeline's copy of the segment
  /// where a timeline starts is kept, the WAL sender reads the start of the new timeline's first
  /// segment from it).
  void wal_gc();

  /// A stepdown that has not handed over the leadership in kStepdownTimeouts election timeouts is
  /// called off, so that the leader doesn't refuse the clients for ever.
  void check_stepdown_timeout();

  /// Write the WAL bytes of an applied entry to its segment in wal_dir (pwrite; synced by the
  /// next checkpoint).
  void write_wal(Entry const& e, std::span<uint8_t const> wal);

  /// A segment of wal_dir open for writing (created, zero filled, if missing).
  struct WalFile {
    int fd = -1;
    WalFile() = default;
    WalFile(WalFile const&)            = delete;
    WalFile& operator=(WalFile const&) = delete;
    ~WalFile() {
      if (fd >= 0)
        ::close(fd);
    }
  };
  Result<shared_ptr<WalFile>> wal_file(LogSeqNumber const& lsn);

  /// The checkpoint (docs/ledger.md §9.2): syncs the segments written since the last one (without
  /// the server lock), then saves the state applied before them.
  void checkpoint();
  void checkpoint_loop();

  /// The InstallSnapshot's last chunk: the install file replaces the ledger.
  void install(InstallInfo const& info);

  /// The meta file: the state, and an install in progress. Its state never runs ahead of
  /// wal_dir: the entries' part of it is the last checkpoint's (the node's own fields are
  /// current).
  std::filesystem::path meta_path() const { return dir_ / "meta"; }
  /// What the meta file holds now (with the server lock held).
  LedgerMeta meta_snapshot(std::optional<RaftIndex> installing = std::nullopt) const {
    auto s        = state_;
    s.log_idx     = checkpoint_state_.log_idx;
    s.timestamp   = checkpoint_state_.timestamp;
    s.flushed_lsn = checkpoint_state_.flushed_lsn;
    s.wal_end     = checkpoint_state_.wal_end;
    return LedgerMeta{.state = s, .installing = installing};
  }
  /// Saves the meta file now (with the server lock held).
  void save_meta(std::optional<RaftIndex> installing = std::nullopt) {
    value(write_meta(++meta_seq_, meta_snapshot(installing)));
  }
  /// Writes save number |seq| of the meta file, unless a later one is written already (the
  /// compaction's saves are written by the WAL deletion thread, without the server lock).
  Result<void> write_meta(uint64_t seq, LedgerMeta const& meta);

  /// A stepdown is called off after this many election timeouts.
  constexpr static unsigned kStepdownTimeouts = 32U;

  /// Returns the current term and if it has not been changed.
  std::tuple<RaftTerm, bool> set_current_term(RaftTerm term);

  template <typename T>
  T value(Result<T> r) const {
    if (!r)
      system_error(r.error());
    return std::move(r.value());
  }
  void value(Result<void> r) const {
    if (!r)
      system_error(r.error());
  }

  bool vote(RaftTerm term, RaftNodeId candidate) {
    return value(term_file_.vote(term, candidate));
  }

  /// The leader's append of an entry, with the WAL bytes its command names.
  RaftIndex append(Entry const& e, std::span<uint8_t const> wal);

  RaftIndex compaction_idx() const { return compaction_idx_.load(std::memory_order_relaxed); }

  enum class NodeType : uint8_t {
    kFollower  = 0,
    kLeader    = 1,
    kCandidate = 2,
  };

  std::atomic<NodeType> type_{NodeType::kFollower};

  Config const& config_;

  // Self Node Id.
  RaftNodeId const self_id_;

  // All log statement are prefixed by the node_id.
  string const log_prefix_;

  // Working directory
  std::filesystem::path const dir_;

  // The WAL segments.
  std::filesystem::path const       wal_dir_;
  shared_ptr<WalSegmentCache> const cache_;

  // The segments used last, kept open: segment() is called for every published write and every
  // block sent or received, and opening one costs a stat, allocations and the mapping cache's lock.
  // wal_gc drops the ones it deletes.
  static constexpr size_t                           kOpenSegments = 4;
  mutable std::mutex                                open_segments_mtx_;
  mutable map<LogSeqNumber, shared_ptr<WalSegment>> open_segments_;

  // Persistent state.
  TermFile term_file_;
  Ledger   ledger_;

  // The state machine (under the lock), and the state at the last checkpoint: wal_dir is synced
  // up to it, and the meta file holds it.
  RaftState state_;
  RaftState checkpoint_state_;
  uint64_t  install_gen_ = 0;  // bumped by an install: a checkpoint taken before it is void

  // The segments of wal_dir open for writing, and those written since the last checkpoint.
  std::mutex                                   wal_files_mtx_;
  map<LogSeqNumber, shared_ptr<WalFile>>       wal_files_;
  map<LogSeqNumber, shared_ptr<WalFile>>       dirty_;

  // The checkpoint thread.
  std::mutex              checkpoint_mtx_;
  std::condition_variable checkpoint_cv_;
  bool                    checkpoint_stop_{false};
  std::thread             checkpoint_thread_;

  /// A copy of state_ for applied_state(), under published_mtx_ (a leaf lock).
  mutable std::mutex published_mtx_;
  RaftState          published_;
  void               publish_state();

  std::optional<LogSeqNumber> keep_from_;  // under the server lock

  // The meta file's saves are numbered (under the server lock) and written under meta_mtx_: one
  // written makes the earlier ones void (each holds the state at its number).
  uint64_t   meta_seq_ = 0;  // under the server lock
  std::mutex meta_mtx_;
  uint64_t   meta_written_ = 0;  // under meta_mtx_

  // The compactions the WAL deletion thread finishes (under gc_mtx_): the latest one's meta save,
  // then every one's ledger files.
  struct DropJob {
    uint64_t                      seq   = 0;
    uint64_t                      epoch = 0;  // drop_epoch_ when the files were detached
    LedgerMeta                    meta;
    vector<std::filesystem::path> files;
  };
  // An install replaces every ledger file: it bumps the epoch (under unlink_mtx_, which the WAL
  // deletion thread holds while it unlinks), and the files detached before it are not unlinked
  // (they are gone; a name may be the installed file's).
  std::mutex            unlink_mtx_;
  std::atomic<uint64_t> drop_epoch_{0};

  // The WAL deletion thread.
  std::mutex               gc_mtx_;
  std::condition_variable  gc_cv_;
  bool                     gc_wake_{false};
  bool                     gc_stop_{false};
  std::optional<DropJob>   drop_;  // under gc_mtx_
  std::thread              gc_thread_;
  std::atomic<RaftIndex>   applied_idx_{RaftIndex{0}};
  shared_ptr<RaftObserver> observer_;

  std::atomic<RaftIndex> compaction_idx_{RaftIndex{0}};

  auto timer_lock() { return std::unique_lock<std::mutex>{timer_mtx_}; }

  // For calling periodic function.
  boost::asio::steady_timer timer_;
  std::mutex                timer_mtx_;

  /// Last ping (epoch_msec()), used for starting an election.
  std::atomic<unsigned> last_ping_epoch_{0};

  unsigned last_ping_epoch() const { return last_ping_epoch_.load(); }

  /// Term and time of the last AppendEntries from a leader, used to log untriggered elections.
  std::atomic<RaftTerm> leader_msg_term_{RaftTerm{0}};
  std::atomic<unsigned> leader_msg_epoch_{0};  ///< epoch_msec()

  void update_last_ping(bool delay_election) {
    auto cur_v = last_ping_epoch_.load();
    auto new_v = epoch_msec() + (delay_election ? options_.election_timeout_msecs : 0U);

    while (new_v > cur_v &&
           !last_ping_epoch_.compare_exchange_strong(cur_v, new_v, std::memory_order_relaxed))
      ;
  }

  // The last log entry time.
  // The leader adds a tick entry at least every tick_seconds seconds.
  std::atomic<std::time_t> last_entry_time_{0};

  // Current term.
  std::atomic<RaftTerm> current_term_{RaftTerm{0}};

  // The leadership term
  std::atomic<RaftTerm> leadership_term_{RaftTerm{0}};

  // Volatile state
  std::atomic<RaftIndex> commit_idx_{RaftIndex{0}};
  MatchIndexes           match_indexes_;

  // The handlers of the entries that the leader appended, called when they are committed.
  // The raft index of the entry, the handler and when it was enqueued (epoch_msec()).
  list<std::tuple<RaftIndex, ExecHandler, unsigned>> handlers_;

  shared_ptr<PeerConnectionMgr> connections_;

  std::atomic_flag can_vote_ = ATOMIC_FLAG_INIT;
  VoteTally        votes_;

  // On receiving a shutdown signal, we send an election request to one of the servers.
  // A tick entry is added to the log and on successful replication of the tick entry to one of the
  // node tiggers an election request to it.
  std::atomic_flag stepdown_         = ATOMIC_FLAG_INIT;
  std::atomic_flag election_request_ = ATOMIC_FLAG_INIT;

  // When the last stepdown started (epoch_msec()), 0 if none.
  std::atomic<unsigned> stepdown_epoch_{0};

  std::atomic_flag serving_ = ATOMIC_FLAG_INIT;

  mutable std::mutex      pg_status_mtx_;
  std::optional<uint64_t> primary_flushed_lsn_;
  std::optional<uint64_t> standby_applied_lsn_;

  std::mutex                                          backup_mtx_;
  std::function<void(SnapshotRequest, BackupHandler)> backup_provider_;

  mutable std::mutex mtx_;

  // Before any usage, the callee should acquire ausage lock.
  mutable std::shared_mutex usage_lock_;
  std::atomic_flag          closed_ = ATOMIC_FLAG_INIT;

  mutable std::mt19937                    rd_{std::random_device{}()};
  mutable std::uniform_int_distribution<> random_election_timeout_;

  // The election timeout, drawn once for each last ping (only used by periodic()).
  unsigned election_timeout_epoch_{0};  ///< The last_ping_epoch_ election_timeout_ was drawn for.
  unsigned election_timeout_{0};

  Options const options_;
};
