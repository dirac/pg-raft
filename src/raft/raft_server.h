#pragma once

#include "src/base/config.h"
#include "src/base/wal_segment.h"
#include "src/net/server.h"
#include "src/raft/match_indexes.h"
#include "src/raft/options.h"
#include "src/raft/peer_connection_mgr.h"
#include "src/raft/raft_client_connection.h"
#include "src/raft/raft_follower_connection.h"
#include "src/raft/raft_log.h"
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
//  - The WAL bytes travel with the entries and are written to the segments in wal_dir as they are
//    accepted.
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
  /// |dir| holds the raft log, |wal_dir| the WAL segments (raft_dir/pg_wal).
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

  /// Append the command to the log (leader only); the handler is called when it is committed, with
  /// the server lock held, or with ErrorCode::kNotLeader.
  void exec(WalCommand cmd, ExecHandler handler);

  /// The WAL segment in wal_dir, created (zero filled) if it doesn't exist.
  Result<shared_ptr<WalSegment>> segment(LogSeqNumber const& lsn) const override;

  /// The latest timeline history file in wal_dir (written when a TimelineOp is applied, or by an
  /// InstallSnapshot), or nullopt before the first promotion.
  std::optional<TimelineOp> latest_timeline() const override;

  /// A copy of the state, published under its own mutex after every change, so that a peer
  /// connection can read it without the server lock.
  RaftState applied_state() const override;

  RaftResponse handle_peer_request(RaftNodeId node_id, RaftTerm term, std::monostate) override;

  RaftResponse handle_peer_request(RaftNodeId  candidate,
                                   RaftTerm    term,
                                   RequestVote request) override;

  RaftResponse handle_peer_request(RaftNodeId                         leader,
                                   RaftTerm                           term,
                                   InstallSnapshot                    request,
                                   inplace_vector<MemBlock, 2> const& blocks,
                                   Bytes const&                       bytes) override;

  RaftResponse handle_peer_request(RaftNodeId                         leader,
                                   RaftTerm                           term,
                                   AppendEntries                      request,
                                   inplace_vector<MemBlock, 2> const& blocks,
                                   Bytes const&                       bytes) override;

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

  RaftIndex commit_idx() const { return commit_idx_.load(std::memory_order_relaxed); }

  RaftLog const& raft_log() const { return log_; }

  unsigned random_election_timeout() const {
    return static_cast<unsigned>(random_election_timeout_(rd_));
  }

  /// Some LMDB error or file system error. The only sane thing to do is exit.
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

  /// For the tests: fn(state, log, commit_idx) with the lock held, so that nothing is appended or
  /// applied meanwhile.
  template <typename Fn>
  auto inspect_for_test(Fn&& fn) const {
    auto lk = unique_lock();
    return std::forward<Fn>(fn)(state_, log_, commit_idx());
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

  unsigned send_append_entries(bool check_sync_flag);

  unsigned send_request_votes(RaftTerm term);

  RaftTerm update_current_term(RaftTerm term);

  void make_follower(std::source_location loc = std::source_location::current());
  void log_untriggered_election(unsigned now) const;
  void make_candidate(unsigned inc = 1);

  void make_leader(UniqueLock lk);
  void make_follower(UniqueLock lk, std::source_location loc = std::source_location::current());
  void make_candidate(unsigned inc, UniqueLock lk);

  void set_commit_idx(RaftIndex idx) {
    auto curr = commit_idx();
    while (curr < idx && !commit_idx_.compare_exchange_strong(curr, idx, std::memory_order_relaxed))
      ;
  }

  void update_match_index(RaftNodeId node_id, RaftIndex idx);

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

  /// A stepdown that has not handed over the leadership in kStepdownTimeouts election timeouts is
  /// called off, so that the leader doesn't refuse the clients for ever.
  void check_stepdown_timeout();

  /// Write the WAL bytes of the blocks to the segments.
  void write_blocks(inplace_vector<MemBlock, 2> const& blocks, Bytes const& bytes);

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

  // Wrapper function on raft log.

  bool vote(RaftTerm term, RaftNodeId candidate) { return value(log_.vote(term, candidate)); }

  RaftState load_snapshot() const { return value(log_.load_snapshot()); }

  bool append_entries(RaftIndex            prev_log_idx,
                      RaftTerm             prev_log_term,
                      vector<Entry> const& entries,
                      bool                 delete_entries) {
    return value(log_.append_entries(prev_log_idx, prev_log_term, entries, delete_entries));
  }

  Entry entry(RaftIndex idx) const {
    // The entry is not in the log (it is not there, or it is before the first), or it is empty.
    auto e = value(log_.entries_from(idx, 1)).value_or(vector<Entry>{});
    if (e.empty())
      system_error(system_error_code(EINVAL));

    return std::move(e.at(0));
  }

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

  // Persistent state.
  RaftLog log_;

  // The state machine.
  RaftState state_;

  /// A copy of state_ for applied_state(), under published_mtx_ (a leaf lock).
  mutable std::mutex       published_mtx_;
  RaftState                published_;
  void                     publish_state();
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
