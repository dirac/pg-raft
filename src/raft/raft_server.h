#pragma once

#include "src/net/server.h"
#include "src/net/ssl_context.h"
#include "src/raft/match_indexes.h"
#include "src/raft/options.h"
#include "src/raft/peer_connection_mgr.h"
#include "src/raft/raft_client_connection.h"
#include "src/raft/raft_follower_connection.h"
#include "src/raft/raft_log.h"
#include "src/raft/raft_peer_connection.h"
#include "src/raft/raft_types.h"
#include "src/raft/snapshot.h"
#include "src/raft/vote_tally.h"
#include "src/raft/zk_state.h"

class RaftPeerConnection;
class RaftServer : public ClientConnectionApi,
                   public FollowerConnectionApi,
                   public PeerConnectionApi,
                   public Server {
 public:
  RaftServer(RaftNodeId                     id,
             boost::asio::ip::tcp::acceptor acceptor,
             Certificates const&            certs,
             Config const&                  config,
             bool                           enable_ssl,
             Options                        options = Options{});

  virtual ~RaftServer();

  /// Start the server.
  void start() override;
  void close();

  void on_shutdown() override;

  std::shared_lock<std::shared_mutex> usage_lock() const override {
    auto lk = std::shared_lock<std::shared_mutex>{usage_lock_};
    if (!closed_.test())
      return lk;

    LOG_INFO << log_prefix_ << "returning null lock";
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

  /// Connection from client and other peers.
  void new_connection(string peer, socket_type socket) override;

  Config const& config() const { return config_; }

  /// Number of nodes
  size_t nodes() const { return config().zk_servers.size(); }

  /// Does the server think it is the leader?
  bool is_leader() const override { return type_.load() == NodeType::kLeader; }
  bool resigning() const override { return stepping_down(false); }

  /// Client request.
  void exec(ZkRequest request, std::function<void(ZkResponse)> handler) override;

  std::optional<RaftResponse> handle_peer_request(RaftNodeId node_id,
                                                  RaftTerm   term,
                                                  std::monostate) override;

  std::optional<RaftResponse> handle_peer_request(RaftNodeId  candidate,
                                                  RaftTerm    term,
                                                  RequestVote request) override;

  std::optional<RaftResponse> handle_peer_request(RaftNodeId      leader,
                                                  RaftTerm        term,
                                                  InstallSnapshot request) override;

  std::optional<RaftResponse> handle_peer_request(RaftNodeId    leader,
                                                  RaftTerm      term,
                                                  AppendEntries request) override;

  std::optional<RaftResponse> handle_peer_request(RaftNodeId      candidate,
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

  RaftLog const& log() const { return log_; }

  unsigned random_election_timeout() const {
    auto v = random_election_timeout_(rd_);
    return static_cast<unsigned>(v);
  }

  /// Some LMDB error or file system error. The only sane thing to do is exit.
  [[noreturn]] void system_error(std::error_code ec) const override;

  RaftTerm current_term() const { return current_term_.load(std::memory_order_relaxed); }
  RaftTerm leadership_term() const { return leadership_term_.load(std::memory_order_relaxed); }

  bool has_votes() const {
    auto [votes, _] = votes_.votes_tally();
    return votes > (nodes() >> 1);
  }
  bool has_lost_election() const {
    auto [votes, responses] = votes_.votes_tally();
    return (responses - votes) > (nodes() >> 1);
  }

  RaftIndex apply_entries();

 private:
  using UniqueLock = std::unique_lock<std::mutex>;

  // Acquire a lock.
  UniqueLock unique_lock() const { return std::unique_lock<std::mutex>{mtx_}; }

  UniqueLock try_unique_lock() const {
    return std::unique_lock<std::mutex>{mtx_, std::try_to_lock_t{}};
  }

  void on_connect(shared_ptr<RaftPeerConnection> conn);

  // We can vote only after receiving the term from majority of the nodes.
  bool can_vote() const { return can_vote_.test(); }

  bool is_candidate() const { return type_.load() == NodeType::kCandidate; }
  bool is_follower() const { return type_.load() == NodeType::kFollower; }

  // Every second we add a tick entry if we have not apppend of real entry.
  // Returns true if an tick entry was appended.
  bool tick(bool force = false);

  unsigned send_append_entries(bool check_sync_flag);

  unsigned send_request_votes(RaftTerm term);

  RaftTerm update_current_term(RaftTerm term);

  void make_leader();
  void make_follower(std::source_location loc = std::source_location::current());
  void make_candidate(unsigned inc = 1);

  void make_leader(UniqueLock lk);
  void make_follower(UniqueLock lk, std::source_location loc = std::source_location::current());
  void make_candidate(unsigned inc, UniqueLock lk);

  void set_commit_idx(RaftIndex idx) {
    auto curr = commit_idx();
    while (curr < idx &&
           commit_idx_.compare_exchange_strong(curr, idx, std::memory_order_relaxed) == false)
      ;
  }

  void update_match_index(RaftNodeId node_id, RaftIndex idx);

  /// Number of entries.
  unsigned count(RaftIndex idx, RaftTerm term) const;

  /// Function to support stepping down a leader.
  // 1. Add a tick entry and send an append entries to all.
  // void stepdown();

  // 2. When the server processes AppendEntries response, it checks if the remote node has received
  //    the last tick entry.
  bool stepping_down(bool acquire_lock) const;

  // 3. If so then the first response results in sending an election request to the node.
  bool should_send_election_request();

  /// Returns the current term and if it has not been changed.
  std::tuple<RaftTerm, bool> set_current_term(RaftTerm term);

  template <typename T>
  T value(Result<T> r) const {
    if (!r)
      system_error(r.error());
    return std::move(r.value());
  }
  template <>
  void value<void>(Result<void> r) const {
    if (!r)
      system_error(r.error());
  }

  // Wrapper function on raft log.

  bool vote(RaftTerm term, RaftNodeId candidate) { return value(log_.vote(term, candidate)); }

  void apply_db_snapshot(std::filesystem::path file) {
    return value(log_.apply_db_snapshot(std::move(file)));
  }
  ZkState load_snapshot() const { return value(log_.load_snapshot()); }

  bool append_entries(RaftIndex            prev_log_idx,
                      RaftTerm             prev_log_term,
                      vector<Entry> const& entries,
                      bool                 delete_entries) {
    return value(log_.append_entries(prev_log_idx, prev_log_term, entries, delete_entries));
  }

  Entry entry(RaftIndex idx) const {
    std::optional<vector<Entry>> e = value(log_.entries_from(idx, 1));
    if (!e || e.value().empty())
      system_error(system_error_code(EINVAL));

    return std::move(e.value().at(0));
  }

  RaftIndex compaction_idx() const { return compaction_idx_.load(std::memory_order_relaxed); }

 private:
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

  // Tmp dir for the followers
  std::filesystem::path const tmpdir_;

  // Persistent state.
  RaftLog log_;

  std::atomic<RaftIndex> compaction_idx_{RaftIndex{0}};
  std::mutex             compaction_mtx_;

  auto timer_lock() { return std::unique_lock<std::mutex>{timer_mtx_}; }

  // For calling periodic function.
  boost::asio::steady_timer timer_;
  std::mutex                timer_mtx_;

  /// Last ping, used for starting an election.
  std::atomic<unsigned> last_ping_epoch_{0};

  unsigned last_ping_epoch() const { return last_ping_epoch_.load(); }

  void update_last_ping(bool delay_election) {
    auto cur_v = last_ping_epoch();
    auto new_v = epoch_msec() + (delay_election ? options_.election_timeout : 0);

    while (new_v > cur_v &&
           !last_ping_epoch_.compare_exchange_strong(cur_v, new_v, std::memory_order_relaxed))
      ;
  }

  // The last log entry time.
  // The leader adds a tick entry at least every kTickSeconds seconds.
  std::atomic<std::time_t> last_entry_time_{0};

  // Current term.
  std::atomic<RaftTerm> current_term_{RaftTerm{0}};

  // The leadership term
  std::atomic<RaftTerm> leadership_term_{RaftTerm{0}};

  // Volatile state
  std::atomic<RaftIndex> commit_idx_{RaftIndex{0}};
  MatchIndexes           match_indexes_;

  // The callback when the request is eventually processed after reaching quorum.
  // The raft index of the request, callback for the response and when it was enqueued.
  list<std::tuple<RaftIndex, std::function<void(ZkResponse)>, unsigned>> handlers_;

  // The number of requests in the handlers_.
  std::atomic_int64_t queue_size_;

  shared_ptr<PeerConnectionMgr> connections_;

  std::atomic_flag can_vote_ = ATOMIC_FLAG_INIT;
  VoteTally        votes_;

  // On receiving a shutdown signal, we send an election request to one of the servers.
  // A tick entry is added to the log and on successful replication of the tick entry to one of the
  // node tiggers an election request to it.
  std::atomic_flag stepdown_ = ATOMIC_FLAG_INIT;
  std::atomic_flag election_request_;

  mutable std::mutex mtx_;

  // Before any usage, the callee should acquire ausage lock.
  mutable std::shared_mutex usage_lock_;
  std::atomic_flag          closed_ = ATOMIC_FLAG_INIT;

  mutable std::mt19937                    rd_{std::random_device{}()};
  mutable std::uniform_int_distribution<> random_election_timeout_;

  Options const options_;
};
