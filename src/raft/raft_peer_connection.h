#pragma once

#include "src/base/wal_segment.h"
#include "src/net/connection.h"
#include "src/raft/options.h"
#include "src/raft/raft_log.h"
#include "src/raft/raft_types.h"

// The requests carry the WAL bytes of their entries (MemBlocks, copied from the segment mmap and
// sent as an attached buffer); an InstallSnapshot sends the latest entries and their segments.

class RaftPeerConnection;

struct PeerConnectionApi {
  virtual ~PeerConnectionApi() = default;

  virtual void handle_response(shared_ptr<RaftPeerConnection> conn,
                               RaftTerm                       request_term,
                               RaftTerm                       current_term,
                               std::monostate) = 0;

  virtual void handle_response(shared_ptr<RaftPeerConnection> conn,
                               RaftTerm                       request_term,
                               RaftTerm                       current_term,
                               InstallSnapshotResponse        response) = 0;

  virtual void handle_response(shared_ptr<RaftPeerConnection> conn,
                               RaftTerm                       request_term,
                               RaftTerm                       current_term,
                               AppendEntriesResponse          response) = 0;

  virtual void handle_response(shared_ptr<RaftPeerConnection> conn,
                               RaftTerm                       request_term,
                               RaftTerm                       current_term,
                               RequestVoteResponse            response) = 0;

  virtual void handle_response(shared_ptr<RaftPeerConnection> conn,
                               RaftTerm                       request_term,
                               RaftTerm                       current_term,
                               RequestElectionResponse        response) = 0;

  virtual std::shared_lock<std::shared_mutex> usage_lock() const = 0;

  /// The WAL segment, to send its bytes.
  virtual Result<shared_ptr<WalSegment>> segment(LogSeqNumber const& lsn) const = 0;

  /// The latest timeline history file in wal_dir, to send with an InstallSnapshot.
  virtual std::optional<TimelineOp> latest_timeline() const = 0;

  /// A copy of the applied state. It must not take the server lock: a caller may hold the
  /// connection's lock.
  virtual RaftState applied_state() const = 0;

  /// A copy of the state at the entry before the first one (RaftLog::load_boundary), sent with an
  /// InstallSnapshot of the oldest entries. Without the server lock, as applied_state().
  virtual RaftState boundary_state() const = 0;

  [[noreturn]] virtual void system_error(std::error_code ec) const = 0;
};

/// A raft server creates a peer connection. The other end is an instance of RaftFollowerConnection.
class RaftPeerConnection : public Connection {
 public:
  RaftPeerConnection(RaftNodeId                  self_id,
                     RaftNodeId                  peer_id,
                     weak_ptr<PeerConnectionApi> server,
                     socket_type                 socket,
                     std::chrono::seconds        conn_timeout,
                     Options const&              options)
      : Connection(std::format("{0}", INT(peer_id)), std::move(socket), conn_timeout),
        self_id_(self_id),
        peer_id_(peer_id),
        server_(std::move(server)),
        options_(options) {
    set_log_prefix(std::format("({0} -> {1}) ", INT(self_id), INT(peer_id)));
  }

  void close() {
    log(INFO, "{}Closing connection", log_prefix());
    shutdown();
  }

  bool in_sync() const { return in_sync_.test(); }
  void set_in_sync() { in_sync_.test_and_set(); }
  void set_not_in_sync() { in_sync_.clear(); }

  ///
  /// Whenever a node starts, it sends a ping to the peers to get the current_term as well as to
  /// inform them about the node, so that they can connect. The node will only vote after it
  /// receives a reply from the majority of nodes (Exclusing self) and it will not vote for one more
  /// than the  largest term number received.
  ///
  void send_ping(RaftLog const& rlog);

  /// If the node is a leader.
  void send_append_entries(RaftLog const& rlog, RaftTerm leadership_term, RaftIndex commit_idx) {
    return send_append_entries(
        std::unique_lock<std::mutex>{mtx_}, rlog, leadership_term, commit_idx);
  }

  void send_append_entries(std::unique_lock<std::mutex> lk,
                           RaftLog const&               rlog,
                           RaftTerm                     leadership_term,
                           RaftIndex                    commit_idx);

  void send_install_snapshot(std::unique_lock<std::mutex> lk,
                             RaftLog const&               rlog,
                             RaftTerm                     leadership_term);

  /// On failure of AppendEntries, should we send any append entries. If we have an outstanding
  /// append entry its response will also fail and trigger an append entries request.
  ///
  std::unique_lock<std::mutex> should_send_append_entries() const {
    if (append_entries() <= 0 && install_snapshot() <= 0)
      return std::unique_lock<std::mutex>{mtx_};
    return std::unique_lock<std::mutex>{};
  }

  /// If the node is a candidate.
  void send_request_vote(RaftLog const& rlog, RaftTerm candidate_term);

  /// Request election from the node
  void send_request_election(RaftLog const& rlog, RaftTerm leadership_term);

  /// On leader election or on failure of append entries the server resets the nextIndex.
  void set_next_idx(RaftIndex idx, std::unique_lock<std::mutex> const&) {
    // The thread has the lock.
    next_idx_ = idx;
  }

  int64_t append_entries() const { return outstanding_requests<AppendEntries>(); }
  int64_t install_snapshot() const { return outstanding_requests<InstallSnapshot>(); }

  RaftNodeId self_id() const { return self_id_; }
  RaftNodeId peer_id() const { return peer_id_; }

  /// Drop the entries after the first ones that write to more than two segments: a request carries
  /// the bytes of at most two segments.
  static void trim(vector<Entry>& entries);

  /// The regions of the segments written by the entries (the whole segments with
  /// full_wal_segment).
  static inplace_vector<MemBlock, 2> mem_blocks(vector<Entry> const& entries,
                                                bool                 full_wal_segment);

 private:
  static constexpr unsigned kSeqBits = 32;
  static constexpr uint64_t kSeqMask = (static_cast<uint64_t>(0x1) << kSeqBits) - 1ULL;

  static_assert(kSeqBits != 32 || kSeqMask == std::numeric_limits<uint32_t>::max());

  template <typename T>
  static uint16_t index() {
    auto r = RaftRequest{.request = T{}};
    return static_cast<uint16_t>(r.request.index());
  }

  template <typename T>
  int64_t outstanding_requests() const {
    auto static const kIdx = index<T>();
    auto seq               = sequence_.at(kIdx).load(std::memory_order_relaxed);
    return static_cast<int64_t>(seq & kSeqMask) - static_cast<int64_t>(seq >> kSeqBits);
  }

  void set_info_and_node_id(RaftRequest& request) {
    request.node_id           = self_id_;
    request.info.epoch        = epoch_msec();
    request.info.request_term = request.term;
    request.info.request_idx  = static_cast<uint16_t>(request.request.index());

    // This function is invoked with a lock (mtx_).
    request.info.request_seq =
        u4((sequence_.at(request.info.request_idx).fetch_add(1U, std::memory_order_relaxed) + 1U) &
           kSeqMask);

    // Fix if we overflow.
    if (kSeqMask == request.info.request_seq) {
      sequence_.at(request.info.request_idx).store(0);

      request.info.request_seq = u4(
          (sequence_.at(request.info.request_idx).fetch_add(1U, std::memory_order_relaxed) + 1U) &
          kSeqMask);
    }
    if (auto const* r = std::get_if<AppendEntries>(&request.request)) {
      request.info.log =
          options_.log_req_response && (r->prev_log_idx != prev_log_idx_ || !r->entries.empty());
      prev_log_idx_ = r->prev_log_idx;
    } else {
      request.info.log = true;
      prev_log_idx_    = RaftIndex{0};
    }
  }

  void update_response_seq(RaftResponse const& response) {
    uint64_t seq = sequence_.at(response.info.request_idx).load(std::memory_order_relaxed);

    uint64_t n_seq = 0;
    do {
      if ((seq & kSeqMask) < response.info.request_seq)
        return;

      n_seq = std::max<uint64_t>(response.info.request_seq, seq >> kSeqBits) << kSeqBits |
              (seq & kSeqMask);
    } while (!sequence_.at(response.info.request_idx)
                  .compare_exchange_strong(/*expected=*/seq,
                                           /*desired=*/n_seq));
  }

  void system_error(std::error_code      ec,
                    std::source_location loc = std::source_location::current()) {
    shutdown(ec, loc);
    if (auto s = server_.lock(); s)
      s->system_error(ec);
  }

  // Read the message size.
  void read_response();

  // Read the message (RaftResponse);
  void response_handler(Result<Data> msg);

  /// Send the request followed by the bytes of its MemBlocks; releases the lock.
  void send_request(std::unique_lock<std::mutex> lk, RaftRequest request);

  void set_leader_term(RaftLog const&                      rlog,
                       RaftTerm                            leadership_term,
                       std::unique_lock<std::mutex> const& lk);

  RaftNodeId const self_id_;
  RaftNodeId const peer_id_;

  RaftTerm leader_term_{0};

  // The prev_log_idx for append entries request. We log Append entry request only when this changes
  // from the previous request.
  RaftIndex prev_log_idx_{0};

  weak_ptr<PeerConnectionApi> server_;

  /// Serialize the send messages.
  mutable std::mutex mtx_;

  std::atomic_flag in_sync_;

  RaftIndex next_idx_{0};
  unsigned  last_send_{0};  // When (epoch_msec()) append entries or install snapshot was sent.

  // The diff between LSB and MSB is the number of outstanding calls of each kind.
  std::array<std::atomic_uint64_t, 5> sequence_{};

  Options const options_;
};
