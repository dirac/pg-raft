#pragma once

#include "src/base/wal_segment.h"
#include "src/net/connection.h"
#include "src/raft/options.h"
#include "src/raft/raft_log.h"
#include "src/raft/raft_types.h"

class RaftPeerConnection;

struct PeerConnectionApi {
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

  virtual Result<shared_ptr<WalSegment>> segment(LogSeqNumber const& lsn) const = 0;

  [[noreturn]] virtual void system_error(std::error_code ec) const = 0;
};

/// A raft server creates a peer connection. The other end is an instance of RaftFollowerConnection.
class RaftPeerConnection : public Connection {
 public:
  RaftPeerConnection(RaftNodeId                    self_id,
                     RaftNodeId                    peer_id,
                     shared_ptr<PeerConnectionApi> server,
                     socket_type                   socket,
                     Options const&                options)
      : Connection(std::format("Raft:{0}", self_id),
                   std::move(socket),
                   std::chrono::seconds(options.timeout_secs)),
        self_id_(self_id),
        peer_id_(peer_id),
        server_(server),
        last_send_(0),
        options_(options) {
    set_log_prefix(std::format("({0} -> {1}) ", self_id, peer_id));
  }

  void close() {
    log(INFO, "{} Closing connecion", log_prefix());
    shutdown();
  }

  std::atomic_flag in_sync_;

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
  void send_append_entries(RaftLog const&         rlog,
                           RaftTerm               leadership_term,
                           RaftIndex              commit_idx,
                           shared_ptr<WalSegment> segment) {
    auto lk = std::unique_lock{mtx_};

    if (segment && segment != segment_)
      segment_ = segment;

    return send_append_entries(std::move(lk), rlog, leadership_term, commit_idx);
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
    auto static idx = index<T>();
    auto seq        = sequence_.at(idx).load(std::memory_order_relaxed);
    return (seq & kSeqMask) - (seq >> kSeqBits);
  }

  void set_info_and_node_id(RaftRequest& request, bool respond) {
    request.node_id           = self_id_;
    request.info.epoch        = epoch_msec();
    request.info.request_term = request.term;
    request.info.respond      = respond;
    request.info.request_idx  = static_cast<uint16_t>(request.request.index());

    if (respond) {
      // This function is invoked with a lock (mtx_).
      request.info.request_seq =
          (sequence_.at(request.info.request_idx).fetch_add(1U, std::memory_order_relaxed) + 1U) &
          kSeqMask;

      // By design, reset if we overflow.
      if (kSeqMask == request.info.request_seq) {
        sequence_.at(request.info.request_idx).store(0);

        request.info.request_seq =
            (sequence_.at(request.info.request_idx).fetch_add(1U, std::memory_order_relaxed) + 1U) &
            kSeqMask;
      }
    } else {
      request.info.request_seq = 0;
    }

    if (auto const* r = std::get_if<AppendEntries>(&(request.request))) {
      request.info.log =
          options_.log_req_response && (r->prev_log_idx != prev_log_idx || !r->entries.empty());
      prev_log_idx = r->prev_log_idx;
    } else {
      request.info.log = true;
      prev_log_idx     = RaftIndex{0};
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
    server_->system_error(ec);
  }

  // Read the message size.
  void read_response();

  // Read the message (RaftResponse);
  void response_handler(Result<Data> response);

  /// Send the request and followed by the file, released the lock on completion.
  void send_request(std::unique_lock<std::mutex> lk, RaftRequest request, bool respond);

  void set_leader_term(RaftLog const&                      rlog,
                       RaftTerm                            leadership_term,
                       std::unique_lock<std::mutex> const& lk);

  // Remove entries if it references more that two segments.
  static bool trim(vector<Entry>& entries);

  // The region spaned by the write op entries.
  static inplace_vector<MemBlock, 2> mem_blocks(vector<Entry> const& entries);

 private:
  RaftNodeId const self_id_;
  RaftNodeId const peer_id_;

  RaftTerm leader_term_{0};

  // The prev_log_idx for append entries request. We log Append entry request only when this changes
  // from the previous request.
  RaftIndex prev_log_idx{0};

  shared_ptr<PeerConnectionApi> server_;

  /// Serialize the send messages.
  mutable std::mutex mtx_;

  RaftIndex next_idx_{0};
  unsigned  last_send_;  // The last time append entries or install snapshot was sent.

  // The diff between LSB and MSB is the number of outstanding calls of each kind.
  std::array<std::atomic_uint64_t, 5> sequence_;

  Options const options_;

  shared_ptr<WalSegment> segment_{nullptr};
};
