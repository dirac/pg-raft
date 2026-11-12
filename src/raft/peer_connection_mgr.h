#pragma once

#include "src/base/config.h"
#include "src/raft/raft_peer_connection.h"

/// The identity a raft server sends in the connection handshake: "Raft:<node id>".
inline string raft_identity(RaftNodeId id) {
  return std::format("Raft:{0}", INT(id));
}

struct PeerConnectionMgr : public std::enable_shared_from_this<PeerConnectionMgr> {
 public:
  PeerConnectionMgr(boost::asio::io_context& io_context,
                    RaftNodeId               self_id,
                    Config const&            config,
                    Options const&           options);
  void close();

  unsigned post(std::function<void(shared_ptr<RaftPeerConnection>)> func);

  void reconnect(weak_ptr<PeerConnectionApi> const&                  server,
                 Config const&                                       config,
                 std::function<void(shared_ptr<RaftPeerConnection>)> on_connect);

  /// Number of peers connections.
  unsigned connected_peers() const;

  /// Number of nodes.
  unsigned nodes() const { return nodes_; }

  /// Has unconnected peers.
  bool has_unconnected_peers() const { return (nodes() - 1U) != connected_peers(); }

  shared_ptr<PeerConnectionMgr> self() { return shared_from_this(); }

  Options const& options() const { return options_; }

  void reset_last_connect_attempts() {
    std::ranges::for_each(last_connect_attempt_,
                          [](auto& t) { t.store(0, std::memory_order_relaxed); });
    // Never connect to self.
    last_connect_attempt_.at(INT(self_id_)).store(kConnecting);
  }

 private:
  /// Get a copy of connections_.
  vector<shared_ptr<RaftPeerConnection>> get_connections();

  /// Update connections_.
  shared_ptr<RaftPeerConnection> set_connection(uint16_t idx, shared_ptr<RaftPeerConnection> conn);

  void set_connection(uint16_t                                                   idx,
                      weak_ptr<PeerConnectionApi> const&                         server,
                      std::function<void(shared_ptr<RaftPeerConnection>)> const& on_connect,
                      Result<socket_type>                                        socket);

  boost::asio::io_context& io_context() { return io_context_; }

  boost::asio::io_context& io_context_;

  RaftNodeId const self_id_;

  string const log_prefix_;

  /// Number of nodes.
  unsigned const nodes_;

  // The connections.
  vector<shared_ptr<RaftPeerConnection>> connections_;

  /// A connect attempt in progress, or the node itself (never connected to).
  constexpr static unsigned kConnecting = std::numeric_limits<unsigned>::max();

  // The last reconnect attempt (epoch_msec()), so that we don't do it often; 0 for none.
  std::array<std::atomic<unsigned>, 15U> last_connect_attempt_;

  mutable std::mutex mtx_;

  /// The options of the raft servers.
  Options const options_;

  std::atomic_flag closed_ = ATOMIC_FLAG_INIT;
};
