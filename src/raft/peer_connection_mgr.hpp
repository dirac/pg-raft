#ifndef LIB_SERVERS_ZOOKEEPER_PEER_CONNECTION_MGR_HPP_
#define LIB_SERVERS_ZOOKEEPER_PEER_CONNECTION_MGR_HPP_

#include "lib/base/config.hpp"
#include "lib/net/ssl_context.hpp"
#include "lib/servers/zookeeper/raft_peer_connection.hpp"

namespace banyandb::servers::zookeeper {

using namespace banyandb::base;
using namespace banyandb::net;

struct PeerConnectionMgr : public std::enable_shared_from_this<PeerConnectionMgr> {
 public:
  PeerConnectionMgr(boost::asio::io_context& io_context,
                    RaftNodeId               self_id,
                    Config const&            config,
                    Certificates const&      certs,
                    std::filesystem::path    tmpdir,
                    Options const&           options);
  void close();

  unsigned post(std::function<void(shared_ptr<RaftPeerConnection>)> func);
  unsigned for_each(std::function<void(shared_ptr<RaftPeerConnection>&)> func);

  void reconnect(weak_ptr<PeerConnectionApi>                         server,
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
    std::for_each(std::begin(last_connect_attempt_), std::end(last_connect_attempt_), [](auto& t) {
      t.store(0ULL, std::memory_order_relaxed);
    });
  }

 private:
  /// Get a copy of connections_.
  vector<shared_ptr<RaftPeerConnection>> get_connections();

  /// Update connections_.
  shared_ptr<RaftPeerConnection> set_connection(uint16_t idx, shared_ptr<RaftPeerConnection> conn);

  void set_connection(uint16_t                                            idx,
                      weak_ptr<PeerConnectionApi>                         server,
                      std::function<void(shared_ptr<RaftPeerConnection>)> on_connect,
                      Result<socket_type>                                 socket);

  boost::asio::io_context& io_context() { return io_context_; }

  boost::asio::io_context& io_context_;

  RaftNodeId const self_id_;

  string const log_prefix_;

  /// Number of nodes.
  unsigned const nodes_;

  // Temp directory to be used for the connections.
  std::filesystem::path const tmpdir_;

  // SSL Context for each connection.
  vector<banyandb::net::ssl::Context> ssl_contexts_;

  // The connections.
  vector<shared_ptr<RaftPeerConnection>> connections_;

  // The last reconnect attempt, so that we don;t do it often.
  std::array<std::atomic<uint64_t>, 15U> last_connect_attempt_;

  mutable std::mutex mtx_;

  Options const options_;

  std::atomic_flag closed_ = ATOMIC_FLAG_INIT;
};
}  // namespace banyandb::servers::zookeeper

#endif
