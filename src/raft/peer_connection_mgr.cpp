#include "lib/servers/zookeeper/peer_connection_mgr.hpp"

#include "lib/net/async_connect.hpp"

namespace banyandb::servers::zookeeper {

PeerConnectionMgr::PeerConnectionMgr(boost::asio::io_context& io_context,
                                     RaftNodeId               self_id,
                                     Config const&            config,
                                     Certificates const&      certs,
                                     std::filesystem::path    tmpdir,
                                     Options const&           options)
    : io_context_(io_context),
      self_id_(self_id),
      log_prefix_(fn::format("({0}) ", INT(self_id_))),
      nodes_(static_cast<unsigned>(config.zk_servers.size())),
      tmpdir_(tmpdir),
      options_(options) {
  for (auto i = 0U; i < nodes(); ++i) {
    ssl_contexts_.emplace_back(fn::format("{0}:{1}", to_string(Role::kZookeeper), INT(self_id)),
                               certs);
    connections_.emplace_back(nullptr);
    last_connect_attempt_.at(i) = 0U;
  }

  // This will prevent from connecting to self.
  last_connect_attempt_.at(INT(self_id)).store(std::numeric_limits<unsigned>::max());
}

shared_ptr<RaftPeerConnection> PeerConnectionMgr::set_connection(
    uint16_t                       idx,
    shared_ptr<RaftPeerConnection> conn) {
  auto lk = std::unique_lock<std::mutex>{mtx_};

  if (closed_.test())
    return nullptr;

  last_connect_attempt_.at(idx).store(epoch_msec());
  if (connections_.at(idx) && !connections_.at(idx)->closed())
    return nullptr;

  connections_.at(idx) = conn;
  return conn;
}

void PeerConnectionMgr::close() {
  auto lk = std::unique_lock<std::mutex>{mtx_};

  if (closed_.test_and_set())
    return;

  auto weak_ptrs = std::accumulate(std::begin(connections_),
                                   std::end(connections_),
                                   vector<weak_ptr<RaftPeerConnection>>{},
                                   [](auto result, auto& conn) {
                                     if (conn) {
                                       conn->shutdown();
                                       result.emplace_back(conn);
                                       conn.reset();
                                     }
                                     return result;
                                   });

  std::this_thread::sleep_for(std::chrono::seconds(1));
  LOG_INFO << log_prefix_ << "Connections hanging around "
           << std::accumulate(
                  std::begin(weak_ptrs), std::end(weak_ptrs), 0U, [](unsigned result, auto& ptr) {
                    return result + ptr.use_count();
                  });
}

vector<shared_ptr<RaftPeerConnection>> PeerConnectionMgr::get_connections() {
  auto lk = std::unique_lock<std::mutex>{mtx_};
  return connections_;
}

unsigned PeerConnectionMgr::connected_peers() const {
  auto lk = std::unique_lock<std::mutex>{mtx_};

  return std::accumulate(
      connections_.begin(), connections_.end(), 0U, [](unsigned result, auto const& conn) {
        return result + ((conn && !conn->closed()) ? 1 : 0);
      });
}

void PeerConnectionMgr::set_connection(
    uint16_t                                            idx,
    weak_ptr<PeerConnectionApi>                         server,
    std::function<void(shared_ptr<RaftPeerConnection>)> on_connect,
    Result<socket_type>                                 socket) {
  if (!socket) {
    LOG_ERROR << log_prefix_ << "Unable to connect to node=" << idx << " error: " << socket.error();
    set_connection(idx, nullptr);

  } else {
    LOG_INFO << log_prefix_ << "Connected to node=" << idx;
    if (auto conn =
            set_connection(idx,
                           Connection<>::create<RaftPeerConnection>(self_id_,
                                                                    /*peer_id=*/RaftNodeId{idx},
                                                                    server,
                                                                    tmpdir_,
                                                                    std::move(socket.value()),
                                                                    options_));
        conn) {
      on_connect(conn);
    }
  }
}

void PeerConnectionMgr::reconnect(weak_ptr<PeerConnectionApi>                         server,
                                  Config const&                                       config,
                                  std::function<void(shared_ptr<RaftPeerConnection>)> on_connect) {
  auto connections = get_connections();

  for (uint16_t idx = 0U; idx < connections.size(); ++idx) {
    if (INT(self_id_) == idx)
      continue;

    auto& conn = connections.at(idx);
    if (conn && !conn->closed())
      continue;

    auto last_connect = last_connect_attempt_.at(idx).load();
    if ((last_connect == 0 || (last_connect + options().reconnect_interval < epoch_msec())) &&
        last_connect_attempt_.at(idx).compare_exchange_strong(
            last_connect, std::numeric_limits<unsigned>::max())) {
      LOG_INFO << log_prefix_ << "Connecting to node=" << idx << " last try @ " << last_connect;

      banyandb::net::async_connect(
          io_context(),
          ssl_contexts_.at(idx),
          HostAddress{.address = config.zk_servers.at(idx).hostname,
                      .port    = config.zk_servers.at(idx).port},
          [this, s = self(), idx, server, on_connect](Result<socket_type> socket) {
            set_connection(idx, server, std::move(on_connect), std::move(socket));
          });
    }
  }
}

unsigned PeerConnectionMgr::post(std::function<void(shared_ptr<RaftPeerConnection>)> func) {
  unsigned closed = 0;

  auto connections = get_connections();
  for (uint16_t idx = 0U; idx < connections.size(); ++idx) {
    auto& conn = connections.at(idx);
    if (conn && !conn->closed())
      ::boost::asio::post(io_context(), [conn, func]() { func(conn); });
    else
      ++closed;
  }
  return nodes() - closed;
}
unsigned PeerConnectionMgr::for_each(std::function<void(shared_ptr<RaftPeerConnection>&)> func) {
  unsigned closed = 0;

  auto connections = get_connections();
  for (uint16_t idx = 0U; idx < connections.size(); ++idx) {
    auto& conn = connections.at(idx);
    if (conn && !conn->closed())
      func(conn);
    else
      ++closed;
  }
  return nodes() - closed;
}

}  // namespace banyandb::servers::zookeeper
