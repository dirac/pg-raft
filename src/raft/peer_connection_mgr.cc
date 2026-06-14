#include "src/raft/peer_connection_mgr.h"

#include "src/net/async_connect.h"

PeerConnectionMgr::PeerConnectionMgr(boost::asio::io_context& io_context,
                                     RaftNodeId               self_id,
                                     Config const&            config,
                                     Options const&           options)
    : io_context_(io_context),
      self_id_(self_id),
      log_prefix_(std::format("({0}) ", INT(self_id_))),
      nodes_(static_cast<unsigned>(config.servers.size())),
      options_(options) {
  for (auto i = 0U; i < nodes(); ++i) {
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
  log(INFO,
      "{} Connections hanging around {}",
      log_prefix(),
      std::accumulate(
          std::begin(weak_ptrs), std::end(weak_ptrs), 0U, [](unsigned result, auto& ptr) {
            return result + ptr.use_count();
          }));
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
    shared_ptr<PeerConnectionApi>                       server,
    std::function<void(shared_ptr<RaftPeerConnection>)> on_connect,
    Result<socket_type>                                 socket) {
  if (!socket) {
    log(INFO, "{} Unable to connect to node={} error: {}", log_prefix(), idx, socket.error());
    set_connection(idx, nullptr);

  } else {
    log(INFO, "{} Connected to node={}", log_prefix(), idx);
    if (auto conn =
            set_connection(idx,
                           Connection::create<RaftPeerConnection>(self_id_,
                                                                  /*peer_id=*/RaftNodeId{idx},
                                                                  server,
                                                                  std::move(socket.value()),
                                                                  options_));
        conn) {
      on_connect(conn);
    }
  }
}

void PeerConnectionMgr::reconnect(shared_ptr<PeerConnectionApi>                       server,
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
      log(INFO, "{} Connecting to node={} last try @ {}", log_prefix(), idx, last_connect);

      async_connect(io_context(),
                    std::format("Raft:{}", self_id_),
                    HostAddress{.address = config.servers.at(idx).hostname,
                                .port    = config.servers.at(idx).ha_port},
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
