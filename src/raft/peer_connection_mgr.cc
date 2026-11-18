#include "src/raft/peer_connection_mgr.h"

#include "src/base/logger.h"
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
  assert(nodes_ <= last_connect_attempt_.size());

  for (auto i = 0U; i < nodes(); ++i) {
    connections_.emplace_back(nullptr);
    last_connect_attempt_.at(i) = 0;
  }

  // This will prevent from connecting to self.
  last_connect_attempt_.at(INT(self_id)).store(kConnecting);
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
  lk.unlock();

  // Give the connections a moment to go away before reporting them; never block an IO thread.
  if (!io_context_.get_executor().running_in_this_thread())
    std::this_thread::sleep_for(std::chrono::seconds(1));
  log(INFO,
      "{}Connections hanging around {}",
      log_prefix_,
      std::accumulate(std::begin(weak_ptrs), std::end(weak_ptrs), 0L, [](long result, auto& ptr) {
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
        return result + ((conn && !conn->closed()) ? 1U : 0U);
      });
}

void PeerConnectionMgr::set_connection(
    uint16_t                                                   idx,
    weak_ptr<PeerConnectionApi> const&                         server,
    std::function<void(shared_ptr<RaftPeerConnection>)> const& on_connect,
    Result<socket_type>                                        socket) {
  if (!socket) {
    log(INFO, "{}Unable to connect to node={}: {}", log_prefix_, idx, socket.error().message());
    set_connection(idx, nullptr);

  } else {
    log(INFO, "{}Connected to node={}", log_prefix_, idx);

    auto created = Connection::create<RaftPeerConnection>(
        self_id_,
        /*peer_id=*/RaftNodeId{idx},
        server,
        std::move(socket.value()),
        std::chrono::seconds(options_.connection_timeout_secs),
        options_);

    if (auto conn = set_connection(idx, created); conn)
      on_connect(conn);
    else
      created->close();
  }
}

void PeerConnectionMgr::reconnect(weak_ptr<PeerConnectionApi> const&                  server,
                                  Config const&                                       config,
                                  std::function<void(shared_ptr<RaftPeerConnection>)> on_connect) {
  auto connections = get_connections();

  for (size_t idx = 0U; idx < connections.size(); ++idx) {
    if (INT(self_id_) == idx)
      continue;

    auto& conn = connections.at(idx);
    if (conn && !conn->closed())
      continue;

    auto       last_connect = last_connect_attempt_.at(idx).load();
    auto const due =
        last_connect == 0 || (last_connect != kConnecting &&
                              last_connect + options().reconnect_interval_msecs < epoch_msec());
    if (due && last_connect_attempt_.at(idx).compare_exchange_strong(last_connect, kConnecting)) {
      log(INFO, "{}Connecting to node={} last try @ {}", log_prefix_, idx, last_connect);

      async_connect(io_context(),
                    raft_identity(self_id_),
                    HostAddress{
                        .address = config.servers.at(idx).hostname,
                        .port    = config.servers.at(idx).ha_port,
                    },
                    [this, s = self(), idx, server, on_connect](Result<socket_type> socket) {
                      set_connection(
                          static_cast<uint16_t>(idx), server, on_connect, std::move(socket));
                    });
    }
  }
}

unsigned PeerConnectionMgr::run(std::function<void(shared_ptr<RaftPeerConnection>)> const& func) {
  unsigned closed = 0;
  // The nearest follower first (the moving average of its round trips; unmeasured ones last):
  // a commit needs the first replies, and each send delays the next by its copy into the socket.
  // The reader threads update the round trips meanwhile: each is read once, then sorted on (a
  // key that changes during the sort breaks it).
  vector<std::pair<uint64_t, shared_ptr<RaftPeerConnection>>> order;
  for (auto& c : get_connections()) {
    auto const ns = c ? c->round_trip_ns() : 0U;
    order.emplace_back(ns == 0 ? std::numeric_limits<uint64_t>::max() : ns, std::move(c));
  }
  std::ranges::stable_sort(order, {}, &std::pair<uint64_t, shared_ptr<RaftPeerConnection>>::first);
  for (auto& [_, conn] : order) {
    if (conn && !conn->closed())
      func(conn);
    else
      ++closed;
  }
  return nodes() - closed;
}

unsigned PeerConnectionMgr::post(std::function<void(shared_ptr<RaftPeerConnection>)> func) {
  unsigned closed = 0;

  auto connections = get_connections();
  for (auto& conn : connections) {
    if (conn && !conn->closed())
      ::boost::asio::post(io_context(), [conn, func] { func(conn); });
    else
      ++closed;
  }
  return nodes() - closed;
}
