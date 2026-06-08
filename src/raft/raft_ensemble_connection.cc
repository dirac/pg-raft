#include "src/raft/raft_ensemble_connection.h"

#include "src/net/async_connect.h"

namespace {

struct Coordination {
  std::mutex              mtx{};
  std::condition_variable cv{};

  unsigned const nodes;

  std::atomic<bool>     leader_found{false};
  std::atomic<unsigned> responses{0};

  void update(Result<bool> v) {
    leader_found = leader_found || (v && v.value());
    ++responses;

    cv.notify_all();
  }

  bool is_done() { return leader_found || nodes == responses; }

  Coordination(unsigned n) : nodes(n) {}
};

vector<HostAddress> addresses(Config const& cfg) {
  vector<HostAddress> result;

  for (auto const& zk : cfg.zk_servers)
    result.emplace_back(HostAddress{
        .address = zk.hostname,
        .port    = zk.port,
    });
  return result;
}
}  // namespace

RaftEnsembleConnection::RaftEnsembleConnection(boost::asio::io_context& io_context,
                                               Config const&            config,
                                               Role                     role,
                                               string                   self,
                                               Certificates const&      certs,
                                               std::chrono::seconds     timeout)
    : io_context_(io_context),
      nodes_(static_cast<unsigned>(config.zk_servers.size())),
      timeout_(timeout),
      addresses_(addresses(config)),
      mutexes_(nodes_),
      connections_(nodes_),
      timer_(io_context),
      name_(fn::format("{0}:{1}", to_string(role), self)),
      log_prefix_(fn::format("({0}:{1}) ", to_string(role), self)) {
  while (ssl_contexts_.size() < nodes())
    ssl_contexts_.emplace_back(fn::format("{0}:{1}", to_string(role), self), certs);

  closed_.clear();
  session_ping_started_.clear();
}

RaftEnsembleConnection::~RaftEnsembleConnection() {
  close();
}

bool RaftEnsembleConnection::connect() {
  if (session_id_)
    exec(ZkRequest{.commands = {CloseSessionCommand{.session_id = session_id_}}});

  session_id_ = 0;
  auto response =
      exec(ZkRequest{.commands = {NewSessionCommand{.timeout_seconds = kSessionTimeoutSeconds}}});

  if (response.status == ResultStatus::kOk) {
    session_id_ = std::get<NewSessionResult>(response.results.front().result).session_id;

    if (session_ping_started_.test_and_set())
      session_ping();
  }

  LOG_INFO << log_prefix_ << "ZkSession Id = " << session_id_;
  return session_id_ != 0;
}

ZkResponse RaftEnsembleConnection::exec(ZkRequest request, unsigned tries_on_close) {
  auto lk = lock();

  // The request is to keep the session alive and fetch the watches.
  bool keep_session_alive = request.commands.size() == 0;
  if (keep_session_alive && ((last_request_epoch_msecs_ - epoch_msec()) < ping_duration_msecs_))
    return ZkResponse{};

  if (session_id_ == 0) [[unlikely]] {
    if (request.commands.size() != 1 ||
        !std::holds_alternative<NewSessionCommand>(request.commands.front()))
      return ZkResponse{.status = ResultStatus::kSessionExpired};
  }

  request.session_id = session_id_;
  request.request_no = ++request_no_;

  unsigned tries = 0;
  while (lk && (!closed() || (++tries <= tries_on_close))) {
    auto conn = leader();
    if (conn) {
      auto r = conn->exec(request);
      if (r.has_error())
        LOG_ERROR << log_prefix_ << "Connection error";
      else if (r.value().status == ResultStatus::kInvalidLeader)
        LOG_ERROR << log_prefix_ << "No longer a leader " << conn->node_id();
      else {
        last_request_epoch_msecs_ = epoch_msec();
        return process_watches(std::move(r.value()));
      }
      // Sleep for a bit.
    } else {
      // LOG_ERROR << log_prefix_ << "Leader not found.";
      std::this_thread::sleep_for(std::chrono::milliseconds(250));
      // Sleep for a bit.
    }

    if (keep_session_alive) {
      lk.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      lk.try_lock();
    }
  }

  assert(closed() || keep_session_alive);
  return ZkResponse{
      .status = ResultStatus::kUnknown,
      .state  = SessionState::kUnknown,
  };
}

void RaftEnsembleConnection::watch_event(WatchEvent const&) {}

ZkResponse RaftEnsembleConnection::process_watches(ZkResponse response) {
  std::for_each(std::begin(response.events),
                std::end(response.events),
                [this](WatchEvent const& we) { watch_event(we); });
  response.events.clear();
  return response;
}

void RaftEnsembleConnection::close_session() {
  timer_.cancel();
  exec(ZkRequest{.commands = {CloseSessionCommand{.session_id = session_id_}}}, 1U);

  std::for_each(std::begin(connections_), std::end(connections_), [](auto& conn) {
    if (conn && !conn->closed())
      conn->close();
    conn.reset();
  });
}

void RaftEnsembleConnection::session_ping() {
  if (closed())
    return;
  exec(ZkRequest{});

  timer_.expires_after(std::chrono::milliseconds(ping_duration_msecs_));
  timer_.async_wait([this, s = self()](boost::system::error_code const& ec) {
    if (!ec)
      session_ping();
    else
      LOG_ERROR << log_prefix_ << "Timer error " << ec.message();
  });
}

void RaftEnsembleConnection::ping_all(std::function<void(Result<bool>)> callback) {
  for (unsigned idx = 0U; idx < nodes(); ++idx) {
    std::unique_lock<std::mutex> lk(mutexes_.at(idx));

    auto& conn = connections_.at(idx);

    if (!conn || conn->closed())
      async_connect(idx, callback);
    else
      conn->async_leadership_check(callback);
  }
}

void RaftEnsembleConnection::async_connect(unsigned                          idx,
                                           std::function<void(Result<bool>)> callback) {
  LOG_INFO << log_prefix_ << "Connecting to raft node " << idx;
  net::async_connect(
      io_context(),
      ssl_contexts_.at(idx),
      addresses_.at(idx),
      [this, s = self(), idx, callback = std::move(callback)](Result<socket_type> socket) {
        if (!socket)
          return callback(socket.error());
        set_connection(idx,
                       Connection<>::create<RaftServerConnection>(
                           name_, RaftNodeId{u2(idx)}, timeout_, std::move(socket.value())),
                       std::move(callback));
      });
}

shared_ptr<RaftServerConnection> RaftEnsembleConnection::set_leader() {
  for (unsigned idx = 0U; idx < nodes(); ++idx) {
    std::unique_lock<std::mutex> lk(mutexes_.at(idx));

    auto& conn = connections_.at(idx);
    if (conn && !conn->closed() && conn->is_leader()) {
      std::unique_lock<std::mutex> llk{leader_mtx_};
      leader_ = conn;
      return conn;
    }
  }
  return nullptr;
}

shared_ptr<RaftServerConnection> RaftEnsembleConnection::get_leader() {
  std::unique_lock<std::mutex> llk{leader_mtx_};
  return leader_.lock();
}

shared_ptr<RaftServerConnection> RaftEnsembleConnection::leader() {
  if (shared_ptr<RaftServerConnection> conn = get_leader();
      conn && !conn->closed() && conn->is_leader())
    return conn;

  if (shared_ptr<RaftServerConnection> conn = set_leader();
      conn && !conn->closed() && conn->is_leader())
    return conn;

  LOG_INFO << log_prefix_ << "Looking for leader";

  auto c        = fn::make_shared<Coordination>(nodes());
  auto callback = [c](Result<bool> r) { c->update(r); };

  std::unique_lock<std::mutex> lk(c->mtx);

  // Send a leadership check.
  ping_all(callback);

  // Wait until we have found a leader or got responses from all nodes.
  if (!c->cv.wait_for(lk, std::chrono::seconds(1), [c] { return c->is_done(); }))
    LOG_ERROR << "Timeout waiting for leadership replies.";

  return set_leader();
}

void RaftEnsembleConnection::set_connection(unsigned idx, shared_ptr<RaftServerConnection> conn) {
  return set_connection(idx, std::move(conn), [](Result<bool>) {});
}

void RaftEnsembleConnection::set_connection(unsigned                          idx,
                                            shared_ptr<RaftServerConnection>  new_conn,
                                            std::function<void(Result<bool>)> leadership_check) {
  LOG_INFO << log_prefix_ << "New connection to raft node " << idx;
  std::unique_lock<std::mutex> lk(mutexes_.at(idx));

  auto& conn = connections_.at(idx);
  if (!conn || conn->closed()) {
    connections_.at(idx) = std::move(new_conn);
    connections_.at(idx)->async_leadership_check(std::move(leadership_check));
  } else
    LOG_INFO << log_prefix_ << "Ignoring the connection, we already have one";
}

