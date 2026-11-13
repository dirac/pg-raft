#include "src/proxy/proxy.h"

#include "src/base/logger.h"
#include "src/net/async_connect.h"
#include "src/proxy/admin.h"

using tcp        = asio::ip::tcp;
using error_code = boost::system::error_code;

// ── LeaderTracker ─────────────────────────────────────────────────────────────

LeaderTracker::LeaderTracker(asio::io_context&   io,
                             Config const&       config,
                             ProxyOptions const& options,
                             ChangeCb            cb)
    : io_(io),
      config_(config),
      options_(options),
      on_change_(std::move(cb)),
      conns_(config.servers.size()),
      connecting_(config.servers.size(), false),
      timer_(io) {}

void LeaderTracker::start() {
  for (unsigned i = 0; i < config_.servers.size(); ++i)
    connect(i);
  check();
}

void LeaderTracker::stop() {
  vector<shared_ptr<RaftServerConnection>> conns;
  {
    auto lk  = std::unique_lock{mtx_};
    stopped_ = true;
    timer_.cancel();
    conns.swap(conns_);
  }
  for (auto& c : conns)
    if (c)
      c->close();
}

std::optional<unsigned> LeaderTracker::primary() const {
  auto lk = std::unique_lock{mtx_};
  return primary_;
}

vector<shared_ptr<RaftServerConnection>> LeaderTracker::connections() const {
  auto lk = std::unique_lock{mtx_};
  return conns_;
}

void LeaderTracker::connect(unsigned node) {
  {
    auto lk = std::unique_lock{mtx_};
    if (stopped_ || connecting_.at(node))
      return;
    connecting_.at(node) = true;
  }
  auto const& server = config_.servers.at(node);
  async_connect(io_,
                "proxy",
                HostAddress{.address = server.hostname, .port = server.ha_port},
                [this, self = shared_from_this(), node](Result<socket_type> socket) {
                  auto lk              = std::unique_lock{mtx_};
                  connecting_.at(node) = false;
                  if (stopped_ || !socket)
                    return;  // check() tries again
                  conns_.at(node) = Connection::create<RaftServerConnection>(
                      "proxy",
                      RaftNodeId(static_cast<uint16_t>(node)),
                      options_.status_timeout,
                      std::move(socket.value()));
                });
}

void LeaderTracker::check() {
  std::optional<unsigned> primary;
  vector<unsigned>        reconnect;
  bool                    changed = false;
  {
    auto lk = std::unique_lock{mtx_};
    if (stopped_)
      return;

    // The primary: the one node reporting leader and serving.
    unsigned serving = 0;
    for (unsigned i = 0; i < conns_.size(); ++i) {
      auto const& c = conns_.at(i);
      if (!c || c->closed()) {
        reconnect.push_back(i);
        continue;
      }
      if (c->is_serving()) {
        ++serving;
        primary = i;
      }
    }
    if (serving != 1)
      primary.reset();

    if (primary != primary_) {
      log(INFO,
          "Proxy: the primary is {}",
          primary ? std::format("server {}", *primary) : string{"none"});
      primary_ = primary;
      changed  = true;
    }

    timer_.expires_after(std::chrono::milliseconds{100});
    timer_.async_wait([this, self = shared_from_this()](error_code ec) {
      if (!ec)
        check();
    });
  }

  for (auto i : reconnect)
    connect(i);
  if (changed && on_change_)
    on_change_(primary);
}

// ── Bridge ────────────────────────────────────────────────────────────────────

/// A client and its upstream connection: bytes are pumped both ways, one read at a time in each
/// direction (back-pressure is natural), and the first error or EOF closes both, once. Everything
/// runs on the bridge's strand; close() can be called from any thread (a primary change).
class Bridge : public std::enable_shared_from_this<Bridge> {
 public:
  Bridge(asio::io_context& io,
         tcp::socket       client,
         tcp::socket       upstream,
         unsigned          node,
         bool              primary)
      : strand_(asio::make_strand(io)),
        client_(std::move(client)),
        upstream_(std::move(upstream)),
        node_(node),
        primary_(primary) {}

  void start() {
    asio::post(strand_, [self = shared_from_this()] {
      self->pump(self->client_, self->upstream_, self->down_buf_);
      self->pump(self->upstream_, self->client_, self->up_buf_);
    });
  }

  void close() {
    asio::post(strand_, [self = shared_from_this()] { self->do_close(); });
  }

  unsigned node() const { return node_; }
  bool     primary() const { return primary_; }

 private:
  void do_close() {
    if (closed_)
      return;
    closed_ = true;
    error_code ec;
    client_.shutdown(tcp::socket::shutdown_both, ec);
    upstream_.shutdown(tcp::socket::shutdown_both, ec);
    client_.close(ec);
    upstream_.close(ec);
  }

  void pump(tcp::socket& from, tcp::socket& to, std::array<char, 16384>& buf) {
    if (closed_)
      return;
    from.async_read_some(
        asio::buffer(buf),
        asio::bind_executor(
            strand_, [this, self = shared_from_this(), &from, &to, &buf](error_code ec, size_t n) {
              if (ec || closed_)
                return do_close();
              asio::async_write(
                  to,
                  asio::buffer(buf.data(), n),
                  asio::bind_executor(strand_,
                                      [this, self, &from, &to, &buf](error_code wec, size_t) {
                                        if (wec || closed_)
                                          return do_close();
                                        pump(from, to, buf);
                                      }));
            }));
  }

  asio::strand<asio::io_context::executor_type> strand_;
  tcp::socket                                   client_;
  tcp::socket                                   upstream_;
  unsigned const                                node_;
  bool const                                    primary_;
  std::array<char, 16384>                       down_buf_{};     // client -> upstream
  std::array<char, 16384>                       up_buf_{};       // upstream -> client
  bool                                          closed_{false};  // on the strand
};

// ── Proxy ─────────────────────────────────────────────────────────────────────

namespace {

void tune(tcp::socket& s) {
  error_code ec;
  s.set_option(tcp::no_delay(true), ec);
  s.set_option(asio::socket_base::keep_alive(true), ec);
}

}  // namespace

shared_ptr<Proxy> Proxy::create(asio::io_context& io, Config config, ProxyOptions options) {
  return shared_ptr<Proxy>(new Proxy(io, std::move(config), std::move(options)));
}

Proxy::Proxy(asio::io_context& io, Config config, ProxyOptions options)
    : io_(io), config_(std::move(config)), options_(std::move(options)) {}

Result<void> Proxy::start() {
  tracker_ = make_shared<LeaderTracker>(
      io_, config_, options_, [w = weak_from_this()](std::optional<unsigned> p) {
        if (auto self = w.lock())
          self->on_primary_change(p);
      });

  auto const address = asio::ip::make_address(options_.listen_address);
  for (size_t i = 0; i <= config_.servers.size(); ++i) {
    auto const port = static_cast<uint16_t>(config_.proxy_pg_port + i);
    auto       a    = make_unique<tcp::acceptor>(io_);
    error_code ec;
    a->open(tcp::v4(), ec);
    if (!ec)
      a->set_option(tcp::acceptor::reuse_address(true), ec);
    if (!ec)
      a->bind(tcp::endpoint(address, port), ec);
    if (!ec)
      a->listen(asio::socket_base::max_listen_connections, ec);
    if (ec)
      return elog(ec, "Proxy: unable to listen on port {}", port);
    acceptors_.push_back(std::move(a));
  }

  if (options_.admin) {
    admin_ = make_shared<AdminServer>(io_, weak_from_this());
    if (auto r = admin_->start(address, config_.proxy_http_port); !r)
      return r;
  }

  tracker_->start();
  for (size_t i = 0; i < acceptors_.size(); ++i)
    accept(i);
  log(INFO,
      "Proxy: primary on port {}, servers on {}..{}",
      config_.proxy_pg_port,
      config_.proxy_pg_port + 1,
      config_.proxy_pg_port + config_.servers.size());
  return ok();
}

void Proxy::stop() {
  if (stopped_.exchange(true))
    return;
  for (auto& a : acceptors_) {
    error_code ec;
    a->close(ec);
  }
  if (admin_)
    admin_->stop();
  if (tracker_)
    tracker_->stop();
  auto lk = std::unique_lock{bridges_mtx_};
  for (auto& w : bridges_)
    if (auto b = w.lock())
      b->close();
  bridges_.clear();
}

void Proxy::accept(size_t listener) {
  auto client = make_shared<tcp::socket>(io_);
  acceptors_.at(listener)->async_accept(
      *client, [this, self = shared_from_this(), listener, client](error_code ec) {
        if (stopped_.load())
          return;
        if (!ec) {
          tune(*client);
          if (listener == 0)
            route_primary(client, std::chrono::steady_clock::now() + options_.failover_window);
          else
            connect_upstream(client, static_cast<unsigned>(listener - 1), /*primary=*/false);
        } else if (ec == asio::error::operation_aborted) {
          return;
        }
        accept(listener);
      });
}

void Proxy::route_primary(shared_ptr<tcp::socket>               client,
                          std::chrono::steady_clock::time_point deadline) {
  if (stopped_.load())
    return;
  if (auto p = tracker_->primary())
    return connect_upstream(client, *p, /*primary=*/true);

  // No primary (an election or a promotion): hold the client, then give up. The proxy doesn't
  // speak the protocol, so the client just sees its connection closed, and reconnects.
  if (std::chrono::steady_clock::now() >= deadline) {
    log(INFO, "Proxy: no primary within the failover window: closing a client");
    error_code ec;
    client->close(ec);
    return;
  }
  auto timer = make_shared<asio::steady_timer>(io_, std::chrono::milliseconds{100});
  timer->async_wait([this, self = shared_from_this(), client, deadline, timer](error_code) {
    route_primary(client, deadline);
  });
}

void Proxy::connect_upstream(shared_ptr<tcp::socket> client, unsigned node, bool primary) {
  auto const& server   = config_.servers.at(node);
  auto        upstream = make_shared<tcp::socket>(io_);
  auto        resolver = make_shared<tcp::resolver>(io_);
  auto        timer    = make_shared<asio::steady_timer>(io_, options_.connect_timeout);
  timer->async_wait([upstream](error_code ec) {
    if (!ec) {
      error_code e;
      upstream->close(e);  // the connect fails with operation_aborted
    }
  });

  resolver->async_resolve(
      server.hostname,
      std::to_string(server.pg_port),
      [this, self = shared_from_this(), client, upstream, resolver, timer, node, primary](
          error_code ec, tcp::resolver::results_type results) {
        if (ec) {
          error_code e;
          client->close(e);
          return;
        }
        asio::async_connect(
            *upstream,
            results,
            [this, self, client, upstream, timer, node, primary](error_code cec,
                                                                 tcp::endpoint const&) {
              timer->cancel();
              if (cec || stopped_.load()) {
                log(DEBUG, "Proxy: unable to reach server {}: {}", node, cec.message());
                error_code e;
                client->close(e);
                return;
              }
              tune(*upstream);
              auto bridge =
                  make_shared<Bridge>(io_, std::move(*client), std::move(*upstream), node, primary);

              // A primary that changed while this one was connecting: the bridge is closed below.
              {
                auto lk = std::unique_lock{bridges_mtx_};
                std::erase_if(bridges_, [](auto const& w) { return w.expired(); });
                bridges_.push_back(bridge);
              }
              bridge->start();
              if (primary && tracker_->primary() != std::optional<unsigned>{node})
                bridge->close();
            });
      });
}

uint16_t Proxy::admin_port() const {
  return admin_ ? admin_->port() : 0;
}

boost::json::object Proxy::status() const {
  auto lsn = [](std::optional<uint64_t> l) -> boost::json::value {
    if (!l)
      return nullptr;
    return boost::json::value(
        boost::json::string(std::format("{:X}/{:X}", *l >> 32U, *l & 0xFFFFFFFFU)));
  };

  auto const primary = tracker_->primary();
  auto const conns   = tracker_->connections();

  boost::json::array servers;
  for (unsigned i = 0; i < config_.servers.size(); ++i) {
    auto const&         s = config_.servers.at(i);
    boost::json::object o{
        {"id", i}, {"hostname", s.hostname}, {"pg_port", s.pg_port}, {"ha_port", s.ha_port}};
    auto const c   = i < conns.size() ? conns.at(i) : nullptr;
    o["connected"] = c && !c->closed();
    o["leader"]    = c && c->is_leader();
    o["serving"]   = c && c->is_serving();
    if (c && !c->closed()) {
      if (auto st = c->node_status(); st) {
        o["applied_idx"]         = INT(st->applied_idx);
        o["primary_flushed_lsn"] = lsn(st->primary_flushed_lsn);
        o["standby_applied_lsn"] = lsn(st->standby_applied_lsn);
      } else {
        o["error"] = st.error().message();
      }
    }
    servers.push_back(std::move(o));
  }

  // The open bridges, per port.
  uint64_t              on_primary = 0;
  std::vector<uint64_t> on_server(config_.servers.size(), 0);
  {
    auto lk = std::unique_lock{bridges_mtx_};
    for (auto const& w : bridges_)
      if (auto b = w.lock()) {
        if (b->primary())
          ++on_primary;
        else
          ++on_server.at(b->node());
      }
  }

  return boost::json::object{
      {"proxy_pg_port", config_.proxy_pg_port},
      {"primary", primary ? boost::json::value(*primary) : boost::json::value(nullptr)},
      {"servers", std::move(servers)},
      {"bridges",
       boost::json::object{{"primary", on_primary},
                           {"servers", boost::json::array(on_server.begin(), on_server.end())}}},
  };
}

void Proxy::on_primary_change(std::optional<unsigned> primary) {
  // The old primary's clients are cut: they reconnect and reach the new one.
  auto lk = std::unique_lock{bridges_mtx_};
  for (auto const& w : bridges_)
    if (auto b = w.lock(); b && b->primary() && std::optional<unsigned>{b->node()} != primary)
      b->close();
}
