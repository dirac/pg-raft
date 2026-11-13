#pragma once

#include "src/base/config.h"
#include "src/raft/raft_server_connection.h"

// The client entry point (docs/proxy.md): a plain TCP proxy on Boost.Asio that forwards
// PostgreSQL's protocol byte for byte (TLS passes through to PostgreSQL):
//
//   proxy_pg_port          -> the PostgreSQL of the node that raft reports as leader and serving
//   proxy_pg_port + 1 + i  -> servers[i]'s PostgreSQL, whatever its role
//
// The leader comes from raft's client channel: a RaftServerConnection to every node's ha_port,
// whose heartbeats carry the node's status byte (0x3: leader and serving). A client of the primary
// port that arrives while there is no primary is held for up to failover_window, then closed. When
// the primary changes, the primary port's bridges to the old one are closed (its transactions are
// lost anyway); the per-server ports' bridges are not touched.

struct ProxyOptions {
  std::chrono::milliseconds failover_window{30000};  // hold a primary-port client this long
  std::chrono::milliseconds connect_timeout{5000};   // upstream connect
  std::chrono::milliseconds status_timeout{1000};    // raft connections: heartbeats every 100 ms
  std::chrono::milliseconds reconnect_interval{500};
  bool                      admin          = true;  // the admin interface on proxy_http_port
  string                    listen_address = "0.0.0.0";
};

/// Which node is the primary: one RaftServerConnection per node, reconnected when it closes.
class LeaderTracker : public std::enable_shared_from_this<LeaderTracker> {
 public:
  using ChangeCb = std::function<void(std::optional<unsigned>)>;

  LeaderTracker(asio::io_context&   io,
                Config const&       config,
                ProxyOptions const& options,
                ChangeCb            cb);

  void start();
  void stop();

  /// The node whose connection reports leader and serving; nullopt if none, or more than one
  /// (during a transition).
  std::optional<unsigned> primary() const;

  /// The connection to each node (nullptr while it is not connected).
  vector<shared_ptr<RaftServerConnection>> connections() const;

 private:
  void connect(unsigned node);
  void check();

  asio::io_context&   io_;
  Config const&       config_;
  ProxyOptions const& options_;
  ChangeCb            on_change_;

  mutable std::mutex                       mtx_;
  vector<shared_ptr<RaftServerConnection>> conns_;
  vector<bool>                             connecting_;
  std::optional<unsigned>                  primary_;
  bool                                     stopped_{false};
  asio::steady_timer                       timer_;
};

class Bridge;
class AdminServer;

class Proxy : public std::enable_shared_from_this<Proxy> {
 public:
  static shared_ptr<Proxy> create(asio::io_context& io, Config config, ProxyOptions options = {});

  /// Bind the listeners and start; false if a port can't be bound.
  Result<void> start();
  void         stop();

  std::optional<unsigned> primary() const { return tracker_->primary(); }

  /// The admin interface's view (GET /status). Blocks up to 2 s per node (a raft status request):
  /// call it off the io threads.
  boost::json::object status() const;

  Config const& config() const { return config_; }

  /// The admin interface's port (proxy_http_port; 0 when it is not running).
  uint16_t admin_port() const;

 private:
  Proxy(asio::io_context& io, Config config, ProxyOptions options);

  void accept(size_t listener);
  void route_primary(shared_ptr<asio::ip::tcp::socket>     client,
                     std::chrono::steady_clock::time_point deadline);
  void connect_upstream(shared_ptr<asio::ip::tcp::socket> client, unsigned node, bool primary);
  void on_primary_change(std::optional<unsigned> primary);

  asio::io_context&                           io_;
  Config const                                config_;
  ProxyOptions const                          options_;
  shared_ptr<LeaderTracker>                   tracker_;
  vector<unique_ptr<asio::ip::tcp::acceptor>> acceptors_;  // [0]: primary, [1 + i]: servers[i]

  shared_ptr<AdminServer> admin_;

  mutable std::mutex       bridges_mtx_;
  vector<weak_ptr<Bridge>> bridges_;
  std::atomic_bool         stopped_{false};
};
