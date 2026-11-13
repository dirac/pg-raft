#pragma once

#include "src/base/base.h"

class Proxy;

// The proxy's admin interface (docs/proxy.md): HTTP/1.1 on proxy_http_port, with Boost.Beast,
// read-only. The extension picks the format: /status.json and /primary.json are JSON,
// /status.html and /primary.html are HTML pages (refreshing every 5 s); without one, JSON.
//
//   GET /health   200 "ok"
//   GET /primary  200 {"server": i, "hostname", "pg_port"}, or 503 {"server": null} when there is
//                 no primary (an election or a promotion)
//   GET /status   200 {"proxy_pg_port", "primary", "servers": [{"id", "hostname", "pg_port",
//                 "ha_port", "connected", "leader", "serving", "applied_idx",
//                 "primary_flushed_lsn", "standby_applied_lsn"}], "bridges": {"primary",
//                 "servers": [...]}}
//
// Anything else is 404 (405 for another method). /status asks every node for its raft status (up
// to 2 s each), on a thread of its own; the response is written on the session's executor.
class AdminServer : public std::enable_shared_from_this<AdminServer> {
 public:
  AdminServer(asio::io_context& io, weak_ptr<Proxy> proxy);

  Result<void> start(asio::ip::address const& address, uint16_t port);
  void         stop();
  uint16_t     port() const { return port_; }

 private:
  void accept();

  asio::io_context&       io_;
  weak_ptr<Proxy>         proxy_;
  asio::ip::tcp::acceptor acceptor_;
  uint16_t                port_{0};
};
