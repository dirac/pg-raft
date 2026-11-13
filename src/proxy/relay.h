#pragma once

#include "src/base/config.h"

// The proxy's data path (docs/proxy.md): accepting clients on the proxy's ports and relaying their
// bytes to a PostgreSQL. The control plane (LeaderTracker, the admin interface) stays on Asio; the
// data path has engines:
//
//   asio   the Bridge classes in proxy.cc: portable, every completion through an io_context and a
//          strand (the default off Linux)
//   epoll  epoll_relay.cc (Linux): one thread per core, each with its own epoll loop and its own
//          SO_REUSEPORT listeners, owning its connections from accept to close (the default)
//   uring  uring_relay.cc (Linux): the same, on io_uring

struct ProxyOptions;

/// Open connections per port: the primary port, and each server's.
struct RelayCounts {
  uint64_t         primary = 0;
  vector<uint64_t> servers;
};

class Relay {
 public:
  using PrimaryFn = std::function<std::optional<unsigned>()>;

  virtual ~Relay() = default;

  /// Bind the listeners (an error if a port can't be bound) and start the threads.
  virtual Result<void> start() = 0;
  virtual void         stop()  = 0;

  /// The primary changed: the primary port's connections to another node are closed, and the
  /// clients waiting for a primary are routed.
  virtual void primary_changed(std::optional<unsigned> primary) = 0;

  virtual RelayCounts counts() const = 0;
};

#ifdef __linux__
unique_ptr<Relay> make_epoll_relay(Config const&       config,
                                   ProxyOptions const& options,
                                   Relay::PrimaryFn    primary);
unique_ptr<Relay> make_uring_relay(Config const&       config,
                                   ProxyOptions const& options,
                                   Relay::PrimaryFn    primary);
#endif
