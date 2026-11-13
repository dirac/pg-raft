#pragma once

// What the Linux engines of the proxy's data path (epoll_relay.cc, uring_relay.cc) share.

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include "src/base/base.h"
#include "src/base/logger.h"

namespace relay {

/// The proxy's sockets, both sides: no Nagle, keepalive.
inline void tune(int fd) {
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));
}

/// A server's PostgreSQL address, resolved once (again on failure).
struct Upstream {
  string           host;
  uint16_t         port = 0;
  sockaddr_storage addr{};
  socklen_t        len = 0;
};

inline bool resolve(Upstream& u) {
  addrinfo hints{};
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res     = nullptr;
  if (::getaddrinfo(u.host.c_str(), std::to_string(u.port).c_str(), &hints, &res) != 0 || !res)
    return false;
  std::memcpy(&u.addr, res->ai_addr, res->ai_addrlen);
  u.len = res->ai_addrlen;
  ::freeaddrinfo(res);
  return true;
}

/// A listening socket on address:port, SO_REUSEPORT (one per thread: the kernel spreads the
/// connections over them).
inline Result<int> listen_socket(string const& address, uint16_t port, bool nonblocking) {
  sockaddr_storage addr{};
  socklen_t        len = 0;
  if (auto* a4 = reinterpret_cast<sockaddr_in*>(&addr);
      ::inet_pton(AF_INET, address.c_str(), &a4->sin_addr) == 1) {
    a4->sin_family = AF_INET;
    a4->sin_port   = htons(port);
    len            = sizeof(sockaddr_in);
  } else if (auto* a6 = reinterpret_cast<sockaddr_in6*>(&addr);
             ::inet_pton(AF_INET6, address.c_str(), &a6->sin6_addr) == 1) {
    a6->sin6_family = AF_INET6;
    a6->sin6_port   = htons(port);
    len             = sizeof(sockaddr_in6);
  } else {
    return elog(system_error_code(EINVAL), "Proxy: bad listen address {}", address);
  }

  int fd =
      ::socket(addr.ss_family, SOCK_STREAM | SOCK_CLOEXEC | (nonblocking ? SOCK_NONBLOCK : 0), 0);
  if (fd < 0)
    return elog(system_error_code(errno), "Proxy: socket");
  int one = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), len) != 0 || ::listen(fd, 1024) != 0) {
    auto e = errno;
    ::close(fd);
    return elog(system_error_code(e), "Proxy: unable to listen on port {}", port);
  }
  return fd;
}

}  // namespace relay
