#pragma once

#include "src/net/connection.h"

struct Server : public std::enable_shared_from_this<Server> {
  /// Server should be constructed at the start, it can abort.
  Server(boost::asio::ip::tcp::acceptor acceptor);

  Server(boost::asio::io_context& io_context, boost::asio::ip::tcp::endpoint const& endpoint)
      : Server(boost::asio::ip::tcp::acceptor(io_context, endpoint)) {}

  virtual ~Server();

  template <class ServerType, typename... Args>
  static shared_ptr<ServerType> create(Args&&... args) {
    auto s = make_shared<ServerType>(std::forward<Args>(args)...);
    s->start();
    return s;
  }

  template <typename ServerType = Server>
  shared_ptr<ServerType> self() {
    if constexpr (std::is_same_v<ServerType, Server>)
      return shared_from_this();
    else
      return std::dynamic_pointer_cast<ServerType>(shared_from_this());
  }

  /// Server implements server initialization and call do_accept.
  virtual void start() = 0;

  // Function, when server no longer accepts any more connections.
  virtual void on_shutdown() = 0;

  // Construct a client connection when a remote client connects to the server.
  virtual void new_connection(string peer, socket_type socket) = 0;

  weak_ptr<Server> weak_from_this() { return shared_from_this(); }

  void shutdown();

  bool shutdown_signal() const { return shutdown_signal_.test(); }

  HostAddress const& host() const { return host_; }

 protected:
  void acceptor_cancel();
  void do_accept();

 private:
  void do_handshake();

  ::boost::asio::io_context&       io_context_;
  ::boost::asio::ip::tcp::acceptor acceptor_;

  HostAddress const host_;

  std::atomic_flag shutdown_signal_;

  std::latch       accept_latch_{1};
  std::atomic_uint accept_counter_{0};
};
