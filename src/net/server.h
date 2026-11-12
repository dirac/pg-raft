#pragma once

#include "src/net/connection.h"

// Handshake of an accepted connection: the server sends one byte, 1 if TLS follows (not yet
// supported, always 0), then reads the client's 64-byte NUL-terminated identity, which is passed
// to new_connection() as |peer|. The handshake must complete within 10 seconds.
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

  bool ssl_enabled() const { return *ssl_byte_ == u1(1); }

  ::boost::asio::io_context& io_context() { return io_context_; }

 protected:
  void acceptor_cancel();
  void do_accept();

  // Retry accepting after a short delay (e.g. when out of file descriptors).
  void accept_later();

 private:
  ::boost::asio::io_context& io_context_;

  // Wraps the acceptor, intentionally hiding its methods to serialize access with a mutex.
  struct Acceptor : ::boost::asio::ip::tcp::acceptor {
   public:
    Acceptor(::boost::asio::ip::tcp::acceptor&& a)
        : ::boost::asio::ip::tcp::acceptor(std::move(a)) {}

    auto local_endpoint() const {  // NOLINT(bugprone-derived-method-shadowing-base-method)
      std::unique_lock lk{mtx_};
      return ::boost::asio::ip::tcp::acceptor::local_endpoint();
    }

    auto is_open() const {  // NOLINT(bugprone-derived-method-shadowing-base-method)
      std::unique_lock lk{mtx_};
      return ::boost::asio::ip::tcp::acceptor::is_open();
    }

    template <typename Handler>
    auto async_accept(Handler&& handler) {
      std::unique_lock lk{mtx_};
      return ::boost::asio::ip::tcp::acceptor::async_accept(std::forward<Handler>(handler));
    }

    auto cancel() {  // NOLINT(bugprone-derived-method-shadowing-base-method)
      std::unique_lock lk{mtx_};
      return ::boost::asio::ip::tcp::acceptor::cancel();
    }

    auto close() {  // NOLINT(bugprone-derived-method-shadowing-base-method)
      std::unique_lock lk{mtx_};
      return ::boost::asio::ip::tcp::acceptor::close();
    }

   private:
    mutable std::mutex mtx_;
  } acceptor_;

  HostAddress const         host_;
  shared_ptr<uint8_t> const ssl_byte_;

  std::atomic_flag shutdown_signal_;

  std::latch accept_latch_{1};

  mutable std::mutex        accept_timer_mtx_;
  boost::asio::steady_timer accept_timer_{io_context_};
};
