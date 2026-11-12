#include "src/net/server.h"

#include "src/base/logger.h"

// Ported from sparrowdb lib/net/server.cpp, without TLS (certificates) and roles.

Server::Server(boost::asio::ip::tcp::acceptor acceptor)
    : io_context_(static_cast<boost::asio::io_context&>(acceptor.get_executor().context())),
      acceptor_(std::move(acceptor)),
      host_(HostAddress{
          .address = string{acceptor_.local_endpoint().address().to_string()},
          .port    = acceptor_.local_endpoint().port(),
      }),
      ssl_byte_(make_shared<uint8_t>(static_cast<uint8_t>(0))) {
  shutdown_signal_.clear();
}

Server::~Server() = default;

void Server::acceptor_cancel() {
  if (!acceptor_.is_open())
    return;

  acceptor_.cancel();
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  while (!accept_latch_.try_wait()) {
    acceptor_.cancel();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  acceptor_.close();
}

void Server::shutdown() {
  if (shutdown_signal_.test_and_set())
    return;

  acceptor_cancel();
  on_shutdown();
}

namespace {

using Handler = std::function<void(string, socket_type)>;

struct HandshakeConnection : public std::enable_shared_from_this<HandshakeConnection> {
  HandshakeConnection(tcp_socket socket, Handler handler)
      : timer_(socket.get_executor()), socket_(std::move(socket)), handler_(std::move(handler)) {}

  void start(shared_ptr<uint8_t> const& ssl_byte, bool do_ssl) {
    timer_.expires_after(std::chrono::milliseconds(10000));
    timer_.async_wait([this, self = shared_from_this()](boost::system::error_code const& ec) {
      if (!ec)
        cancel();
    });

    auto completion_handler = [this, ssl_byte, do_ssl, self = shared_from_this()](
                                  boost::system::error_code const& ec, size_t) {
      if (ec) {
        log(ERROR, "Sending SSL bit failed {}", ec.message());
        return;
      }

      // TLS is not supported yet (the server never sets the byte).
      assert(!do_ssl);
      (void)do_ssl;
      get_peer_info();
    };
    boost::asio::async_write(std::get<tcp_socket>(socket_),
                             boost::asio::const_buffer(ssl_byte.get(), 1),
                             std::move(completion_handler));
  }

  void get_peer_info() {
    auto p = make_shared<std::array<char, 64U>>();

    auto completion_handler = [this, p, self = shared_from_this()](
                                  boost::system::error_code const& error, size_t) {
      timer_.cancel();

      if (error) {
        log(ERROR, "Ack failed {}", error.message());
        return;
      }

      if (p->back() != '\0') {
        log(ERROR, "Data not null-terminated");
        return;
      }
      log(INFO, "Connected to {}", p->data());

      handler_(string{p->data()}, std::move(socket_));
    };

    boost::asio::async_read(std::get<tcp_socket>(socket_),
                            boost::asio::mutable_buffer(p->data(), p->size()),
                            std::move(completion_handler));
  }

 private:
  void cancel() {
    std::unique_lock lk{mtx_};

    boost::system::error_code ec;
    switch (socket_.index()) {
      case 0:
        std::get<0>(socket_).cancel(ec);
        break;

      case 1:
        std::get<1>(socket_).next_layer().cancel(ec);
        break;
    }
  }

  boost::asio::steady_timer timer_;
  socket_type               socket_;

  std::mutex mtx_;

  Handler handler_;
};

// Errors caused by a single incoming connection: keep accepting.
bool transient_accept_error(boost::system::error_code const& ec) {
  // (would_block is the same error as try_again.)
  return ec == boost::asio::error::connection_aborted ||
         ec == boost::asio::error::connection_reset || ec == boost::asio::error::interrupted ||
         ec == boost::asio::error::try_again;
}

// Errors caused by a temporary lack of resources: keep accepting after a delay.
bool resource_accept_error(boost::system::error_code const& ec) {
  return ec == boost::asio::error::no_descriptors ||
         ec == boost::system::errc::too_many_files_open_in_system ||
         ec == boost::asio::error::no_buffer_space || ec == boost::asio::error::no_memory;
}
}  // namespace

void Server::accept_later() {
  auto lk = std::unique_lock{accept_timer_mtx_};
  accept_timer_.expires_after(std::chrono::milliseconds(100));
  accept_timer_.async_wait(
      [this, self = shared_from_this()](boost::system::error_code const&) { do_accept(); });
}

void Server::do_accept() {
  if (shutdown_signal()) {
    accept_latch_.count_down();
    return;
  }

  auto accept_handler = [this, self = shared_from_this()](boost::system::error_code const& ec,
                                                          boost::asio::ip::tcp::socket     socket) {
    if (shutdown_signal() || ec == boost::asio::error::operation_aborted) {
      accept_latch_.count_down();
      if (ec)
        log(INFO, "{}", ec.message());
      return;
    }

    if (ec) {
      if (transient_accept_error(ec)) {
        log(ERROR, "Accept failed, continuing {}", ec.message());
        return do_accept();
      }
      if (resource_accept_error(ec)) {
        log(ERROR, "Accept failed, retrying later {}", ec.message());
        return accept_later();
      }
      log(ERROR, "Accept failed, shutting down {}", ec.message());
      accept_latch_.count_down();
      return shutdown();
    }

    make_shared<HandshakeConnection>(std::move(socket),
                                     [this, self](string peer, socket_type socket) {
                                       if (!shutdown_signal())
                                         new_connection(std::move(peer), std::move(socket));
                                     })
        ->start(ssl_byte_, ssl_enabled());

    // The server can be shut down before it accepts again.
    do_accept();
  };
  acceptor_.async_accept(std::move(accept_handler));
}
