#include "src/net/server.h"

#include "src/base/logger.h"

Server::Server(boost::asio::ip::tcp::acceptor acceptor)
    : io_context_(static_cast<boost::asio::io_context&>(acceptor.get_executor().context())),
      acceptor_(std::move(acceptor)),
      host_(HostAddress{.address = string{acceptor_.local_endpoint().address().to_string()},
                        .port    = acceptor_.local_endpoint().port()}) {
  shutdown_signal_.clear();
}

Server::~Server() {}

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
      : socket_(std::move(socket)), handler_(std::move(handler)) {}

  void start() {
    auto p = make_shared<std::array<char, 64U>>();

    auto completion_handler = [this, p, self = shared_from_this()](
                                  boost::system::error_code const& ec, size_t) {
      if (ec) {
        elog(ec, "Ack failed");
        return;
      }

      if (p->back() != '\0')
        return;

      log(INFO, "Connected to {}", p->data());
      handler_(string{p->data()}, std::move(socket_));
    };
    boost::asio::async_read(
        socket_, boost::asio::mutable_buffer(p->data(), p->size()), std::move(completion_handler));
  }

 private:
  tcp_socket socket_;
  Handler    handler_;
};
}  // namespace

void Server::do_accept() {
  if (shutdown_signal()) {
    accept_latch_.count_down();
    return;
  }

  auto accept_handler = [this, self = shared_from_this()](boost::system::error_code const& ec,
                                                          boost::asio::ip::tcp::socket     socket) {
    if (ec || shutdown_signal()) {
      accept_latch_.count_down();
      if (ec != boost::asio::error::operation_aborted)
        return shutdown();

      elog(ec, "Accept handler");
      return;
    }

    make_shared<HandshakeConnection>(std::move(socket),
                                     [this, self](string peer, socket_type socket) {
                                       if (!shutdown_signal())
                                         new_connection(std::move(peer), std::move(socket));
                                     })
        ->start();

    do_accept();
    accept_counter_.fetch_sub(1U, std::memory_order_relaxed);
  };
  acceptor_.async_accept(std::move(accept_handler));
}
