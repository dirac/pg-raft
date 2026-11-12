#pragma once

#include "src/base/base.h"

// Socket wrappers that serialize the calls on the socket with a mutex (reads and writes are
// initiated from different threads) and count the reads, which the connection uses to detect a
// silent peer.

struct tcp_socket : private boost::asio::ip::tcp::socket {  // NOLINT(readability-identifier-naming)
 public:
  using boost::asio::ip::tcp::socket::executor_type;
  using lowest_layer_type = tcp_socket;

  tcp_socket(tcp_socket&& that) : boost::asio::ip::tcp::socket(std::move(that)) {}

  tcp_socket& operator=(tcp_socket&& that) {
    static_cast<boost::asio::ip::tcp::socket&>(*this) = std::move(that);
    return *this;
  }

  tcp_socket(boost::asio::ip::tcp::socket&& s) : boost::asio::ip::tcp::socket(std::move(s)) {}

  auto get_executor() -> decltype(std::declval<boost::asio::ip::tcp::socket&>().get_executor()) {
    auto lk = lock();
    return boost::asio::ip::tcp::socket::get_executor();
  }

  auto cancel(boost::system::error_code& ec)
      -> decltype(std::declval<boost::asio::ip::tcp::socket&>().cancel(ec)) {
    auto lk = lock();
    return boost::asio::ip::tcp::socket::cancel(ec);
  }

  auto close(boost::system::error_code& ec)
      -> decltype(std::declval<boost::asio::ip::tcp::socket&>().close(ec)) {
    auto lk = lock();
    return boost::asio::ip::tcp::socket::close(ec);
  }

  auto shutdown(boost::asio::socket_base::shutdown_type what, boost::system::error_code& ec)
      -> decltype(std::declval<boost::asio::ip::tcp::socket&>().shutdown(what, ec)) {
    auto lk = lock();
    return boost::asio::ip::tcp::socket::shutdown(what, ec);
  }

  template <typename MutableBufferSequence, typename ReadToken>
  auto async_read_some(MutableBufferSequence const& buffers, ReadToken&& token)
      -> decltype(std::declval<boost::asio::ip::tcp::socket&>()
                      .async_read_some(buffers, std::forward<ReadToken>(token))) {
    auto lk = lock();
    counter_.fetch_add(1U, std::memory_order_relaxed);
    return boost::asio::ip::tcp::socket::async_read_some(buffers, std::forward<ReadToken>(token));
  }

  template <typename ConstBufferSequence, typename WriteToken>
  auto async_write_some(ConstBufferSequence const& buffers, WriteToken&& token)
      -> decltype(std::declval<boost::asio::ip::tcp::socket&>()
                      .async_write_some(buffers, std::forward<WriteToken>(token))) {
    auto lk = lock();
    return boost::asio::ip::tcp::socket::async_write_some(buffers, std::forward<WriteToken>(token));
  }

  lowest_layer_type&       lowest_layer() { return *this; }
  lowest_layer_type const& lowest_layer() const { return *this; }

  auto counter() const { return counter_.load(std::memory_order_relaxed); }

 private:
  std::unique_lock<std::mutex> lock() const { return std::unique_lock{mtx_}; }

  mutable std::mutex                     mtx_;
  mutable std::atomic_unsigned_lock_free counter_;
};

struct ssl_socket  // NOLINT(readability-identifier-naming)
    : private boost::asio::ssl::stream<tcp_socket> {
 public:
  using boost::asio::ssl::stream<tcp_socket>::executor_type;
  using boost::asio::ssl::stream<tcp_socket>::next_layer_type;
  using boost::asio::ssl::stream<tcp_socket>::lowest_layer_type;

  static_assert(std::is_same_v<next_layer_type, tcp_socket>);

  ssl_socket(ssl_socket&& that) : boost::asio::ssl::stream<tcp_socket>(std::move(that)) {}

  ssl_socket& operator=(ssl_socket&& that) {
    static_cast<boost::asio::ssl::stream<tcp_socket>&>(*this) = std::move(that);
    return *this;
  }

  ssl_socket(boost::asio::ssl::stream<tcp_socket> s)
      : boost::asio::ssl::stream<tcp_socket>(std::move(s)) {}

  auto get_executor()
      -> decltype(std::declval<boost::asio::ssl::stream<tcp_socket>&>().get_executor()) {
    auto lk = lock();
    return boost::asio::ssl::stream<tcp_socket>::get_executor();
  }

  auto counter() const { return next_layer().counter(); }

  next_layer_type&       next_layer() { return boost::asio::ssl::stream<tcp_socket>::next_layer(); }
  next_layer_type const& next_layer() const {
    return boost::asio::ssl::stream<tcp_socket>::next_layer();
  }

  lowest_layer_type& lowest_layer() { return boost::asio::ssl::stream<tcp_socket>::next_layer(); }
  lowest_layer_type const& lowest_layer() const {
    return boost::asio::ssl::stream<tcp_socket>::lowest_layer();
  }

  template <typename VerifyCallback>
  auto set_verify_callback(VerifyCallback const& callback)
      -> decltype(std::declval<boost::asio::ssl::stream<tcp_socket>&>().set_verify_callback(
          callback)) {
    auto lk = lock();
    return boost::asio::ssl::stream<tcp_socket>::set_verify_callback(callback);
  }

  template <typename HandshakeToken>
  auto async_handshake(boost::asio::ssl::stream_base::handshake_type type, HandshakeToken&& token)
      -> decltype(std::declval<boost::asio::ssl::stream<tcp_socket>&>()
                      .async_handshake(type, std::forward<HandshakeToken>(token))) {
    auto lk = lock();
    return boost::asio::ssl::stream<tcp_socket>::async_handshake(
        type, std::forward<HandshakeToken>(token));
  }

  template <typename ShutdownToken>
  auto async_shutdown(ShutdownToken&& token)
      -> decltype(std::declval<boost::asio::ssl::stream<tcp_socket>&>().async_shutdown(
          std::forward<ShutdownToken>(token))) {
    auto lk = lock();
    return boost::asio::ssl::stream<tcp_socket>::async_shutdown(std::forward<ShutdownToken>(token));
  }

  template <typename MutableBufferSequence, typename ReadToken>
  auto async_read_some(MutableBufferSequence const& buffers, ReadToken&& token)
      -> decltype(std::declval<boost::asio::ssl::stream<tcp_socket>&>()
                      .async_read_some(buffers, std::forward<ReadToken>(token))) {
    auto lk = lock();
    return boost::asio::ssl::stream<tcp_socket>::async_read_some(buffers,
                                                                 std::forward<ReadToken>(token));
  }

  template <typename ConstBufferSequence, typename WriteToken>
  auto async_write_some(ConstBufferSequence const& buffers, WriteToken&& token)
      -> decltype(std::declval<boost::asio::ssl::stream<tcp_socket>&>()
                      .async_write_some(buffers, std::forward<WriteToken>(token))) {
    auto lk = lock();
    return boost::asio::ssl::stream<tcp_socket>::async_write_some(buffers,
                                                                  std::forward<WriteToken>(token));
  }

 private:
  std::unique_lock<std::mutex> lock() const { return std::unique_lock{mtx_}; }

  mutable std::mutex mtx_;
};

/// The ssl_socket alternative is kept so that TLS can be enabled later; nothing creates one yet.
using socket_type = std::variant<tcp_socket, ssl_socket>;
