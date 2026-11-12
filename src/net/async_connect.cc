#include "src/net/async_connect.h"

#include "src/base/logger.h"

// Ported from sparrowdb lib/net/async_connect.cpp, without TLS: if the server asks for TLS (its
// first byte is not 0) the connect fails with ENOTSUP.

Result<socket_type> connect(boost::asio::io_context& io_context,
                            string const&            self,
                            HostAddress const&       host) {
  auto promise = std::promise<Result<socket_type>>{};
  auto future  = promise.get_future();

  async_connect(io_context, self, host, [&promise](Result<socket_type> result) {
    promise.set_value(std::move(result));
  });

  return future.get();
}

namespace {
using Handler = std::function<void(Result<socket_type>)>;

using RawSocket = boost::asio::ip::tcp::socket;

struct SendInfoHandler {
  HostAddress                       host;
  shared_ptr<std::array<char, 64U>> self;
  shared_ptr<RawSocket>             socket;
  Handler                           handler;

  void operator()(boost::system::error_code const& ec, size_t) {
    if (ec)
      return handler(elog(ec, "Handshake ack error with host {}:{}", host.address, host.port));

    handler(tcp_socket(std::move(*socket)));
  }
};

struct SSLByteHandler {
  HostAddress                       host;
  shared_ptr<std::array<char, 64U>> self;
  shared_ptr<uint8_t>               ssl_byte;
  shared_ptr<RawSocket>             socket;
  Handler                           handler;

  void operator()(boost::system::error_code const& ec, size_t) {
    if (ec)
      return handler(elog(ec, "SSL byte error with host {}:{}", host.address, host.port));

    if (*ssl_byte != 0)
      return handler(elog(system_error_code(ENOTSUP),
                          "Host {}:{} requires TLS, which is not supported",
                          host.address,
                          host.port));

    boost::asio::async_write(*socket,
                             boost::asio::const_buffer(self->data(), self->size()),
                             SendInfoHandler{
                                 .host    = host,
                                 .self    = self,
                                 .socket  = socket,
                                 .handler = std::move(handler),
                             });
  }
};

struct ConnectHandler {
  shared_ptr<vector<boost::asio::ip::tcp::endpoint>> endpoints;
  HostAddress                                        host;
  shared_ptr<std::array<char, 64U>>                  self;
  shared_ptr<uint8_t>                                ssl_byte;
  shared_ptr<RawSocket>                              socket;
  Handler                                            handler;

  void operator()(boost::system::error_code const& ec,
                  ::boost::asio::ip::tcp::endpoint const& /*ep*/) {
    if (ec)
      return handler(elog(ec, "Connect error to host {}:{}", host.address, host.port));

    boost::asio::async_read(*socket,
                            boost::asio::mutable_buffer(ssl_byte.get(), 1),
                            SSLByteHandler{
                                .host     = host,
                                .self     = self,
                                .ssl_byte = ssl_byte,
                                .socket   = socket,
                                .handler  = std::move(handler),
                            });
  }
};

struct ResolveHandler {
  HostAddress                                host;
  shared_ptr<std::array<char, 64U>>          self;
  shared_ptr<RawSocket>                      socket;
  shared_ptr<boost::asio::ip::tcp::resolver> resolver;
  Handler                                    handler;

  void operator()(boost::system::error_code const&                    ec,
                  boost::asio::ip::tcp::resolver::results_type const& endpts) {
    if (ec)
      return handler(elog(ec, "Resolve error to host {}:{}", host.address, host.port));

    auto endpoints = make_shared<vector<boost::asio::ip::tcp::endpoint>>();

    for (auto const& e : endpts)
      endpoints->emplace_back(e.endpoint().address(), host.port);

    boost::asio::async_connect(*socket,
                               *endpoints,
                               ConnectHandler{
                                   .endpoints = endpoints,
                                   .host      = host,
                                   .self      = self,
                                   .ssl_byte  = make_shared<uint8_t>(),
                                   .socket    = socket,
                                   .handler   = std::move(handler),
                               });
  }
};

}  // namespace

void async_connect(boost::asio::io_context&                 io_context,
                   string const&                            self,
                   HostAddress const&                       host,
                   std::function<void(Result<socket_type>)> handler) {
  auto resolver = make_shared<boost::asio::ip::tcp::resolver>(io_context);
  auto socket   = make_shared<RawSocket>(io_context);
  auto self_ptr = make_shared<std::array<char, 64U>>();

  std::strncpy(self_ptr->data(), self.c_str(), std::min(self.size(), self_ptr->size()));
  self_ptr->back() = '\0';

  auto ec      = ::boost::system::error_code{};
  auto address = ::boost::asio::ip::make_address(host.address, ec);

  if (ec) {
    resolver->async_resolve(host.address,
                            "",
                            ResolveHandler{
                                .host     = host,
                                .self     = self_ptr,
                                .socket   = socket,
                                .resolver = resolver,
                                .handler  = std::move(handler),
                            });
  } else {
    auto endpoints = make_shared<vector<boost::asio::ip::tcp::endpoint>>();
    endpoints->emplace_back(address, host.port);

    ::boost::asio::async_connect(*socket,
                                 *endpoints,
                                 ConnectHandler{
                                     .endpoints = endpoints,
                                     .host      = host,
                                     .self      = self_ptr,
                                     .ssl_byte  = make_shared<uint8_t>(),
                                     .socket    = socket,
                                     .handler   = std::move(handler),
                                 });
  }
}
