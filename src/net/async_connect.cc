#include "src/net/async_connect.h"

#include "src/base/logger.h"

Result<socket_type> connect(boost::asio::io_context& io_context,
                            string const&            self,
                            HostAddress              host) {
  auto promise = std::promise<Result<socket_type>>{};
  auto future  = promise.get_future();

  async_connect(io_context, self, std::move(host), [&promise](Result<socket_type> result) {
    promise.set_value(std::move(result));
  });

  return future.get();
}

namespace {
using Handler = std::function<void(Result<socket_type>)>;

using tcp_socket = boost::asio::ip::tcp::socket;
using ssl_socket = boost::asio::ssl::stream<boost::asio::ip::tcp::socket>;

template <typename SocketType>
struct SendInfoHandler {
  HostAddress                       host;
  shared_ptr<std::array<char, 64U>> self;
  shared_ptr<SocketType>            socket;
  Handler                           handler;

  void operator()(boost::system::error_code const& ec, size_t) {
    if (ec)
      return handler(
          ERROR_CODE_MSG(ec, "Handshake ack error with host {}:{}", host.address, host.port));
    handler(std::move(*socket));
  }
};

struct ConnectHandler {
  shared_ptr<vector<boost::asio::ip::tcp::endpoint>> endpoints;
  HostAddress                                        host;
  shared_ptr<std::array<char, 64U>>                  self;
  shared_ptr<boost::asio::ip::tcp::socket>           socket;
  Handler                                            handler;

  void operator()(boost::system::error_code const& ec, ::boost::asio::ip::tcp::endpoint const& ep) {
    if (ec)
      return handler(
          ERROR_CODE_MSG(ec, "Failed connecting to {}:{}", ep.address().to_string(), ep.port()));

    LOG_INFO("Connected to {}:{}", ep.address().to_string(), ep.port());
    boost::asio::async_write(*socket,
                             boost::asio::const_buffer(self->data(), self->size()),
                             SendInfoHandler<tcp_socket>{
                                 .host    = host,
                                 .self    = self,
                                 .socket  = socket,
                                 .handler = std::move(handler),
                             });
  }
};

struct ResolveHandler {
  HostAddress                                host;
  shared_ptr<std::array<char, 64U>>          self;
  shared_ptr<boost::asio::ip::tcp::socket>   socket;
  shared_ptr<boost::asio::ip::tcp::resolver> resolver;
  Handler                                    handler;

  void operator()(boost::system::error_code const&             ec,
                  boost::asio::ip::tcp::resolver::results_type endpts) {
    if (ec)
      return handler(ERROR_CODE_MSG(ec, "Resolve error to host {0}:{1}", host.address, host.port));

    auto endpoints = make_shared<vector<boost::asio::ip::tcp::endpoint>>();

    for (auto& e : endpts) {
      endpoints->emplace_back(e.endpoint().address(), host.port);
      LOG_INFO("Resolved to endpoint {}:{}", endpoints->back().address().to_string(), host.port);
    }

    boost::asio::async_connect(*socket,
                               *endpoints,
                               ConnectHandler{.endpoints = endpoints,
                                              .host      = host,
                                              .self      = self,
                                              .socket    = socket,
                                              .handler   = std::move(handler)});
  }
};

}  // namespace

void async_connect(boost::asio::io_context&                 io_context,
                   string const&                            self,
                   HostAddress                              host,
                   std::function<void(Result<socket_type>)> handler) {
  auto resolver = make_shared<boost::asio::ip::tcp::resolver>(io_context);
  auto socket   = make_shared<boost::asio::ip::tcp::socket>(io_context);
  auto self_ptr = make_shared<std::array<char, 64U>>();

  std::strncpy(self_ptr->data(), self.c_str(), std::max(self.size(), self_ptr->size()));
  self_ptr->back() = '\0';

  LOG_INFO("Connecting to {}", host);

  auto ec      = ::boost::system::error_code{};
  auto address = ::boost::asio::ip::make_address(host.address, ec);

  if (ec)
    resolver->async_resolve(host.address,
                            "",
                            ResolveHandler{
                                .host     = host,
                                .self     = self_ptr,
                                .socket   = socket,
                                .resolver = resolver,
                                .handler  = std::move(handler),
                            });
  else {
    auto endpoints = make_shared<vector<boost::asio::ip::tcp::endpoint>>();
    endpoints->emplace_back(address, host.port);

    ::boost::asio::async_connect(*socket,
                                 *endpoints,
                                 ConnectHandler{.endpoints = endpoints,
                                                .host      = host,
                                                .self      = self_ptr,
                                                .socket    = socket,
                                                .handler   = std::move(handler)});
  }
}
