#define BOOST_TEST_MODULE async_connect
#include "src/net/async_connect.h"

#include "src/base/testing.h"
#include "src/net/server.h"

namespace banyandb::net::test {

namespace {

Logger test_logger{"ssl_test"};

struct TestConnection : public Connection<> {
  TestConnection(char const* role, string peer, std::filesystem::path tmpdir, socket_type socket)
      : Connection<>(role, std::move(peer), std::move(socket), std::move(tmpdir), test_logger) {}

  void start() override {}

  ~TestConnection() override = default;
};

struct TestServer : public Server {
  TestServer(boost::asio::io_context&              io_context,
             Certificates const&                   certs,
             std::filesystem::path                 tmpdir,
             bool                                  enable_ssl,
             boost::asio::ip::tcp::endpoint const& ep)
      : Server(io_context, ep, certs, tmpdir, enable_ssl) {}

  void start() final { do_accept(); }

  void on_shutdown() final {}

  void new_connection(char const*           role,
                      string                peer,
                      std::filesystem::path tmpdir,
                      socket_type           socket) override {
    Connection<>::create<TestConnection>(
        role, std::move(peer), std::move(tmpdir), std::move(socket));
  }
};

}  // namespace

BOOST_AUTO_TEST_CASE(non_ssl) {
  TmpDir tmpdir;

  auto io_context = ::boost::asio::io_context{};

  auto work = ::boost::asio::executor_work_guard<decltype(io_context.get_executor())>(
      io_context.get_executor());

  auto th = std::thread([&io_context]() { io_context.run(); });

  auto ep = ::boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0);

  auto server = Server::create<TestServer>(io_context, ep);

  info("{}", server->host());

  auto socket = connect(io_context, ssl_ctx, server->host());

  BOOST_TEST(socket.has_value());
  BOOST_TEST(std::holds_alternative<tcp_socket>(socket.value()));

  std::this_thread::sleep_for(std::chrono::seconds(5));

  work.reset();
  io_context.stop();

  th.join();
}

}  // namespace banyandb::net::test
