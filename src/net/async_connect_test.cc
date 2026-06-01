#define BOOST_TEST_MODULE async_connect
#include "src/net/async_connect.h"

#include "src/base/logger.h"
#include "src/base/testing.h"
#include "src/net/server.h"

namespace {

struct TestConnection : public Connection {
  TestConnection(string peer, socket_type socket)
      : Connection(std::move(peer), std::move(socket)) {}

  void start() override {}

  ~TestConnection() override = default;
};

struct TestServer : public Server {
  TestServer(boost::asio::io_context& io_context, boost::asio::ip::tcp::endpoint const& ep)
      : Server(io_context, ep) {}

  void start() final { do_accept(); }

  void on_shutdown() final {}

  void new_connection(string peer, socket_type socket) override {
    Connection::create<TestConnection>(std::move(peer), std::move(socket));
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

  LOG_INFO("Server {}:{}", server->host().address, server->host().port);

  auto socket = connect(io_context, "test", server->host());

  BOOST_TEST(socket.has_value());
  BOOST_TEST(std::holds_alternative<tcp_socket>(socket.value()));

  std::this_thread::sleep_for(std::chrono::seconds(5));

  work.reset();
  io_context.stop();

  th.join();
}
