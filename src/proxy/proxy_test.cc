#define BOOST_TEST_MODULE proxy
#include "src/proxy/proxy.h"

#include "src/raft/raft_ensemble_test_common.h"

// The proxy against an in-process raft ensemble (the leader learned from raft's client channel)
// and fake "PostgreSQL" servers that answer every read with "node<i>:" and the bytes read, so a
// client can tell which server it reached.

using tcp = boost::asio::ip::tcp;

namespace {

constexpr uint16_t kProxyPort = 15640;  // + 1 + i for the servers

/// An echo server per node, on an ephemeral port.
struct EchoServer {
  EchoServer(boost::asio::io_context& io, unsigned node)
      : acceptor_(io, tcp::endpoint(tcp::v4(), 0)), node_(node) {
    accept();
  }
  uint16_t port() const { return acceptor_.local_endpoint().port(); }
  void     close() {
    boost::system::error_code ec;
    acceptor_.close(ec);
  }

 private:
  void accept() {
    acceptor_.async_accept([this](boost::system::error_code ec, tcp::socket s) {
      if (ec)
        return;
      serve(make_shared<tcp::socket>(std::move(s)));
      accept();
    });
  }
  void serve(shared_ptr<tcp::socket> s) {
    auto buf = make_shared<std::array<char, 1024>>();
    s->async_read_some(
        boost::asio::buffer(*buf), [this, s, buf](boost::system::error_code ec, size_t n) {
          if (ec)
            return;
          auto reply = make_shared<string>(std::format("node{}:", node_) + string(buf->data(), n));
          boost::asio::async_write(*s,
                                   boost::asio::buffer(*reply),
                                   [this, s, reply](boost::system::error_code e, size_t) {
                                     if (!e)
                                       serve(s);
                                   });
        });
  }

  tcp::acceptor  acceptor_;
  unsigned const node_;
};

/// A synchronous client: connect, send, read the reply ("" if the connection is closed).
struct Client {
  explicit Client(uint16_t port) {
    socket.connect(tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), port));
  }
  string ask(string const& what) {
    boost::system::error_code ec;
    boost::asio::write(socket, boost::asio::buffer(what), ec);
    if (ec)
      return "";
    std::array<char, 1024> buf{};
    auto                   n = socket.read_some(boost::asio::buffer(buf), ec);
    return ec ? "" : string(buf.data(), n);
  }
  boost::asio::io_context io;
  tcp::socket             socket{io};
};

bool wait_for(std::chrono::seconds timeout, std::function<bool()> const& pred) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred())
      return true;
    std::this_thread::sleep_for(100ms);
  }
  return pred();
}

}  // namespace

BOOST_AUTO_TEST_CASE(routing_and_failover, *boost::unit_test::timeout(timeout)) {
  auto io = boost::asio::io_context{};
  auto th = IoThreads{io, 8};

  auto dir     = TmpDir{};
  auto cfg     = Config{};
  auto service = RaftTestService{io, dir(), cfg, options, 3};  // sets cfg.servers' ha_port

  vector<unique_ptr<EchoServer>> pg;
  for (unsigned i = 0; i < 3; ++i) {
    pg.push_back(make_unique<EchoServer>(io, i));
    cfg.servers.at(i).pg_port = pg.back()->port();
  }
  cfg.proxy_pg_port = kProxyPort;

  service.start_all();
  service.wait_for_leader();

  auto opts            = ProxyOptions{};
  opts.failover_window = 3s;
  opts.listen_address  = "127.0.0.1";
  auto proxy           = Proxy::create(io, cfg, opts);
  BOOST_REQUIRE(proxy->start().has_value());

  // 1. The per-server ports reach their server, whatever its role.
  for (unsigned i = 0; i < 3; ++i) {
    Client c(static_cast<uint16_t>(kProxyPort + 1 + i));
    BOOST_TEST(c.ask("x") == std::format("node{}:x", i));
  }

  // 3 (first part). No node serves yet: a primary-port client is held ...
  auto leader = service.leader().value();
  BOOST_TEST(!proxy->primary().has_value());
  string      held;
  std::thread holder([&] {
    Client c(kProxyPort);
    held = c.ask("held");
  });
  std::this_thread::sleep_for(500ms);

  // ... and connected when the leader starts serving (its PostgreSQL promoted).
  service.server(leader)->set_serving(true);
  BOOST_REQUIRE(wait_for(10s, [&] { return proxy->primary() == std::optional<unsigned>{leader}; }));
  holder.join();
  BOOST_TEST(held == std::format("node{}:held", leader));

  // 2. The primary port reaches the serving leader.
  Client on_primary(kProxyPort);
  BOOST_TEST(on_primary.ask("a") == std::format("node{}:a", leader));
  Client on_server(static_cast<uint16_t>(kProxyPort + 1 + leader));
  BOOST_TEST(on_server.ask("b") == std::format("node{}:b", leader));

  // 4. A hand-over: the old primary's primary-port clients are cut; new ones reach the new
  //    primary; the per-server port's client is untouched.
  BOOST_TEST(service.stepdown());
  BOOST_REQUIRE(wait_for(20s, [&] {
    auto l = service.leader();
    return l && *l != leader;
  }));
  auto const next = service.leader().value();
  service.server(next)->set_serving(true);
  BOOST_REQUIRE(wait_for(10s, [&] { return proxy->primary() == std::optional<unsigned>{next}; }));

  BOOST_TEST(on_primary.ask("c") == "");  // closed
  BOOST_TEST(on_server.ask("d") == std::format("node{}:d", leader));
  Client again(kProxyPort);
  BOOST_TEST(again.ask("e") == std::format("node{}:e", next));

  // 5. No primary for longer than the failover window: the client is closed.
  service.server(next)->set_serving(false);
  BOOST_REQUIRE(wait_for(10s, [&] { return !proxy->primary().has_value(); }));
  auto const start = std::chrono::steady_clock::now();
  Client     late(kProxyPort);
  BOOST_TEST(late.ask("f") == "");
  auto const held_for = std::chrono::steady_clock::now() - start;
  BOOST_TEST((held_for >= 2500ms));

  proxy->stop();
  for (auto& p : pg)
    p->close();
  service.shutdown();
  io.stop();
}
