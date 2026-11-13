#define BOOST_TEST_MODULE proxy
#include "src/proxy/proxy.h"

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

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

/// GET (or another method) on the admin interface: the status code and the body.
std::tuple<unsigned, string> http(uint16_t                 port,
                                  string const&            target,
                                  boost::beast::http::verb verb = boost::beast::http::verb::get) {
  namespace http = boost::beast::http;
  boost::asio::io_context  io;
  boost::beast::tcp_stream stream(io);
  stream.connect(tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), port));
  http::request<http::string_body> req{verb, target, 11};
  req.set(http::field::host, "localhost");
  http::write(stream, req);
  boost::beast::flat_buffer         buffer;
  http::response<http::string_body> res;
  http::read(stream, buffer, res);
  boost::beast::error_code ec;
  stream.socket().shutdown(tcp::socket::shutdown_both, ec);
  return {res.result_int(), res.body()};
}

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
  cfg.proxy_pg_port   = kProxyPort;
  cfg.proxy_http_port = 0;  // the admin interface on an ephemeral port

  service.start_all();
  service.wait_for_leader();

  auto opts            = ProxyOptions{};
  opts.failover_window = 3s;
  opts.listen_address  = "127.0.0.1";
  auto proxy           = Proxy::create(io, cfg, opts);
  BOOST_REQUIRE(proxy->start().has_value());

  auto const admin = proxy->admin_port();
  BOOST_REQUIRE(admin != 0);

  // The admin interface: up, and no primary yet.
  BOOST_TEST((http(admin, "/health") == std::tuple<unsigned, string>{200, "ok\n"}));
  {
    auto [code, body] = http(admin, "/primary");
    BOOST_TEST(code == 503U);
    BOOST_TEST(boost::json::parse(body).at("server").is_null());
  }
  BOOST_TEST(std::get<0>(http(admin, "/nope")) == 404U);
  {
    auto [code, page] = http(admin, "/primary.html");
    BOOST_TEST(code == 503U);
    BOOST_TEST(page.find("No primary") != string::npos);
  }
  BOOST_TEST(std::get<0>(http(admin, "/status", boost::beast::http::verb::post)) == 405U);

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

  // The admin interface sees the primary, every node, and the open bridges.
  {
    auto [code, body] = http(admin, "/primary");
    BOOST_TEST(code == 200U);
    auto p = boost::json::parse(body).as_object();
    BOOST_TEST(p.at("server").as_int64() == leader);
    BOOST_TEST(p.at("pg_port").as_int64() == cfg.servers.at(leader).pg_port);
  }
  {
    auto [code, body] = http(admin, "/status");
    BOOST_TEST(code == 200U);
    auto st = boost::json::parse(body).as_object();
    BOOST_TEST(st.at("primary").as_int64() == leader);
    auto const& servers = st.at("servers").as_array();
    BOOST_REQUIRE(servers.size() == 3U);
    for (unsigned i = 0; i < 3; ++i) {
      auto const& s = servers.at(i).as_object();
      BOOST_TEST(s.at("connected").as_bool(), i);
      BOOST_TEST(s.at("serving").as_bool() == (i == leader), i);
      BOOST_TEST(s.at("leader").as_bool() == (i == leader), i);
      BOOST_TEST(s.contains("applied_idx"), i);
    }
    auto const& bridges = st.at("bridges").as_object();
    BOOST_TEST(bridges.at("primary").as_int64() >= 1);
    BOOST_TEST(bridges.at("servers").as_array().at(leader).as_int64() >= 1);
  }

  // The extension picks the format: .json is the same JSON, .html a page.
  {
    auto [code, body] = http(admin, "/status.json");
    BOOST_TEST(code == 200U);
    BOOST_TEST(boost::json::parse(body).as_object().at("primary").as_int64() == leader);

    auto [pcode, pbody] = http(admin, "/primary.json");
    BOOST_TEST(pcode == 200U);
    BOOST_TEST(boost::json::parse(pbody).as_object().at("server").as_int64() == leader);

    auto [hcode, page] = http(admin, "/status.html");
    BOOST_TEST(hcode == 200U);
    BOOST_TEST(page.starts_with("<!doctype html>"));
    BOOST_TEST(page.find(std::format("primary: server {}.", leader)) != string::npos);
    BOOST_TEST(page.find("<th>Serving</th>") != string::npos);

    auto [phcode, ppage] = http(admin, "/primary.html");
    BOOST_TEST(phcode == 200U);
    BOOST_TEST(ppage.find(std::format(
                   "Server {} &mdash; 127.0.0.1:{}", leader, cfg.servers.at(leader).pg_port)) !=
               string::npos);
    BOOST_TEST(std::get<0>(http(admin, "/nope.html")) == 404U);
  }

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
