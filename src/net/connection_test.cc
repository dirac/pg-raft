#define BOOST_TEST_MODULE connection
#include "src/base/io_threads.h"
#include "src/base/testing.h"
#include "src/net/async_connect.h"
#include "src/net/server.h"

namespace {

struct TestConnection : public Connection {
  TestConnection(string peer, socket_type socket, std::chrono::milliseconds timeout)
      : Connection(peer, std::move(socket), timeout) {}

  TestConnection(string peer, socket_type socket) : Connection(peer, std::move(socket)) {}

  uint8_t status() const final {
    std::unique_lock<std::mutex> lk{mtx};
    status_calls.emplace_back(epoch_msec());
    return 'a';
  }
  void set_peer_status(uint8_t x) final {
    std::unique_lock<std::mutex> lk{mtx};
    peer_status_calls.emplace_back(epoch_msec());
    BOOST_TEST(x == 'a');
  }

  size_t status_calls_size() {
    std::unique_lock<std::mutex> lk{mtx};
    return status_calls.size();
  }
  size_t peer_status_calls_size() {
    std::unique_lock<std::mutex> lk{mtx};
    return peer_status_calls.size();
  }

  mutable std::mutex       mtx;
  mutable vector<unsigned> status_calls;
  mutable vector<unsigned> peer_status_calls;
};

struct TestServer : public Server {
  TestServer(boost::asio::io_context& io_context, boost::asio::ip::tcp::endpoint const& ep)
      : Server(io_context, ep) {}

  void start() final { do_accept(); }

  void on_shutdown() final {}

  void new_connection(string peer, socket_type socket) override {
    conn = Connection::create<TestConnection>(std::move(peer), std::move(socket));
    new_conn.count_down();
  }

  shared_ptr<TestConnection> get() {
    new_conn.wait();
    return conn;
  }

 private:
  std::latch                 new_conn{1};
  shared_ptr<TestConnection> conn{nullptr};
};

struct T {
  std::vector<unsigned> elements;

  bool operator==(T const& that) const { return elements == that.elements; }

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & elements;
  }

  static T instance(unsigned e) {
    T t;
    while (t.elements.size() < e)
      t.elements.emplace_back(rand());
    return t;
  }
};

std::filesystem::path create_file(unsigned size) {
  std::filesystem::path file = tmp_file();
  std::ofstream         outFile(file);

  for (unsigned i = 0; i < size; ++i)
    outFile << (rand() % 100) << "\n";
  outFile.close();
  return file;
}

std::vector<char> read_file(std::filesystem::path const& filename) {
  std::ifstream file(filename, std::ios::binary);

  // Stop the stream from skipping whitespace (optional but safer for binary/raw data)
  file.unsetf(std::ios::skipws);

  // Initialize vector using stream iterators
  auto result =
      std::vector<char>{(std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>()};
  file.close();
  return result;
}

std::vector<char> read_file(Result<std::filesystem::path> const& r) {
  if (!r)
    return {};

  auto result = read_file(r.value());

  std::error_code ec;
  std::filesystem::remove(r.value(), ec);
  if (ec)
    ERROR_CODE_MSG(ec, "Removing {}", r.value().c_str());

  return result;
}

}  // namespace

struct TestSetup {
  TestSetup() {
    auto ep = ::boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0);
    server  = Server::create<TestServer>(io, ep);
  }

  shared_ptr<TestConnection> connect(
      std::chrono::milliseconds timeout = std::chrono::milliseconds(0)) {
    if (auto r = ::connect(io, "test", server->host().with_address("localhost")); r) {
      LOG_INFO("Connected to {}", server->host());
      auto conn = Connection::create<TestConnection>("c->s ", std::move(r.value()), timeout);

      client = server->get();
      return conn;
    } else
      ERROR_CODE_MSG(r.error(), "Connect failed");
    return nullptr;
  }

  ~TestSetup() {
    client->shutdown();
    server->shutdown();

    io.stop();
    th.stop();
  }

  boost::asio::io_context io{};
  IoThreads               th{io, 4};
  TmpDir                  dir{};

  shared_ptr<TestServer>     server;
  shared_ptr<TestConnection> client;
};

BOOST_FIXTURE_TEST_CASE(messages_with_buffer, TestSetup) {
  auto conn = connect();

  BOOST_REQUIRE(conn);
  BOOST_REQUIRE(client);

  auto t1 = T::instance(3 * kPageSize);
  auto t2 = T::instance(5 * kPageSize);

  auto b1 = Bytes{};
  while (b1.size() < (3 * kPageSize + 3))
    b1.emplace_back(static_cast<uint8_t>(rand()));
  auto buf1 = boost::asio::const_buffer(b1.data(), b1.size());

  auto b2 = Bytes{};
  while (b2.size() < (kPageSize + 3))
    b2.emplace_back(static_cast<uint8_t>(rand()));
  auto buf2 = boost::asio::const_buffer(b2.data(), b2.size());

  auto c = std::atomic<unsigned>{0};

  unsigned k;
  unsigned p = 0;
  for (k = 0; k < 2; ++k) {
    std::swap(conn, client);

    p++;
    for (unsigned i = 0; i < 2; ++i) {
      conn->self()->write_data(to_data(t1), buf1, [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });
      conn->self()->write_data(to_data(t2), buf2, [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });

      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      client->self()->read_data([&t1, &c, &b1](Result<Data> r) {
        Bytes bytes;
        BOOST_TEST((t1 == from_data<T>(std::move(r.value()), bytes).value()));
        BOOST_TEST(bytes == b1);
        c.fetch_add(1);
      });
      client->self()->read_data([&t2, &c, &b2](Result<Data> r) {
        Bytes bytes;
        BOOST_TEST((t2 == from_data<T>(std::move(r.value()), bytes).value()));
        BOOST_TEST(bytes == b2);
        c.fetch_add(1);
      });
    }
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  while (c != (p * 4 * 2)) {
    LOG_INFO("waiting for {} acks, got {}", (p * 4 * 2), c.load());
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
  conn->shutdown();
}

BOOST_FIXTURE_TEST_CASE(messages, TestSetup) {
  auto conn = connect();

  BOOST_REQUIRE(conn);
  BOOST_REQUIRE(client);

  auto t1 = T::instance(3 * kPageSize);
  auto t2 = T::instance(5 * kPageSize);

  auto c = std::atomic<unsigned>{0};

  unsigned k;
  unsigned p = 0;
  for (k = 0; k < 2; ++k) {
    std::swap(conn, client);

    p++;
    for (unsigned i = 0; i < 10; ++i) {
      conn->self()->write_data(to_data(t1), [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });
      conn->self()->write_data(to_data(t2), [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });

      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      client->self()->read_data([&t1, &c](Result<Data> r) {
        BOOST_TEST((t1 == from_data<T>(std::move(r.value())).value()));
        c.fetch_add(1);
      });
      client->self()->read_data([&t2, &c](Result<Data> r) {
        BOOST_TEST((t2 == from_data<T>(std::move(r.value())).value()));
        c.fetch_add(1);
      });
    }

    p++;
    for (unsigned i = 0; i < 10; ++i) {
      client->self()->read_data([&t1, &c](Result<Data> r) {
        BOOST_TEST((t1 == from_data<T>(std::move(r.value())).value()));
        c.fetch_add(1);
      });
      client->self()->read_data([&t2, &c](Result<Data> r) {
        BOOST_TEST((t2 == from_data<T>(std::move(r.value())).value()));
        c.fetch_add(1);
      });

      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      conn->self()->write_data(to_data(t1), [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });
      conn->self()->write_data(to_data(t2), [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });
    }

    p++;
    for (unsigned i = 0; i < 10; ++i) {
      client->self()->read_data([&t1, &c](Result<Data> r) {
        BOOST_TEST((t1 == from_data<T>(std::move(r.value())).value()));
        c.fetch_add(1);
      });
      client->self()->read_data([&t2, &c](Result<Data> r) {
        BOOST_TEST((t2 == from_data<T>(std::move(r.value())).value()));
        c.fetch_add(1);
      });

      conn->self()->write_data(to_data(t1), [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });
      conn->self()->write_data(to_data(t2), [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });
    }

    p++;
    for (unsigned i = 0; i < 10; ++i) {
      client->self()->read_data([&t1, &c](Result<Data> r) {
        BOOST_TEST((t1 == from_data<T>(std::move(r.value())).value()));
        c.fetch_add(1);
      });
      conn->self()->read_data([&t2, &c](Result<Data> r) {
        BOOST_TEST((t2 == from_data<T>(std::move(r.value())).value()));
        c.fetch_add(1);
      });

      conn->self()->write_data(to_data(t1), [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });
      client->self()->write_data(to_data(t2), [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });
    }
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  while (c != (p * 4 * 10)) {
    LOG_INFO("waiting for {} acks, got {}", (p * 4 * 10), c.load());
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
  conn->shutdown();
}

BOOST_FIXTURE_TEST_CASE(status, TestSetup) {
  auto conn = connect();

  BOOST_TEST(conn);
  BOOST_TEST(client);

  conn->request_peer_status();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  BOOST_TEST(conn->status_calls_size() == 0);
  BOOST_TEST(conn->peer_status_calls_size() == 1);

  BOOST_TEST(client->status_calls_size() == 1);
  BOOST_TEST(client->peer_status_calls_size() == 0);

  conn->shutdown();
}

BOOST_FIXTURE_TEST_CASE(bg_status, TestSetup) {
  auto conn = connect(std::chrono::milliseconds(1000));

  BOOST_TEST(conn);
  BOOST_TEST(client);

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  // We send a status request every timeout_ / 10.
  BOOST_TEST(conn->status_calls_size() == 0);
  BOOST_TEST(conn->peer_status_calls_size() > 5);

  BOOST_TEST(client->status_calls_size() > 5);
  BOOST_TEST(client->peer_status_calls_size() == 0);

  conn->shutdown();
}

BOOST_FIXTURE_TEST_CASE(files, TestSetup) {
  auto conn = connect();

  BOOST_TEST(conn);
  BOOST_TEST(client);

  auto t1 = create_file(3 * kPageSize);
  auto t2 = create_file(5 * kPageSize);

  BOOST_TEST(read_file(t1) != read_file(t2));
  BOOST_TEST(read_file(t1).size() >= 3 * kPageSize);
  BOOST_TEST(read_file(t2).size() >= 5 * kPageSize);

  auto c = std::atomic<unsigned>{0};

  unsigned k;
  unsigned p = 0;
  for (k = 0; k < 2; ++k) {
    std::swap(conn, client);

    p++;
    for (unsigned i = 0; i < 10; ++i) {
      conn->write_file(t1, [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });
      conn->write_file(t2, [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });

      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      client->read_file([&t1, &c](Result<std::filesystem::path> r) {
        BOOST_TEST((read_file(t1) == read_file(r.value())));
        c.fetch_add(1);
      });
      client->read_file([&t2, &c](Result<std::filesystem::path> r) {
        BOOST_TEST((read_file(t2) == read_file(r)));
        c.fetch_add(1);
      });
    }

    p++;
    for (unsigned i = 0; i < 10; ++i) {
      client->read_file([&t1, &c](Result<std::filesystem::path> r) {
        BOOST_TEST((read_file(t1) == read_file(r)));
        c.fetch_add(1);
      });
      client->read_file([&t2, &c](Result<std::filesystem::path> r) {
        BOOST_TEST((read_file(t2) == read_file(r)));
        c.fetch_add(1);
      });

      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      conn->write_file(t1, [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });
      conn->write_file(t2, [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });
    }

    p++;
    for (unsigned i = 0; i < 10; ++i) {
      conn->write_file(t1, [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });
      conn->write_file(t2, [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });

      client->read_file([&t1, &c](Result<std::filesystem::path> r) {
        BOOST_TEST((read_file(t1) == read_file(r)));
        c.fetch_add(1);
      });
      client->read_file([&t2, &c](Result<std::filesystem::path> r) {
        BOOST_TEST((read_file(t2) == read_file(r)));
        c.fetch_add(1);
      });
    }

    p++;
    for (unsigned i = 0; i < 10; ++i) {
      client->read_file([&t1, &c](Result<std::filesystem::path> r) {
        BOOST_TEST((read_file(t1) == read_file(r)));
        c.fetch_add(1);
      });
      client->read_file([&t2, &c](Result<std::filesystem::path> r) {
        BOOST_TEST((read_file(t2) == read_file(r.value())));
        c.fetch_add(1);
      });

      conn->write_file(t1, [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });
      conn->write_file(t2, [&c](Result<void> r) {
        BOOST_TEST(!r.has_error());
        c.fetch_add(1);
      });
    }
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  while (c != (p * 4 * 10)) {
    LOG_INFO("waiting for {} acks, got {}", (p * 4 * 10), c.load());
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
  conn->shutdown();
}
