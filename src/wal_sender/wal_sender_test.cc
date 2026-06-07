#include "src/wal_sender/wal_sender.h"
#include "src/wal_sender/wal_sender_server.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <thread>

#define BOOST_TEST_MODULE wal_sender
#include <boost/test/included/unit_test.hpp>

// ── mini standby helper ───────────────────────────────────────────────────────
//
// Speaks just enough of the standby side of the replication protocol to let
// the tests exercise WalSender without a real PostgreSQL process.

struct MiniStandby {
  int fd;

  bool read_raw(void* buf, size_t n) {
    auto* p = static_cast<uint8_t*>(buf);
    while (n > 0) {
      ssize_t r = recv(fd, p, n, 0);
      if (r <= 0) return false;
      p += r; n -= static_cast<size_t>(r);
    }
    return true;
  }
  bool send_raw(const void* buf, size_t n) {
    auto* p = static_cast<const uint8_t*>(buf);
    while (n > 0) {
      ssize_t w = send(fd, p, n, 0);
      if (w <= 0) return false;
      p += w; n -= static_cast<size_t>(w);
    }
    return true;
  }

  static void put_u32be(std::vector<uint8_t>& b, uint32_t v) {
    b.push_back((v>>24)&0xFF); b.push_back((v>>16)&0xFF);
    b.push_back((v>>8)&0xFF);  b.push_back(v&0xFF);
  }
  static void put_u16be(std::vector<uint8_t>& b, uint16_t v) {
    b.push_back((v>>8)&0xFF); b.push_back(v&0xFF);
  }
  static uint32_t get_u32be(const uint8_t* p) {
    return (uint32_t(p[0])<<24)|(uint32_t(p[1])<<16)|(uint32_t(p[2])<<8)|p[3];
  }

  // Read a single protocol message; returns type and payload.
  bool recv_msg(char* type, std::vector<uint8_t>* payload) {
    uint8_t hdr[5];
    if (!read_raw(hdr, 5)) return false;
    *type = char(hdr[0]);
    uint32_t len = get_u32be(hdr + 1);
    if (len < 4) return false;
    payload->resize(len - 4);
    if (!payload->empty() && !read_raw(payload->data(), payload->size()))
      return false;
    return true;
  }

  // Send a startup message (no type byte, just length + protocol + params).
  bool send_startup(
      const std::vector<std::pair<std::string,std::string>>& params) {
    std::vector<uint8_t> body;
    put_u32be(body, 196608);  // protocol 3.0
    for (auto& [k, v] : params) {
      body.insert(body.end(), k.begin(), k.end()); body.push_back(0);
      body.insert(body.end(), v.begin(), v.end()); body.push_back(0);
    }
    body.push_back(0);  // terminator

    std::vector<uint8_t> msg;
    put_u32be(msg, uint32_t(4 + body.size()));
    msg.insert(msg.end(), body.begin(), body.end());
    return send_raw(msg.data(), msg.size());
  }

  // Send a simple query message.
  bool send_query(const std::string& q) {
    std::vector<uint8_t> payload;
    payload.insert(payload.end(), q.begin(), q.end());
    payload.push_back(0);
    std::vector<uint8_t> msg;
    msg.push_back('Q');
    put_u32be(msg, uint32_t(4 + payload.size()));
    msg.insert(msg.end(), payload.begin(), payload.end());
    return send_raw(msg.data(), msg.size());
  }

  // Drain messages until we see ReadyForQuery.
  bool drain_to_ready() {
    for (;;) {
      char t; std::vector<uint8_t> p;
      if (!recv_msg(&t, &p)) return false;
      if (t == 'Z') return true;
      if (t == 'E') return false;
    }
  }

  // Read a CopyData message and return its inner payload.
  bool recv_copy_data(std::vector<uint8_t>* payload) {
    char t; std::vector<uint8_t> p;
    if (!recv_msg(&t, &p)) return false;
    if (t != 'd') return false;
    *payload = std::move(p);
    return true;
  }

  // Send a terminate message.
  void send_terminate() {
    uint8_t msg[5] = {'X', 0, 0, 0, 4};
    send_raw(msg, 5);
  }

  // Send CopyDone to end streaming.
  void send_copy_done() {
    uint8_t msg[5] = {'c', 0, 0, 0, 4};
    send_raw(msg, 5);
  }
};

// ── fixtures ──────────────────────────────────────────────────────────────────

static std::pair<int,int> make_socket_pair() {
  int fds[2];
  socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
  return {fds[0], fds[1]};
}

// ── tests ─────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(identify_system) {
  auto [server_fd, client_fd] = make_socket_pair();

  std::thread sender_thread([server_fd]() {
    WalSender s(server_fd, 12345678ULL, 1, 0x3000060ULL);
    s.run();
  });

  MiniStandby sb{client_fd};
  BOOST_REQUIRE(sb.send_startup({{"user","postgres"},{"replication","true"}}));
  BOOST_REQUIRE(sb.drain_to_ready());

  BOOST_REQUIRE(sb.send_query("IDENTIFY_SYSTEM"));

  // Expect: RowDescription, DataRow, CommandComplete, ReadyForQuery
  char type; std::vector<uint8_t> payload;
  BOOST_REQUIRE(sb.recv_msg(&type, &payload));
  BOOST_CHECK_EQUAL(type, 'T');  // RowDescription

  BOOST_REQUIRE(sb.recv_msg(&type, &payload));
  BOOST_CHECK_EQUAL(type, 'D');  // DataRow

  BOOST_REQUIRE(sb.recv_msg(&type, &payload));
  BOOST_CHECK_EQUAL(type, 'C');  // CommandComplete

  BOOST_REQUIRE(sb.recv_msg(&type, &payload));
  BOOST_CHECK_EQUAL(type, 'Z');  // ReadyForQuery

  sb.send_terminate();
  sender_thread.join();
  close(client_fd);
}

BOOST_AUTO_TEST_CASE(start_replication_and_receive_wal) {
  auto [server_fd, client_fd] = make_socket_pair();

  WalSender* sender_ptr = nullptr;
  std::thread sender_thread([server_fd, &sender_ptr]() {
    WalSender s(server_fd, 99ULL, 1, 0ULL);
    sender_ptr = &s;
    s.run();
  });

  MiniStandby sb{client_fd};
  BOOST_REQUIRE(sb.send_startup({{"user","rep"},{"replication","true"}}));
  BOOST_REQUIRE(sb.drain_to_ready());

  BOOST_REQUIRE(sb.send_query("IDENTIFY_SYSTEM"));
  BOOST_REQUIRE(sb.drain_to_ready());

  BOOST_REQUIRE(sb.send_query("START_REPLICATION PHYSICAL 0/0"));

  // Expect CopyBothResponse
  char type; std::vector<uint8_t> payload;
  BOOST_REQUIRE(sb.recv_msg(&type, &payload));
  BOOST_CHECK_EQUAL(type, 'W');

  // Wait until sender_ptr is set (run() has started the streaming loop).
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (sender_ptr == nullptr &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  BOOST_REQUIRE(sender_ptr != nullptr);

  // Send a WAL segment.
  std::vector<uint8_t> wal_data = {0xDE, 0xAD, 0xBE, 0xEF};
  sender_ptr->send(WalSegment{0x1000, wal_data});

  // Read the XLogData CopyData.
  BOOST_REQUIRE(sb.recv_copy_data(&payload));
  BOOST_REQUIRE(!payload.empty());
  BOOST_CHECK_EQUAL(char(payload[0]), 'w');   // XLogData marker
  // WAL bytes start at offset 1+8+8+8 = 25
  BOOST_REQUIRE(payload.size() >= 25 + 4);
  BOOST_CHECK_EQUAL_COLLECTIONS(
      payload.begin() + 25, payload.end(),
      wal_data.begin(), wal_data.end());

  sender_ptr->shutdown();
  sender_thread.join();
  close(client_fd);
}

BOOST_AUTO_TEST_CASE(server_accepts_connection) {
  uint64_t received_written_lsn = 0;
  StandbyStatus last_status{};

  WalSender* active_sender = nullptr;
  std::mutex mu;

  WalSenderServer server(
      0,  // bind to any free port
      42ULL, 1, 0ULL,
      [&](WalSender& s) {
        {
          std::lock_guard<std::mutex> lk(mu);
          active_sender = &s;
        }
        s.run();
        {
          std::lock_guard<std::mutex> lk(mu);
          active_sender = nullptr;
        }
      },
      [&](const StandbyStatus& st) {
        std::lock_guard<std::mutex> lk(mu);
        last_status = st;
        received_written_lsn = st.written_lsn;
      });

  BOOST_REQUIRE(server.start());
  BOOST_REQUIRE(server.bound_port() > 0);

  // Connect a mini-standby via TCP.
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  BOOST_REQUIRE(sock >= 0);
  struct sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(server.bound_port());
  BOOST_REQUIRE_EQUAL(
      connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)),
      0);

  MiniStandby sb{sock};
  BOOST_REQUIRE(sb.send_startup({{"user","rep"},{"replication","true"}}));
  BOOST_REQUIRE(sb.drain_to_ready());

  BOOST_REQUIRE(sb.send_query("IDENTIFY_SYSTEM"));
  BOOST_REQUIRE(sb.drain_to_ready());

  BOOST_REQUIRE(sb.send_query("START_REPLICATION PHYSICAL 0/0"));
  char type; std::vector<uint8_t> payload;
  BOOST_REQUIRE(sb.recv_msg(&type, &payload));
  BOOST_CHECK_EQUAL(type, 'W');

  // Wait for the sender to be registered.
  WalSender* sp = nullptr;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (!sp && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::lock_guard<std::mutex> lk(mu);
    sp = active_sender;
  }
  BOOST_REQUIRE(sp != nullptr);

  sp->shutdown();
  server.stop();
  close(sock);
}
