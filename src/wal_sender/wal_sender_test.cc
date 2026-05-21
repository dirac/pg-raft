#include "src/wal_sender/wal_sender.h"
#include "src/wal_sender/wal_sender_server.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <thread>

#define BOOST_TEST_MODULE WalSenderTest
#include <boost/test/included/unit_test.hpp>

// ── mini standby helper ───────────────────────────────────────────────────────
//
// Speaks just enough of the standby side of the replication protocol to let
// the tests exercise WalSender without a real PostgreSQL process.

struct MiniStandby {
  int fd;

  // Helpers to read/write raw bytes.
  bool ReadRaw(void* buf, size_t n) {
    auto* p = static_cast<uint8_t*>(buf);
    while (n > 0) {
      ssize_t r = recv(fd, p, n, 0);
      if (r <= 0) return false;
      p += r; n -= static_cast<size_t>(r);
    }
    return true;
  }
  bool SendRaw(const void* buf, size_t n) {
    auto* p = static_cast<const uint8_t*>(buf);
    while (n > 0) {
      ssize_t w = send(fd, p, n, 0);
      if (w <= 0) return false;
      p += w; n -= static_cast<size_t>(w);
    }
    return true;
  }

  static void PutU32BE(std::vector<uint8_t>& b, uint32_t v) {
    b.push_back((v>>24)&0xFF); b.push_back((v>>16)&0xFF);
    b.push_back((v>>8)&0xFF);  b.push_back(v&0xFF);
  }
  static void PutU16BE(std::vector<uint8_t>& b, uint16_t v) {
    b.push_back((v>>8)&0xFF); b.push_back(v&0xFF);
  }
  static uint32_t GetU32BE(const uint8_t* p) {
    return (uint32_t(p[0])<<24)|(uint32_t(p[1])<<16)|(uint32_t(p[2])<<8)|p[3];
  }

  // Read a single protocol message; returns type and payload.
  bool RecvMsg(char* type, std::vector<uint8_t>* payload) {
    uint8_t hdr[5];
    if (!ReadRaw(hdr, 5)) return false;
    *type = char(hdr[0]);
    uint32_t len = GetU32BE(hdr + 1);
    if (len < 4) return false;
    payload->resize(len - 4);
    if (!payload->empty() && !ReadRaw(payload->data(), payload->size()))
      return false;
    return true;
  }

  // Send a startup message (no type byte, just length + protocol + params).
  bool SendStartup(
      const std::vector<std::pair<std::string,std::string>>& params) {
    std::vector<uint8_t> body;
    PutU32BE(body, 196608);  // protocol 3.0
    for (auto& [k, v] : params) {
      body.insert(body.end(), k.begin(), k.end()); body.push_back(0);
      body.insert(body.end(), v.begin(), v.end()); body.push_back(0);
    }
    body.push_back(0);  // terminator

    std::vector<uint8_t> msg;
    PutU32BE(msg, uint32_t(4 + body.size()));
    msg.insert(msg.end(), body.begin(), body.end());
    return SendRaw(msg.data(), msg.size());
  }

  // Send a simple query message.
  bool SendQuery(const std::string& q) {
    std::vector<uint8_t> payload;
    payload.insert(payload.end(), q.begin(), q.end());
    payload.push_back(0);
    std::vector<uint8_t> msg;
    msg.push_back('Q');
    PutU32BE(msg, uint32_t(4 + payload.size()));
    msg.insert(msg.end(), payload.begin(), payload.end());
    return SendRaw(msg.data(), msg.size());
  }

  // Drain messages until we see ReadyForQuery.
  bool DrainToReady() {
    for (;;) {
      char t; std::vector<uint8_t> p;
      if (!RecvMsg(&t, &p)) return false;
      if (t == 'Z') return true;
      if (t == 'E') return false;
    }
  }

  // Read a CopyData message and return its inner payload.
  bool RecvCopyData(std::vector<uint8_t>* payload) {
    char t; std::vector<uint8_t> p;
    if (!RecvMsg(&t, &p)) return false;
    if (t != 'd') return false;
    *payload = std::move(p);
    return true;
  }

  // Send a terminate message.
  void SendTerminate() {
    uint8_t msg[5] = {'X', 0, 0, 0, 4};
    SendRaw(msg, 5);
  }

  // Send CopyDone to end streaming.
  void SendCopyDone() {
    uint8_t msg[5] = {'c', 0, 0, 0, 4};
    SendRaw(msg, 5);
  }
};

// ── fixtures ──────────────────────────────────────────────────────────────────

static std::pair<int,int> MakeSocketPair() {
  int fds[2];
  socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
  return {fds[0], fds[1]};
}

// ── tests ─────────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(IdentifySystem) {
  auto [server_fd, client_fd] = MakeSocketPair();

  std::thread sender_thread([server_fd]() {
    WalSender s(server_fd, 12345678ULL, 1, 0x3000060ULL);
    s.Run();
  });

  MiniStandby sb{client_fd};
  BOOST_REQUIRE(sb.SendStartup({{"user","postgres"},{"replication","true"}}));
  BOOST_REQUIRE(sb.DrainToReady());

  BOOST_REQUIRE(sb.SendQuery("IDENTIFY_SYSTEM"));

  // Expect: RowDescription, DataRow, CommandComplete, ReadyForQuery
  char type; std::vector<uint8_t> payload;
  BOOST_REQUIRE(sb.RecvMsg(&type, &payload));
  BOOST_CHECK_EQUAL(type, 'T');  // RowDescription

  BOOST_REQUIRE(sb.RecvMsg(&type, &payload));
  BOOST_CHECK_EQUAL(type, 'D');  // DataRow

  BOOST_REQUIRE(sb.RecvMsg(&type, &payload));
  BOOST_CHECK_EQUAL(type, 'C');  // CommandComplete

  BOOST_REQUIRE(sb.RecvMsg(&type, &payload));
  BOOST_CHECK_EQUAL(type, 'Z');  // ReadyForQuery

  sb.SendTerminate();
  sender_thread.join();
  close(client_fd);
}

BOOST_AUTO_TEST_CASE(StartReplicationAndReceiveWal) {
  auto [server_fd, client_fd] = MakeSocketPair();

  WalSender* sender_ptr = nullptr;
  std::thread sender_thread([server_fd, &sender_ptr]() {
    WalSender s(server_fd, 99ULL, 1, 0ULL);
    sender_ptr = &s;
    s.Run();
  });

  MiniStandby sb{client_fd};
  BOOST_REQUIRE(sb.SendStartup({{"user","rep"},{"replication","true"}}));
  BOOST_REQUIRE(sb.DrainToReady());

  BOOST_REQUIRE(sb.SendQuery("IDENTIFY_SYSTEM"));
  BOOST_REQUIRE(sb.DrainToReady());

  BOOST_REQUIRE(sb.SendQuery("START_REPLICATION PHYSICAL 0/0"));

  // Expect CopyBothResponse
  char type; std::vector<uint8_t> payload;
  BOOST_REQUIRE(sb.RecvMsg(&type, &payload));
  BOOST_CHECK_EQUAL(type, 'W');

  // Wait until sender_ptr is set (Run() has started the streaming loop).
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (sender_ptr == nullptr &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  BOOST_REQUIRE(sender_ptr != nullptr);

  // Send a WAL segment.
  std::vector<uint8_t> wal_data = {0xDE, 0xAD, 0xBE, 0xEF};
  sender_ptr->Send(WalSegment{0x1000, wal_data});

  // Read the XLogData CopyData.
  BOOST_REQUIRE(sb.RecvCopyData(&payload));
  BOOST_REQUIRE(!payload.empty());
  BOOST_CHECK_EQUAL(char(payload[0]), 'w');   // XLogData marker
  // WAL bytes start at offset 1+8+8+8 = 25
  BOOST_REQUIRE(payload.size() >= 25 + 4);
  BOOST_CHECK_EQUAL_COLLECTIONS(
      payload.begin() + 25, payload.end(),
      wal_data.begin(), wal_data.end());

  sender_ptr->Shutdown();
  sender_thread.join();
  close(client_fd);
}

BOOST_AUTO_TEST_CASE(ServerAcceptsConnection) {
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
        s.Run();
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

  BOOST_REQUIRE(server.Start());
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
  BOOST_REQUIRE(sb.SendStartup({{"user","rep"},{"replication","true"}}));
  BOOST_REQUIRE(sb.DrainToReady());

  BOOST_REQUIRE(sb.SendQuery("IDENTIFY_SYSTEM"));
  BOOST_REQUIRE(sb.DrainToReady());

  BOOST_REQUIRE(sb.SendQuery("START_REPLICATION PHYSICAL 0/0"));
  char type; std::vector<uint8_t> payload;
  BOOST_REQUIRE(sb.RecvMsg(&type, &payload));
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

  sp->Shutdown();
  server.Stop();
  close(sock);
}
