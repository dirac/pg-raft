#include "src/node/sync_acker.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pwd.h>
#include <sys/socket.h>

#include "src/base/logger.h"

namespace {

constexpr int64_t kPgEpochUs = 946684800LL * 1000000LL;  // 2000-01-01 in Unix microseconds

void put8(Bytes& b, uint8_t v) {
  b.push_back(v);
}
void put32(Bytes& b, uint32_t v) {
  for (int s = 24; s >= 0; s -= 8)
    b.push_back(static_cast<uint8_t>(v >> s));
}
void put64(Bytes& b, uint64_t v) {
  for (int s = 56; s >= 0; s -= 8)
    b.push_back(static_cast<uint8_t>(v >> s));
}
void put_str(Bytes& b, std::string_view s) {
  b.insert(b.end(), s.begin(), s.end());
  b.push_back(0);
}
uint32_t get32(uint8_t const* p) {
  return uint32_t{p[0]} << 24U | uint32_t{p[1]} << 16U | uint32_t{p[2]} << 8U | uint32_t{p[3]};
}
uint64_t get64(uint8_t const* p) {
  return uint64_t{get32(p)} << 32U | get32(p + 4);
}

/// A typed message: type, length (including itself), body.
Bytes message(char type, Bytes const& body) {
  Bytes m;
  put8(m, static_cast<uint8_t>(type));
  put32(m, static_cast<uint32_t>(body.size() + 4U));
  m.insert(m.end(), body.begin(), body.end());
  return m;
}

bool write_all(int fd, Bytes const& b) {
  size_t off = 0;
  while (off < b.size()) {
    auto n = ::send(fd, b.data() + off, b.size() - off, MSG_NOSIGNAL);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return false;
    off += static_cast<size_t>(n);
  }
  return true;
}

bool read_all(int fd, uint8_t* p, size_t n) {
  size_t off = 0;
  while (off < n) {
    auto r = ::recv(fd, p + off, n - off, 0);
    if (r < 0 && errno == EINTR)
      continue;
    if (r <= 0)
      return false;
    off += static_cast<size_t>(r);
  }
  return true;
}

/// The next message: its type and body; nullopt at the end of the connection.
std::optional<std::pair<char, Bytes>> read_message(int fd) {
  std::array<uint8_t, 5> h{};
  if (!read_all(fd, h.data(), h.size()))
    return std::nullopt;
  auto const len = get32(h.data() + 1);
  if (len < 4U || len > (64U << 20U))
    return std::nullopt;
  Bytes body(len - 4U);
  if (!body.empty() && !read_all(fd, body.data(), body.size()))
    return std::nullopt;
  return std::pair{static_cast<char>(h[0]), std::move(body)};
}

string error_text(Bytes const& body) {
  string out;
  for (size_t i = 0; i < body.size() && body[i] != 0;) {
    auto const field = static_cast<char>(body[i++]);
    string     value;
    while (i < body.size() && body[i] != 0)
      value.push_back(static_cast<char>(body[i++]));
    ++i;
    if (field == 'M')
      out = value;
  }
  return out;
}

/// A simple query's results up to ReadyForQuery: the first row's columns; nullopt on an error.
std::optional<vector<string>> simple_query(int fd, string const& sql, char stop_at = 'Z') {
  Bytes q;
  put_str(q, sql);
  if (!write_all(fd, message('Q', q)))
    return std::nullopt;
  vector<string> row;
  while (auto m = read_message(fd)) {
    auto const& [type, body] = *m;
    if (type == 'E') {
      log(ERROR, "sync acker: {}: {}", sql, error_text(body));
      return std::nullopt;
    }
    if (type == 'D' && row.empty() && body.size() >= 2) {
      auto const     n = static_cast<unsigned>((body[0] << 8U) | body[1]);
      uint8_t const* p = body.data() + 2;
      for (unsigned i = 0; i < n; ++i) {
        auto const l = static_cast<int32_t>(get32(p));
        p += 4;
        row.emplace_back(l < 0 ? "" : string(reinterpret_cast<char const*>(p), static_cast<size_t>(l)));
        if (l > 0)
          p += static_cast<size_t>(l);
      }
    }
    if (type == stop_at)
      return row;
  }
  return std::nullopt;
}

}  // namespace

Result<void> SyncAcker::start() {
  stop();
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd == -1)
    return elog(system_error_code(errno), "sync acker: socket");
  sockaddr_in addr{};
  addr.sin_family      = AF_INET;
  addr.sin_port        = htons(port_);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    auto ec = system_error_code(errno);
    ::close(fd);
    return elog(ec, "sync acker: connect to 127.0.0.1:{}", port_);
  }
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

  auto fail = [fd](char const* what) -> Result<void> {
    ::close(fd);
    return elog(system_error_code(EPROTO), "sync acker: {}", what);
  };

  // Startup: the database owner (the OS user, trusted on loopback), physical replication.
  auto const* pw = ::getpwuid(::geteuid());
  Bytes       s;
  put32(s, 0x00030000U);
  put_str(s, "user");
  put_str(s, pw ? pw->pw_name : "postgres");
  put_str(s, "replication");
  put_str(s, "true");
  put_str(s, "application_name");
  put_str(s, "pg_raft");
  put8(s, 0);
  Bytes startup;
  put32(startup, static_cast<uint32_t>(s.size() + 4U));
  startup.insert(startup.end(), s.begin(), s.end());
  if (!write_all(fd, startup))
    return fail("startup");
  for (;;) {
    auto m = read_message(fd);
    if (!m)
      return fail("startup: connection closed");
    if (m->first == 'E') {
      log(ERROR, "sync acker: startup: {}", error_text(m->second));
      return fail("startup refused");
    }
    if (m->first == 'R' && m->second.size() >= 4 && get32(m->second.data()) != 0)
      return fail("startup: authentication other than trust");
    if (m->first == 'Z')
      break;
  }

  // Where to start: the server's flush position, on its timeline.
  auto id = simple_query(fd, "IDENTIFY_SYSTEM");
  if (!id || id->size() < 3)
    return fail("IDENTIFY_SYSTEM");
  auto const& tli = id->at(1);
  auto const& pos = id->at(2);
  if (!simple_query(fd, std::format("START_REPLICATION PHYSICAL {} TIMELINE {}", pos, tli), 'W'))
    return fail("START_REPLICATION");
  unsigned hi = 0, lo = 0;
  if (sscanf(pos.c_str(), "%X/%X", &hi, &lo) == 2)
    received_.store(uint64_t{hi} << 32U | lo);

  fd_ = fd;
  running_.store(true);
  reader_ = std::thread([this] { read_loop(); });
  log(INFO, "sync acker: streaming from {} on timeline {}", pos, tli);
  return ok();
}

void SyncAcker::stop() {
  running_.store(false);
  if (fd_ != -1)
    ::shutdown(fd_, SHUT_RDWR);
  if (reader_.joinable())
    reader_.join();
  if (fd_ != -1) {
    auto lk = std::unique_lock{write_mtx_};
    ::close(std::exchange(fd_, -1));
  }
}

void SyncAcker::ack(uint64_t committed_lsn) {
  auto prev = committed_.load();
  while (prev < committed_lsn && !committed_.compare_exchange_weak(prev, committed_lsn)) {
  }
  if (running_.load())
    send_status(false);
}

void SyncAcker::send_status(bool force, bool reply_requested) {
  auto lk  = std::unique_lock{write_mtx_};
  auto pos = std::min(committed_.load(), received_.load());
  if (fd_ == -1 || (!force && pos <= sent_))
    return;
  sent_ = std::max(sent_, pos);

  auto const now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
  Bytes r;
  put8(r, 'r');
  put64(r, pos);  // written
  put64(r, pos);  // flushed
  put64(r, pos);  // applied
  put64(r, static_cast<uint64_t>(now_us - kPgEpochUs));
  put8(r, reply_requested ? 1 : 0);
  (void)write_all(fd_, message('d', r));
}

void SyncAcker::read_loop() {
  while (running_.load()) {
    auto m = read_message(fd_);
    if (!m)
      break;
    auto const& [type, body] = *m;
    if (type == 'E') {
      log(ERROR, "sync acker: {}", error_text(body));
      break;
    }
    if (type != 'd' || body.empty())
      continue;
    if (body[0] == 'w' && body.size() >= 25) {  // XLogData: start, end of WAL, time, bytes
      auto const start = get64(body.data() + 1);
      auto const end   = start + (body.size() - 25U);
      auto       prev  = received_.load();
      while (prev < end && !received_.compare_exchange_weak(prev, end)) {
      }
      send_status(false);  // WAL committed before it arrived here
    } else if (body[0] == 'k' && body.size() >= 18) {  // keepalive: end of WAL, time, reply?
      if (body[17] != 0)
        send_status(true);
    }
  }
  if (running_.load())
    log(WARN, "sync acker: the replication connection ended");
  running_.store(false);
}
