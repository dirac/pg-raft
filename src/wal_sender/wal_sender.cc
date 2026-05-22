#include "src/wal_sender/wal_sender.h"

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <iostream>

// Microseconds between the Unix epoch (1970) and the PostgreSQL epoch (2000).
static constexpr int64_t kPgEpochOffsetUs = 946684800LL * 1000000LL;

// ── construction / destruction ────────────────────────────────────────────────

WalSender::WalSender(int fd, uint64_t system_id, uint32_t timeline,
                     uint64_t end_lsn, StatusCallback status_cb,
                     uint32_t wal_segment_size)
    : fd_(fd),
      system_id_(system_id),
      timeline_(timeline),
      end_lsn_(end_lsn),
      wal_segment_size_(wal_segment_size),
      status_cb_(std::move(status_cb)) {
  pipe(wakeup_);
  fcntl(wakeup_[0], F_SETFL, O_NONBLOCK);
  fcntl(wakeup_[1], F_SETFL, O_NONBLOCK);

#ifdef SO_NOSIGPIPE
  int one = 1;
  setsockopt(fd_, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
}

WalSender::~WalSender() {
  if (fd_ >= 0) close(fd_);
  if (wakeup_[0] >= 0) close(wakeup_[0]);
  if (wakeup_[1] >= 0) close(wakeup_[1]);
}

// ── public API ────────────────────────────────────────────────────────────────

bool WalSender::run() {
  return do_handshake();
}

void WalSender::send(WalSegment seg) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    queue_.push(std::move(seg));
  }
  // Wake up the streaming poll loop.
  char b = 1;
  write(wakeup_[1], &b, 1);
}

void WalSender::shutdown() {
  {
    std::lock_guard<std::mutex> lk(mu_);
    shutdown_ = true;
  }
  char b = 1;
  write(wakeup_[1], &b, 1);
}

// ── handshake ─────────────────────────────────────────────────────────────────

bool WalSender::do_handshake() {
  std::vector<std::pair<std::string, std::string>> params;
  if (!recv_startup(&params)) return false;
  if (!send_auth_ok()) return false;
  if (!send_parameter_status("server_version", "14.0")) return false;
  if (!send_parameter_status("integer_datetimes", "on")) return false;
  if (!send_parameter_status("server_encoding", "UTF8")) return false;
  if (!send_backend_key_data()) return false;
  if (!send_ready_for_query()) return false;

  while (true) {
    char type;
    std::vector<uint8_t> payload;
    if (!recv_message(&type, &payload)) return false;

    if (type == 'X') return true;  // client terminated

    if (type != 'Q') {
      send_error_response("expected simple query message");
      return false;
    }

    std::string query(reinterpret_cast<const char*>(payload.data()),
                      payload.size());
    while (!query.empty() && query.back() == '\0') query.pop_back();
    std::cerr << "Query " << query << std::endl;

    // Case-insensitive prefix match on the command keyword.
    std::string upper = query;
    for (char& c : upper) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));

    if (upper.find("IDENTIFY_SYSTEM") != std::string::npos) {
      if (!handle_identify_system()) return false;
    } else if (upper.find("START_REPLICATION") != std::string::npos) {
      return handle_start_replication(query);
    } else if (upper.find("TIMELINE_HISTORY") != std::string::npos) {
      std::vector<std::pair<std::string, int32_t>> cols = {
          {"filename", 25}, {"content", 25}};
      if (!send_row_description(cols)) return false;
      if (!send_command_complete("TIMELINE_HISTORY 1")) return false;
      if (!send_ready_for_query()) return false;
    } else if (upper.rfind("SET ", 0) == 0) {
      // SET command – accept and ignore.
      if (!send_command_complete("SET")) return false;
      if (!send_ready_for_query()) return false;
    } else if (upper.rfind("SHOW ", 0) == 0) {
      // SHOW param – return value as a single-column result.
      std::string param = query.substr(5);
      while (!param.empty() && (isspace((unsigned char)param.back()) ||
                                 param.back() == ';'))
        param.pop_back();
      std::string value;
      if (param == "wal_segment_size") {
        value = std::to_string(wal_segment_size_);
      }
      std::vector<std::pair<std::string, int32_t>> cols = {{param, 25}};
      if (!send_row_description(cols)) return false;
      if (!send_data_row({{value}})) return false;
      if (!send_command_complete("SHOW")) return false;
      if (!send_ready_for_query()) return false;
    } else if (upper.rfind("SELECT ", 0) == 0) {
      // Generic SELECT (e.g. set_config) – return one row with empty string.
      std::vector<std::pair<std::string, int32_t>> cols = {{"?column?", 25}};
      if (!send_row_description(cols)) return false;
      if (!send_data_row({{std::string("")}})) return false;
      if (!send_command_complete("SELECT 1")) return false;
      if (!send_ready_for_query()) return false;
    } else {
      if (!send_error_response("unrecognized replication command")) return false;
      if (!send_ready_for_query()) return false;
    }
  }
}

bool WalSender::handle_identify_system() {
  // Columns: systemid text, timeline int4, xlogpos text, dbname text
  std::vector<std::pair<std::string, int32_t>> cols = {
      {"systemid", 25}, {"timeline", 23}, {"xlogpos", 25}, {"dbname", 25}};
  if (!send_row_description(cols)) return false;

  std::vector<std::optional<std::string>> row = {
      std::to_string(system_id_),
      std::to_string(timeline_),
      format_lsn(end_lsn_),
      std::nullopt,  // physical replication has no database
  };
  if (!send_data_row(row)) return false;
  if (!send_command_complete("SELECT 1")) return false;
  if (!send_ready_for_query()) return false;
  return true;
}

bool WalSender::handle_start_replication(const std::string& cmd) {
  // Syntax: START_REPLICATION [SLOT name] [PHYSICAL] start_lsn [TIMELINE n]
  // Find the LSN token: hex digits, a slash, more hex digits.
  uint64_t start_lsn = 0;
  bool found = false;
  std::istringstream ss(cmd);
  std::string tok;
  while (ss >> tok) {
    auto slash = tok.find('/');
    if (slash == std::string::npos || slash == 0 || slash == tok.size() - 1)
      continue;
    bool all_hex = true;
    for (size_t i = 0; i < tok.size(); i++) {
      if (i == slash) continue;
      if (!isxdigit(static_cast<unsigned char>(tok[i]))) { all_hex = false; break; }
    }
    if (all_hex) {
      start_lsn = parse_lsn(tok);
      found = true;
      break;
    }
  }
  if (!found) {
    return send_error_response("could not parse replication start LSN");
  }

  if (!send_copy_both_response()) return false;
  if (replication_start_cb_) replication_start_cb_(start_lsn);
  return streaming_loop(start_lsn);
}

// ── streaming loop ────────────────────────────────────────────────────────────

bool WalSender::streaming_loop(uint64_t /*start_lsn*/) {
  using clock = std::chrono::steady_clock;
  constexpr auto kKeepaliveInterval = std::chrono::seconds(5);

  auto last_ka = clock::now();

  while (true) {
    // Drain the WAL queue and send everything pending.
    std::queue<WalSegment> pending;
    {
      std::lock_guard<std::mutex> lk(mu_);
      std::swap(pending, queue_);
    }
    while (!pending.empty()) {
      if (!send_xlog_data(pending.front())) return false;
      pending.pop();
    }

    // Check shutdown after queue is drained.
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (shutdown_ && queue_.empty()) return true;
    }

    // How long until we must send the next keepalive?
    auto now = clock::now();
    auto next_ka = last_ka + kKeepaliveInterval;
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        next_ka - now).count();
    if (ms < 0) ms = 0;

    struct pollfd fds[2];
    fds[0].fd = fd_;        fds[0].events = POLLIN;
    fds[1].fd = wakeup_[0]; fds[1].events = POLLIN;

    int nready = poll(fds, 2, static_cast<int>(ms));
    if (nready < 0) return false;

    // Keepalive timer fired.
    if (clock::now() >= next_ka) {
      if (!send_keepalive(false)) return false;
      last_ka = clock::now();
    }

    // Drain the wakeup pipe.
    if (fds[1].revents & POLLIN) {
      char buf[64];
      while (read(wakeup_[0], buf, sizeof(buf)) > 0) {}
    }

    // Process incoming message from standby.
    if (fds[0].revents & POLLIN) {
      char type;
      std::vector<uint8_t> payload;
      if (!recv_message(&type, &payload)) return false;

      if (type == 'd') {
        // CopyData
        if (!process_copy_data(payload)) return false;
      } else if (type == 'c') {
        // CopyDone – standby ended streaming
        return true;
      } else if (type == 'f') {
        // CopyFail
        return false;
      } else if (type == 'X') {
        return true;
      }
    }
  }
}

bool WalSender::send_xlog_data(const WalSegment& seg) {
  uint64_t seg_end = seg.start_lsn + seg.data.size();

  std::vector<uint8_t> payload;
  payload.reserve(1 + 8 + 8 + 8 + seg.data.size());
  put_u8(payload, 'w');
  put_u64be(payload, seg.start_lsn);
  put_u64be(payload, end_lsn_);
  put_i64be(payload, now_pg_timestamp());
  payload.insert(payload.end(), seg.data.begin(), seg.data.end());

  if (!send_message('d', payload)) return false;

  if (seg_end > end_lsn_) end_lsn_ = seg_end;
  return true;
}

bool WalSender::send_keepalive(bool reply_requested) {
  std::vector<uint8_t> payload;
  put_u8(payload, 'k');
  put_u64be(payload, end_lsn_);
  put_i64be(payload, now_pg_timestamp());
  put_u8(payload, reply_requested ? 1 : 0);
  return send_message('d', payload);
}

bool WalSender::process_copy_data(const std::vector<uint8_t>& payload) {
  if (payload.empty()) return true;

  char msg_type = static_cast<char>(payload[0]);

  if (msg_type == 'r') {
    // StandbyStatusUpdate: 'r' + written(8) + flushed(8) + applied(8) + ts(8) + reply(1)
    if (payload.size() < 1 + 8 + 8 + 8 + 8 + 1) return true;
    StandbyStatus st;
    st.written_lsn = get_u64be(payload.data() + 1);
    st.flushed_lsn = get_u64be(payload.data() + 9);
    st.applied_lsn = get_u64be(payload.data() + 17);
    bool wants_reply = payload[33] != 0;
    if (status_cb_) status_cb_(st);
    if (wants_reply && !send_keepalive(false)) return false;
  }
  // HotStandbyFeedback ('h') is silently ignored.

  return true;
}

// ── startup message (no type byte, special framing) ───────────────────────────

bool WalSender::recv_startup(
    std::vector<std::pair<std::string, std::string>>* params) {
  for (;;) {
    uint8_t len_buf[4];
    if (!recv_raw(len_buf, 4)) return false;
    uint32_t len = get_u32be(len_buf);
    if (len < 4) return false;

    std::vector<uint8_t> body(len - 4);
    if (!body.empty() && !recv_raw(body.data(), body.size())) return false;
    if (body.size() < 4) return false;

    uint32_t proto = get_u32be(body.data());

    if (proto == 80877103) {
      // SSLRequest – decline
      if (!send_raw("N", 1)) return false;
      continue;
    }
    if (proto == 80877102) {
      // CancelRequest – not handled
      return false;
    }

    // Standard startup (protocol 3.0 = 196608)
    size_t i = 4;
    while (i < body.size() && body[i] != 0) {
      std::string key, value;
      while (i < body.size() && body[i] != 0) key += static_cast<char>(body[i++]);
      if (i < body.size()) i++;
      while (i < body.size() && body[i] != 0) value += static_cast<char>(body[i++]);
      if (i < body.size()) i++;
      params->emplace_back(std::move(key), std::move(value));
    }
    return true;
  }
}

// ── regular message I/O ───────────────────────────────────────────────────────

bool WalSender::recv_message(char* type, std::vector<uint8_t>* payload) {
  uint8_t hdr[5];
  if (!recv_raw(hdr, 5)) return false;
  *type = static_cast<char>(hdr[0]);
  uint32_t len = get_u32be(hdr + 1);
  if (len < 4) return false;
  payload->resize(len - 4);
  if (!payload->empty() && !recv_raw(payload->data(), payload->size()))
    return false;
  return true;
}

bool WalSender::send_message(char type, const std::vector<uint8_t>& payload) {
  uint8_t hdr[5];
  hdr[0] = static_cast<uint8_t>(type);
  uint32_t len = static_cast<uint32_t>(4 + payload.size());
  hdr[1] = (len >> 24) & 0xFF;
  hdr[2] = (len >> 16) & 0xFF;
  hdr[3] = (len >> 8) & 0xFF;
  hdr[4] = len & 0xFF;
  if (!send_raw(hdr, 5)) return false;
  if (!payload.empty() && !send_raw(payload.data(), payload.size())) return false;
  return true;
}

bool WalSender::recv_raw(void* buf, size_t n) {
  auto* p = static_cast<uint8_t*>(buf);
  while (n > 0) {
    ssize_t r = ::recv(fd_, p, n, 0);
    if (r <= 0) return false;
    p += r;
    n -= static_cast<size_t>(r);
  }
  return true;
}

bool WalSender::send_raw(const void* buf, size_t n) {
  const auto* p = static_cast<const uint8_t*>(buf);
  while (n > 0) {
#ifdef MSG_NOSIGNAL
    ssize_t w = ::send(fd_, p, n, MSG_NOSIGNAL);
#else
    ssize_t w = ::send(fd_, p, n, 0);
#endif
    if (w <= 0) return false;
    p += w;
    n -= static_cast<size_t>(w);
  }
  return true;
}

// ── protocol message builders ─────────────────────────────────────────────────

bool WalSender::send_auth_ok() {
  std::vector<uint8_t> p;
  put_u32be(p, 0);  // AuthenticationOk
  return send_message('R', p);
}

bool WalSender::send_parameter_status(const char* name, const char* value) {
  std::vector<uint8_t> p;
  put_str(p, name);
  put_str(p, value);
  return send_message('S', p);
}

bool WalSender::send_backend_key_data() {
  std::vector<uint8_t> p;
  put_u32be(p, static_cast<uint32_t>(getpid()));
  put_u32be(p, 0);  // secret key (cancel not supported)
  return send_message('K', p);
}

bool WalSender::send_ready_for_query() {
  std::vector<uint8_t> p;
  put_u8(p, 'I');  // idle
  return send_message('Z', p);
}

bool WalSender::send_row_description(
    const std::vector<std::pair<std::string, int32_t>>& cols) {
  std::vector<uint8_t> p;
  put_u16be(p, static_cast<uint16_t>(cols.size()));
  for (const auto& [name, type_oid] : cols) {
    put_str(p, name);
    put_u32be(p, 0);                           // table OID
    put_u16be(p, 0);                           // column attribute number
    put_u32be(p, static_cast<uint32_t>(type_oid));
    int16_t sz = (type_oid == 23) ? 4 : (type_oid == 20) ? 8 : -1;
    put_u16be(p, static_cast<uint16_t>(sz));   // type size
    put_u32be(p, static_cast<uint32_t>(-1));   // type modifier
    put_u16be(p, 0);                           // format: text
  }
  return send_message('T', p);
}

bool WalSender::send_data_row(
    const std::vector<std::optional<std::string>>& vals) {
  std::vector<uint8_t> p;
  put_u16be(p, static_cast<uint16_t>(vals.size()));
  for (const auto& v : vals) {
    if (!v) {
      put_u32be(p, static_cast<uint32_t>(-1));  // NULL
    } else {
      put_u32be(p, static_cast<uint32_t>(v->size()));
      p.insert(p.end(), v->begin(), v->end());
    }
  }
  return send_message('D', p);
}

bool WalSender::send_command_complete(const char* tag) {
  std::vector<uint8_t> p;
  put_str(p, tag);
  return send_message('C', p);
}

bool WalSender::send_copy_both_response() {
  std::vector<uint8_t> p;
  put_u8(p, 0);     // overall format: text
  put_u16be(p, 0);  // number of column format codes
  return send_message('W', p);
}

bool WalSender::send_error_response(const char* msg) {
  std::vector<uint8_t> p;
  put_u8(p, 'S');  put_str(p, "ERROR");
  put_u8(p, 'M');  put_str(p, msg);
  put_u8(p, 0);    // terminator
  return send_message('E', p);
}

// ── encode / decode helpers ───────────────────────────────────────────────────

uint64_t WalSender::parse_lsn(const std::string& s) {
  uint32_t hi = 0, lo = 0;
  sscanf(s.c_str(), "%X/%X", &hi, &lo);
  return (static_cast<uint64_t>(hi) << 32) | lo;
}

std::string WalSender::format_lsn(uint64_t lsn) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%X/%X",
           static_cast<uint32_t>(lsn >> 32),
           static_cast<uint32_t>(lsn));
  return buf;
}

void WalSender::put_u8(std::vector<uint8_t>& b, uint8_t v) {
  b.push_back(v);
}
void WalSender::put_u16be(std::vector<uint8_t>& b, uint16_t v) {
  b.push_back((v >> 8) & 0xFF);
  b.push_back(v & 0xFF);
}
void WalSender::put_u32be(std::vector<uint8_t>& b, uint32_t v) {
  b.push_back((v >> 24) & 0xFF);
  b.push_back((v >> 16) & 0xFF);
  b.push_back((v >> 8) & 0xFF);
  b.push_back(v & 0xFF);
}
void WalSender::put_u64be(std::vector<uint8_t>& b, uint64_t v) {
  b.push_back((v >> 56) & 0xFF);
  b.push_back((v >> 48) & 0xFF);
  b.push_back((v >> 40) & 0xFF);
  b.push_back((v >> 32) & 0xFF);
  b.push_back((v >> 24) & 0xFF);
  b.push_back((v >> 16) & 0xFF);
  b.push_back((v >> 8) & 0xFF);
  b.push_back(v & 0xFF);
}
void WalSender::put_i64be(std::vector<uint8_t>& b, int64_t v) {
  put_u64be(b, static_cast<uint64_t>(v));
}
void WalSender::put_str(std::vector<uint8_t>& b, const std::string& s) {
  b.insert(b.end(), s.begin(), s.end());
  b.push_back(0);
}
uint32_t WalSender::get_u32be(const uint8_t* p) {
  return (static_cast<uint32_t>(p[0]) << 24) |
         (static_cast<uint32_t>(p[1]) << 16) |
         (static_cast<uint32_t>(p[2]) << 8) |
          static_cast<uint32_t>(p[3]);
}
uint64_t WalSender::get_u64be(const uint8_t* p) {
  return (static_cast<uint64_t>(p[0]) << 56) |
         (static_cast<uint64_t>(p[1]) << 48) |
         (static_cast<uint64_t>(p[2]) << 40) |
         (static_cast<uint64_t>(p[3]) << 32) |
         (static_cast<uint64_t>(p[4]) << 24) |
         (static_cast<uint64_t>(p[5]) << 16) |
         (static_cast<uint64_t>(p[6]) << 8) |
          static_cast<uint64_t>(p[7]);
}
int64_t WalSender::now_pg_timestamp() {
  auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
  return us - kPgEpochOffsetUs;
}
