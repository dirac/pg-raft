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

// Microseconds between the Unix epoch (1970) and the PostgreSQL epoch (2000).
static constexpr int64_t kPgEpochOffsetUs = 946684800LL * 1000000LL;

// ── construction / destruction ────────────────────────────────────────────────

WalSender::WalSender(int fd, uint64_t system_id, uint32_t timeline,
                     uint64_t end_lsn, StatusCallback status_cb)
    : fd_(fd),
      system_id_(system_id),
      timeline_(timeline),
      end_lsn_(end_lsn),
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

bool WalSender::Run() {
  return DoHandshake();
}

void WalSender::Send(WalSegment seg) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    queue_.push(std::move(seg));
  }
  // Wake up the streaming poll loop.
  char b = 1;
  write(wakeup_[1], &b, 1);
}

void WalSender::Shutdown() {
  {
    std::lock_guard<std::mutex> lk(mu_);
    shutdown_ = true;
  }
  char b = 1;
  write(wakeup_[1], &b, 1);
}

// ── handshake ─────────────────────────────────────────────────────────────────

bool WalSender::DoHandshake() {
  std::vector<std::pair<std::string, std::string>> params;
  if (!RecvStartup(&params)) return false;
  if (!SendAuthOk()) return false;
  if (!SendParameterStatus("server_version", "14.0")) return false;
  if (!SendParameterStatus("integer_datetimes", "on")) return false;
  if (!SendParameterStatus("server_encoding", "UTF8")) return false;
  if (!SendBackendKeyData()) return false;
  if (!SendReadyForQuery()) return false;

  while (true) {
    char type;
    std::vector<uint8_t> payload;
    if (!RecvMessage(&type, &payload)) return false;

    if (type == 'X') return true;  // client terminated

    if (type != 'Q') {
      SendErrorResponse("expected simple query message");
      return false;
    }

    std::string query(reinterpret_cast<const char*>(payload.data()),
                      payload.size());
    while (!query.empty() && query.back() == '\0') query.pop_back();

    // Case-insensitive prefix match on the command keyword.
    std::string upper = query;
    for (char& c : upper) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));

    if (upper.find("IDENTIFY_SYSTEM") != std::string::npos) {
      if (!HandleIdentifySystem()) return false;
    } else if (upper.find("START_REPLICATION") != std::string::npos) {
      return HandleStartReplication(query);
    } else if (upper.find("TIMELINE_HISTORY") != std::string::npos) {
      // Return an empty result set; we only have one timeline.
      std::vector<std::pair<std::string, int32_t>> cols = {
          {"filename", 25}, {"content", 25}};
      if (!SendRowDescription(cols)) return false;
      if (!SendCommandComplete("TIMELINE_HISTORY 1")) return false;
      if (!SendReadyForQuery()) return false;
    } else {
      if (!SendErrorResponse("unrecognized replication command")) return false;
      if (!SendReadyForQuery()) return false;
    }
  }
}

bool WalSender::HandleIdentifySystem() {
  // Columns: systemid text, timeline int4, xlogpos text, dbname text
  std::vector<std::pair<std::string, int32_t>> cols = {
      {"systemid", 25}, {"timeline", 23}, {"xlogpos", 25}, {"dbname", 25}};
  if (!SendRowDescription(cols)) return false;

  std::vector<std::optional<std::string>> row = {
      std::to_string(system_id_),
      std::to_string(timeline_),
      FormatLSN(end_lsn_),
      std::nullopt,  // physical replication has no database
  };
  if (!SendDataRow(row)) return false;
  if (!SendCommandComplete("SELECT 1")) return false;
  if (!SendReadyForQuery()) return false;
  return true;
}

bool WalSender::HandleStartReplication(const std::string& cmd) {
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
      start_lsn = ParseLSN(tok);
      found = true;
      break;
    }
  }
  if (!found) {
    return SendErrorResponse("could not parse replication start LSN");
  }

  if (!SendCopyBothResponse()) return false;
  return StreamingLoop(start_lsn);
}

// ── streaming loop ────────────────────────────────────────────────────────────

bool WalSender::StreamingLoop(uint64_t /*start_lsn*/) {
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
      if (!SendXLogData(pending.front())) return false;
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
    fds[0].fd = fd_;       fds[0].events = POLLIN;
    fds[1].fd = wakeup_[0]; fds[1].events = POLLIN;

    int nready = poll(fds, 2, static_cast<int>(ms));
    if (nready < 0) return false;

    // Keepalive timer fired.
    if (clock::now() >= next_ka) {
      if (!SendKeepalive(false)) return false;
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
      if (!RecvMessage(&type, &payload)) return false;

      if (type == 'd') {
        // CopyData
        if (!ProcessCopyData(payload)) return false;
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

bool WalSender::SendXLogData(const WalSegment& seg) {
  uint64_t seg_end = seg.start_lsn + seg.data.size();

  std::vector<uint8_t> payload;
  payload.reserve(1 + 8 + 8 + 8 + seg.data.size());
  PutU8(payload, 'w');
  PutU64BE(payload, seg.start_lsn);
  PutU64BE(payload, end_lsn_);
  PutI64BE(payload, NowPgTimestamp());
  payload.insert(payload.end(), seg.data.begin(), seg.data.end());

  if (!SendMessage('d', payload)) return false;

  if (seg_end > end_lsn_) end_lsn_ = seg_end;
  return true;
}

bool WalSender::SendKeepalive(bool reply_requested) {
  std::vector<uint8_t> payload;
  PutU8(payload, 'k');
  PutU64BE(payload, end_lsn_);
  PutI64BE(payload, NowPgTimestamp());
  PutU8(payload, reply_requested ? 1 : 0);
  return SendMessage('d', payload);
}

bool WalSender::ProcessCopyData(const std::vector<uint8_t>& payload) {
  if (payload.empty()) return true;

  char msg_type = static_cast<char>(payload[0]);

  if (msg_type == 'r') {
    // StandbyStatusUpdate: 'r' + written(8) + flushed(8) + applied(8) + ts(8) + reply(1)
    if (payload.size() < 1 + 8 + 8 + 8 + 8 + 1) return true;
    StandbyStatus st;
    st.written_lsn = GetU64BE(payload.data() + 1);
    st.flushed_lsn = GetU64BE(payload.data() + 9);
    st.applied_lsn = GetU64BE(payload.data() + 17);
    bool wants_reply = payload[33] != 0;
    if (status_cb_) status_cb_(st);
    if (wants_reply && !SendKeepalive(false)) return false;
  }
  // HotStandbyFeedback ('h') is silently ignored.

  return true;
}

// ── startup message (no type byte, special framing) ───────────────────────────

bool WalSender::RecvStartup(
    std::vector<std::pair<std::string, std::string>>* params) {
  for (;;) {
    uint8_t len_buf[4];
    if (!RecvRaw(len_buf, 4)) return false;
    uint32_t len = GetU32BE(len_buf);
    if (len < 4) return false;

    std::vector<uint8_t> body(len - 4);
    if (!body.empty() && !RecvRaw(body.data(), body.size())) return false;
    if (body.size() < 4) return false;

    uint32_t proto = GetU32BE(body.data());

    if (proto == 80877103) {
      // SSLRequest – decline
      if (!SendRaw("N", 1)) return false;
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

bool WalSender::RecvMessage(char* type, std::vector<uint8_t>* payload) {
  uint8_t hdr[5];
  if (!RecvRaw(hdr, 5)) return false;
  *type = static_cast<char>(hdr[0]);
  uint32_t len = GetU32BE(hdr + 1);
  if (len < 4) return false;
  payload->resize(len - 4);
  if (!payload->empty() && !RecvRaw(payload->data(), payload->size()))
    return false;
  return true;
}

bool WalSender::SendMessage(char type, const std::vector<uint8_t>& payload) {
  uint8_t hdr[5];
  hdr[0] = static_cast<uint8_t>(type);
  uint32_t len = static_cast<uint32_t>(4 + payload.size());
  hdr[1] = (len >> 24) & 0xFF;
  hdr[2] = (len >> 16) & 0xFF;
  hdr[3] = (len >> 8) & 0xFF;
  hdr[4] = len & 0xFF;
  if (!SendRaw(hdr, 5)) return false;
  if (!payload.empty() && !SendRaw(payload.data(), payload.size())) return false;
  return true;
}

bool WalSender::RecvRaw(void* buf, size_t n) {
  auto* p = static_cast<uint8_t*>(buf);
  while (n > 0) {
    ssize_t r = recv(fd_, p, n, 0);
    if (r <= 0) return false;
    p += r;
    n -= static_cast<size_t>(r);
  }
  return true;
}

bool WalSender::SendRaw(const void* buf, size_t n) {
  const auto* p = static_cast<const uint8_t*>(buf);
  while (n > 0) {
#ifdef MSG_NOSIGNAL
    ssize_t w = send(fd_, p, n, MSG_NOSIGNAL);
#else
    ssize_t w = send(fd_, p, n, 0);
#endif
    if (w <= 0) return false;
    p += w;
    n -= static_cast<size_t>(w);
  }
  return true;
}

// ── protocol message builders ─────────────────────────────────────────────────

bool WalSender::SendAuthOk() {
  std::vector<uint8_t> p;
  PutU32BE(p, 0);  // AuthenticationOk
  return SendMessage('R', p);
}

bool WalSender::SendParameterStatus(const char* name, const char* value) {
  std::vector<uint8_t> p;
  PutStr(p, name);
  PutStr(p, value);
  return SendMessage('S', p);
}

bool WalSender::SendBackendKeyData() {
  std::vector<uint8_t> p;
  PutU32BE(p, static_cast<uint32_t>(getpid()));
  PutU32BE(p, 0);  // secret key (cancel not supported)
  return SendMessage('K', p);
}

bool WalSender::SendReadyForQuery() {
  std::vector<uint8_t> p;
  PutU8(p, 'I');  // idle
  return SendMessage('Z', p);
}

bool WalSender::SendRowDescription(
    const std::vector<std::pair<std::string, int32_t>>& cols) {
  std::vector<uint8_t> p;
  PutU16BE(p, static_cast<uint16_t>(cols.size()));
  for (const auto& [name, type_oid] : cols) {
    PutStr(p, name);
    PutU32BE(p, 0);                          // table OID
    PutU16BE(p, 0);                          // column attribute number
    PutU32BE(p, static_cast<uint32_t>(type_oid));
    int16_t sz = (type_oid == 23) ? 4 : (type_oid == 20) ? 8 : -1;
    PutU16BE(p, static_cast<uint16_t>(sz));  // type size
    PutU32BE(p, static_cast<uint32_t>(-1));  // type modifier
    PutU16BE(p, 0);                          // format: text
  }
  return SendMessage('T', p);
}

bool WalSender::SendDataRow(
    const std::vector<std::optional<std::string>>& vals) {
  std::vector<uint8_t> p;
  PutU16BE(p, static_cast<uint16_t>(vals.size()));
  for (const auto& v : vals) {
    if (!v) {
      PutU32BE(p, static_cast<uint32_t>(-1));  // NULL
    } else {
      PutU32BE(p, static_cast<uint32_t>(v->size()));
      p.insert(p.end(), v->begin(), v->end());
    }
  }
  return SendMessage('D', p);
}

bool WalSender::SendCommandComplete(const char* tag) {
  std::vector<uint8_t> p;
  PutStr(p, tag);
  return SendMessage('C', p);
}

bool WalSender::SendCopyBothResponse() {
  std::vector<uint8_t> p;
  PutU8(p, 0);     // overall format: text
  PutU16BE(p, 0);  // number of column format codes
  return SendMessage('W', p);
}

bool WalSender::SendErrorResponse(const char* msg) {
  std::vector<uint8_t> p;
  PutU8(p, 'S');  PutStr(p, "ERROR");
  PutU8(p, 'M');  PutStr(p, msg);
  PutU8(p, 0);    // terminator
  return SendMessage('E', p);
}

// ── encode / decode helpers ───────────────────────────────────────────────────

uint64_t WalSender::ParseLSN(const std::string& s) {
  uint32_t hi = 0, lo = 0;
  sscanf(s.c_str(), "%X/%X", &hi, &lo);
  return (static_cast<uint64_t>(hi) << 32) | lo;
}

std::string WalSender::FormatLSN(uint64_t lsn) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%X/%X",
           static_cast<uint32_t>(lsn >> 32),
           static_cast<uint32_t>(lsn));
  return buf;
}

void WalSender::PutU8(std::vector<uint8_t>& b, uint8_t v) {
  b.push_back(v);
}
void WalSender::PutU16BE(std::vector<uint8_t>& b, uint16_t v) {
  b.push_back((v >> 8) & 0xFF);
  b.push_back(v & 0xFF);
}
void WalSender::PutU32BE(std::vector<uint8_t>& b, uint32_t v) {
  b.push_back((v >> 24) & 0xFF);
  b.push_back((v >> 16) & 0xFF);
  b.push_back((v >> 8) & 0xFF);
  b.push_back(v & 0xFF);
}
void WalSender::PutU64BE(std::vector<uint8_t>& b, uint64_t v) {
  b.push_back((v >> 56) & 0xFF);
  b.push_back((v >> 48) & 0xFF);
  b.push_back((v >> 40) & 0xFF);
  b.push_back((v >> 32) & 0xFF);
  b.push_back((v >> 24) & 0xFF);
  b.push_back((v >> 16) & 0xFF);
  b.push_back((v >> 8) & 0xFF);
  b.push_back(v & 0xFF);
}
void WalSender::PutI64BE(std::vector<uint8_t>& b, int64_t v) {
  PutU64BE(b, static_cast<uint64_t>(v));
}
void WalSender::PutStr(std::vector<uint8_t>& b, const std::string& s) {
  b.insert(b.end(), s.begin(), s.end());
  b.push_back(0);
}
uint32_t WalSender::GetU32BE(const uint8_t* p) {
  return (static_cast<uint32_t>(p[0]) << 24) |
         (static_cast<uint32_t>(p[1]) << 16) |
         (static_cast<uint32_t>(p[2]) << 8) |
          static_cast<uint32_t>(p[3]);
}
uint64_t WalSender::GetU64BE(const uint8_t* p) {
  return (static_cast<uint64_t>(p[0]) << 56) |
         (static_cast<uint64_t>(p[1]) << 48) |
         (static_cast<uint64_t>(p[2]) << 40) |
         (static_cast<uint64_t>(p[3]) << 32) |
         (static_cast<uint64_t>(p[4]) << 24) |
         (static_cast<uint64_t>(p[5]) << 16) |
         (static_cast<uint64_t>(p[6]) << 8) |
          static_cast<uint64_t>(p[7]);
}
int64_t WalSender::NowPgTimestamp() {
  auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
  return us - kPgEpochOffsetUs;
}
