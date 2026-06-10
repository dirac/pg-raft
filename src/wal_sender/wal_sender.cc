#include "src/wal_sender/wal_sender.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <unistd.h>

#include "src/base/io_threads.h"
#include "src/base/logger.h"

using error_code = boost::system::error_code;

static constexpr int64_t kPgEpochUs       = 946684800LL * 1000000LL;
static constexpr int     kKeepaliveSeconds = 5;

// ── wire helpers ──────────────────────────────────────────────────────────────

void WalSender::put16(Bytes& b, uint16_t v) {
  v = htons(v);
  b.insert(b.end(), reinterpret_cast<uint8_t*>(&v),
           reinterpret_cast<uint8_t*>(&v) + 2);
}

void WalSender::put32(Bytes& b, uint32_t v) {
  v = htonl(v);
  b.insert(b.end(), reinterpret_cast<uint8_t*>(&v),
           reinterpret_cast<uint8_t*>(&v) + 4);
}

void WalSender::put64(Bytes& b, uint64_t v) {
  put32(b, static_cast<uint32_t>(v >> 32));
  put32(b, static_cast<uint32_t>(v & 0xFFFFFFFFu));
}

void WalSender::put_str(Bytes& b, string const& s) {
  b.insert(b.end(), s.begin(), s.end());
  b.push_back(0);
}

uint32_t WalSender::get32(uint8_t const* p) {
  uint32_t v;
  memcpy(&v, p, 4);
  return ntohl(v);
}

uint64_t WalSender::get64(uint8_t const* p) {
  return (static_cast<uint64_t>(get32(p)) << 32) | get32(p + 4);
}

// ── protocol message builders ─────────────────────────────────────────────────

Bytes WalSender::build_msg(char type, Bytes body) {
  Bytes out;
  out.push_back(static_cast<uint8_t>(type));
  put32(out, static_cast<uint32_t>(4 + body.size()));
  out.insert(out.end(), body.begin(), body.end());
  return out;
}

Bytes WalSender::auth_ok() {
  Bytes b;
  put32(b, 0u);
  return build_msg('R', std::move(b));
}

Bytes WalSender::param_status(char const* name, char const* value) {
  Bytes b;
  put_str(b, name);
  put_str(b, value);
  return build_msg('S', std::move(b));
}

Bytes WalSender::backend_key_data() {
  Bytes b;
  put32(b, static_cast<uint32_t>(getpid()));
  put32(b, 0u);
  return build_msg('K', std::move(b));
}

Bytes WalSender::ready_for_query() {
  Bytes b;
  b.push_back('I');
  return build_msg('Z', std::move(b));
}

Bytes WalSender::row_desc(vector<std::pair<string, int32_t>> const& cols) {
  Bytes b;
  put16(b, static_cast<uint16_t>(cols.size()));
  for (auto const& [name, oid] : cols) {
    put_str(b, name);
    put32(b, 0u);                          // table oid
    put16(b, 0u);                          // col attribute number
    put32(b, static_cast<uint32_t>(oid));
    int16_t sz = (oid == 23) ? 4 : (oid == 20) ? 8 : -1;
    put16(b, static_cast<uint16_t>(sz));   // type size (-1 = variable)
    put32(b, 0xFFFFFFFFu);                 // type modifier
    put16(b, 0u);                          // format: text
  }
  return build_msg('T', std::move(b));
}

Bytes WalSender::data_row(vector<std::optional<string>> const& vals) {
  Bytes b;
  put16(b, static_cast<uint16_t>(vals.size()));
  for (auto const& v : vals) {
    if (!v) {
      put32(b, 0xFFFFFFFFu);  // NULL
    } else {
      put32(b, static_cast<uint32_t>(v->size()));
      b.insert(b.end(), v->begin(), v->end());
    }
  }
  return build_msg('D', std::move(b));
}

Bytes WalSender::command_complete(char const* tag) {
  Bytes b;
  put_str(b, tag);
  return build_msg('C', std::move(b));
}

Bytes WalSender::copy_both_response() {
  Bytes b;
  put8(b, 0);    // overall format: text
  put16(b, 0u);  // no column format codes
  return build_msg('W', std::move(b));
}

Bytes WalSender::error_response(char const* m) {
  Bytes b;
  put8(b, 'S');
  put_str(b, "ERROR");
  put8(b, 'M');
  put_str(b, m);
  put8(b, 0);
  return build_msg('E', std::move(b));
}

Bytes WalSender::xlog_data(uint64_t lsn, Bytes const& data) const {
  Bytes payload;
  put8(payload, 'w');
  put64(payload, lsn);
  put64(payload, end_lsn_);
  put64(payload, static_cast<uint64_t>(now_pg_ts()));
  payload.insert(payload.end(), data.begin(), data.end());
  return build_msg('d', std::move(payload));
}

Bytes WalSender::keepalive_msg() const {
  Bytes payload;
  put8(payload, 'k');
  put64(payload, end_lsn_);
  put64(payload, static_cast<uint64_t>(now_pg_ts()));
  put8(payload, 0);  // reply not requested
  return build_msg('d', std::move(payload));
}

uint64_t WalSender::parse_lsn(string const& s) {
  uint32_t hi = 0, lo = 0;
  sscanf(s.c_str(), "%X/%X", &hi, &lo);
  return (static_cast<uint64_t>(hi) << 32) | lo;
}

string WalSender::format_lsn(uint64_t lsn) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%X/%X",
           static_cast<uint32_t>(lsn >> 32),
           static_cast<uint32_t>(lsn));
  return buf;
}

int64_t WalSender::now_pg_ts() {
  auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
  return us - kPgEpochUs;
}

// ── construction ──────────────────────────────────────────────────────────────

WalSender::WalSender(stream_socket socket, std::filesystem::path wal_dir,
                     uint64_t system_id, uint32_t timeline,
                     uint64_t flush_lsn, StatusCb status_cb,
                     uint32_t wal_segment_size)
    : socket_(std::move(socket)),
      strand_(io_context()),
      wal_dir_(std::move(wal_dir)),
      system_id_(system_id),
      timeline_(timeline),
      flush_lsn_(flush_lsn),
      end_lsn_(flush_lsn),
      wal_segment_size_(wal_segment_size),
      status_cb_(std::move(status_cb)),
      keepalive_timer_(io_context()) {}

shared_ptr<WalSender> WalSender::create(
    stream_socket socket, std::filesystem::path wal_dir,
    uint64_t system_id, uint32_t timeline, uint64_t flush_lsn,
    StatusCb status_cb, uint32_t wal_segment_size) {
  return shared_ptr<WalSender>(new WalSender(
      std::move(socket), std::move(wal_dir), system_id, timeline, flush_lsn,
      std::move(status_cb), wal_segment_size));
}

void WalSender::start() {
  asio::post(strand_, [s = self()]() { s->do_read_startup_len(); });
}

void WalSender::advance(uint64_t flush_lsn) {
  asio::post(strand_, [this, s = self(), flush_lsn]() { do_advance(flush_lsn); });
}

void WalSender::shutdown() {
  asio::post(strand_, [this, s = self()]() {
    if (shutdown_) return;
    shutdown_ = true;
    keepalive_timer_.cancel();
    close_socket();
  });
}

void WalSender::close_socket() {
  error_code ec;
  socket_.close(ec);
}

// ── startup read chain ────────────────────────────────────────────────────────

void WalSender::do_read_startup_len() {
  asio::async_read(
      socket_, asio::buffer(startup_len_buf_),
      asio::bind_executor(strand_, [this, s = self()](error_code ec, size_t) {
        if (ec || shutdown_) return;
        uint32_t total_len = get32(startup_len_buf_.data());
        if (total_len < 4) {
          close_socket();
          return;
        }
        do_read_startup_body(total_len);
      }));
}

void WalSender::do_read_startup_body(uint32_t total_len) {
  uint32_t body_len = total_len - 4;
  body_buf_.resize(body_len);
  asio::async_read(
      socket_, asio::buffer(body_buf_),
      asio::bind_executor(strand_, [this, s = self()](error_code ec, size_t) {
        if (ec || shutdown_) return;
        if (body_buf_.size() < 4) {
          close_socket();
          return;
        }
        uint32_t proto = get32(body_buf_.data());
        if (proto == 80877103u) {
          // SSLRequest — decline and restart
          enqueue_send(Bytes{'N'});
          do_read_startup_len();
          return;
        }
        if (proto == 80877102u) {
          // CancelRequest — not supported
          close_socket();
          return;
        }
        handle_startup_body(std::move(body_buf_));
      }));
}

void WalSender::handle_startup_body(Bytes /*body*/) {
  // Build and send: AuthOk + ParameterStatus×3 + BackendKeyData + ReadyForQuery
  Bytes resp;
  auto app = [&resp](Bytes b) { resp.insert(resp.end(), b.begin(), b.end()); };
  app(auth_ok());
  app(param_status("server_version", "17.0"));
  app(param_status("integer_datetimes", "on"));
  app(param_status("server_encoding", "UTF8"));
  app(backend_key_data());
  app(ready_for_query());
  enqueue_send(std::move(resp));
  phase_ = Phase::kCommand;
  do_read_msg_hdr();
}

// ── command loop ──────────────────────────────────────────────────────────────

void WalSender::do_read_msg_hdr() {
  asio::async_read(
      socket_, asio::buffer(hdr_buf_),
      asio::bind_executor(strand_, [this, s = self()](error_code ec, size_t) {
        if (ec || shutdown_) return;
        char     type    = static_cast<char>(hdr_buf_[0]);
        uint32_t msg_len = get32(hdr_buf_.data() + 1);
        if (msg_len < 4) {
          close_socket();
          return;
        }
        do_read_msg_body(type, msg_len - 4);
      }));
}

void WalSender::do_read_msg_body(char type, uint32_t body_len) {
  body_buf_.resize(body_len);
  if (body_len == 0) {
    handle_msg(type, {});
    return;
  }
  asio::async_read(
      socket_, asio::buffer(body_buf_),
      asio::bind_executor(strand_, [this, s = self(), type](error_code ec, size_t) {
        if (ec || shutdown_) return;
        handle_msg(type, std::move(body_buf_));
      }));
}

void WalSender::handle_msg(char type, Bytes body) {
  if (type == 'X') {
    close_socket();
    return;
  }
  if (type != 'Q') {
    Bytes resp;
    auto app = [&resp](Bytes b) { resp.insert(resp.end(), b.begin(), b.end()); };
    app(error_response("expected simple query"));
    app(ready_for_query());
    enqueue_send(std::move(resp));
    do_read_msg_hdr();
    return;
  }

  string query(reinterpret_cast<char const*>(body.data()), body.size());
  while (!query.empty() && query.back() == '\0')
    query.pop_back();

  log(INFO, "From standby: {}", query);
  string upper = query;
  for (char& c : upper)
    c = static_cast<char>(toupper(static_cast<unsigned char>(c)));

  Bytes resp;
  auto app = [&resp](Bytes b) { resp.insert(resp.end(), b.begin(), b.end()); };

  if (upper.find("IDENTIFY_SYSTEM") != string::npos) {
    app(row_desc({{"systemid", 25}, {"timeline", 23}, {"xlogpos", 25}, {"dbname", 25}}));
    app(data_row({std::to_string(system_id_), std::to_string(timeline_),
                  format_lsn(end_lsn_), std::nullopt}));
    app(command_complete("SELECT 1"));
    app(ready_for_query());
    enqueue_send(std::move(resp));
    do_read_msg_hdr();
  } else if (upper.find("START_REPLICATION") != string::npos) {
    // Parse the LSN token (hex/hex) from the command.
    uint64_t start_lsn = 0;
    size_t   pos       = 0;
    while (pos < upper.size()) {
      size_t slash = upper.find('/', pos);
      if (slash == string::npos) break;
      size_t tok_start = slash;
      while (tok_start > 0 && isxdigit(static_cast<unsigned char>(upper[tok_start - 1])))
        --tok_start;
      size_t tok_end = slash + 1;
      while (tok_end < upper.size() && isxdigit(static_cast<unsigned char>(upper[tok_end])))
        ++tok_end;
      if (tok_start < slash && tok_end > slash + 1) {
        start_lsn = parse_lsn(query.substr(tok_start, tok_end - tok_start));
        break;
      }
      pos = slash + 1;
    }
    sent_lsn_ = start_lsn;
    enqueue_send(copy_both_response());
    phase_ = Phase::kStreaming;
    do_read_copy_hdr();
    schedule_keepalive();
    do_advance(flush_lsn_);
  } else if (upper.find("TIMELINE_HISTORY") != string::npos) {
    app(row_desc({{"filename", 25}, {"content", 25}}));
    app(command_complete("TIMELINE_HISTORY 1"));
    app(ready_for_query());
    enqueue_send(std::move(resp));
    do_read_msg_hdr();
  } else if (upper.rfind("SET ", 0) == 0) {
    app(command_complete("SET"));
    app(ready_for_query());
    enqueue_send(std::move(resp));
    do_read_msg_hdr();
  } else if (upper.rfind("SHOW ", 0) == 0) {
    string param = query.substr(5);
    while (!param.empty() &&
           (isspace(static_cast<unsigned char>(param.back())) || param.back() == ';'))
      param.pop_back();
    string value =
        (param == "wal_segment_size") ? std::to_string(wal_segment_size_) : "";
    app(row_desc({{param, 25}}));
    app(data_row({{value}}));
    app(command_complete("SHOW"));
    app(ready_for_query());
    enqueue_send(std::move(resp));
    do_read_msg_hdr();
  } else if (upper.rfind("SELECT ", 0) == 0) {
    app(row_desc({{"?column?", 25}}));
    app(data_row({{string("")}}));
    app(command_complete("SELECT 1"));
    app(ready_for_query());
    enqueue_send(std::move(resp));
    do_read_msg_hdr();
  } else {
    app(error_response("unrecognized replication command"));
    app(ready_for_query());
    enqueue_send(std::move(resp));
    do_read_msg_hdr();
  }
}

// ── streaming read chain ──────────────────────────────────────────────────────

void WalSender::do_read_copy_hdr() {
  asio::async_read(
      socket_, asio::buffer(hdr_buf_),
      asio::bind_executor(strand_, [this, s = self()](error_code ec, size_t) {
        if (ec || shutdown_) return;
        char     type    = static_cast<char>(hdr_buf_[0]);
        uint32_t msg_len = get32(hdr_buf_.data() + 1);
        if (msg_len < 4) {
          close_socket();
          return;
        }
        do_read_copy_body(type, msg_len - 4);
      }));
}

void WalSender::do_read_copy_body(char type, uint32_t body_len) {
  body_buf_.resize(body_len);
  if (body_len == 0) {
    handle_copy_msg(type, {});
    return;
  }
  asio::async_read(
      socket_, asio::buffer(body_buf_),
      asio::bind_executor(strand_, [this, s = self(), type](error_code ec, size_t) {
        if (ec || shutdown_) return;
        handle_copy_msg(type, body_buf_);
      }));
}

void WalSender::handle_copy_msg(char type, Bytes const& body) {
  switch (type) {
    case 'd': {  // CopyData from standby
      if (!body.empty() && body[0] == 'r' && body.size() >= 34) {
        // StandbyStatusUpdate
        StandbyStatus st;
        st.written_lsn  = get64(body.data() + 1);
        st.flushed_lsn  = get64(body.data() + 9);
        st.applied_lsn  = get64(body.data() + 17);
        bool wants_reply = (body[33] != 0);
        if (status_cb_) status_cb_(st);
        if (wants_reply) enqueue_send(keepalive_msg());
      }
      // 'h' HotStandbyFeedback: silently ignored.
      do_read_copy_hdr();
      break;
    }
    case 'c':  // CopyDone
    case 'X':  // Terminate
      close_socket();
      break;
    default:
      do_read_copy_hdr();
      break;
  }
}

// ── write side ────────────────────────────────────────────────────────────────

void WalSender::enqueue_send(Bytes buf) {
  send_queue_.push_back(std::move(buf));
  drain_sends();
}

void WalSender::drain_sends() {
  if (writing_ || send_queue_.empty()) return;
  writing_    = true;
  writing_buf_ = std::move(send_queue_.front());
  send_queue_.pop_front();
  asio::async_write(
      socket_, asio::buffer(writing_buf_),
      asio::bind_executor(strand_, [this, s = self()](error_code ec, size_t) {
        writing_ = false;
        if (ec || shutdown_) return;
        drain_sends();
      }));
}

// ── WAL reading ───────────────────────────────────────────────────────────────

void WalSender::do_advance(uint64_t new_flush) {
  if (new_flush > flush_lsn_) {
    flush_lsn_ = new_flush;
    end_lsn_   = std::max(end_lsn_, new_flush);
  }
  if (phase_ != Phase::kStreaming || sent_lsn_ >= flush_lsn_) return;

  uint64_t pos = sent_lsn_;
  while (pos < flush_lsn_) {
    uint64_t seg_num   = pos / wal_segment_size_;
    uint64_t seg_start = seg_num * wal_segment_size_;
    uint64_t seg_end   = seg_start + wal_segment_size_;
    uint64_t read_end  = std::min(seg_end, flush_lsn_);

    uint32_t seghi = static_cast<uint32_t>(seg_num >> 32);
    uint32_t seglo = static_cast<uint32_t>(seg_num & 0xFFFFFFFFu);

    char name[32];
    snprintf(name, sizeof(name), "%08X%08X%08X", timeline_, seghi, seglo);
    auto fpath = wal_dir_ / name;

    int fd = open(fpath.c_str(), O_RDONLY);
    if (fd < 0) {
      log(DEBUG, "wal_sender: cannot open {}: {}", fpath.string(), strerror(errno));
      break;
    }

    size_t  length   = static_cast<size_t>(read_end - pos);
    off_t   file_off = static_cast<off_t>(pos - seg_start);
    Bytes   data(length);
    ssize_t n = pread(fd, data.data(), length, file_off);
    close(fd);

    if (n <= 0) break;
    data.resize(static_cast<size_t>(n));

    uint64_t chunk_lsn = pos;
    pos += static_cast<uint64_t>(n);
    sent_lsn_ = pos;
    end_lsn_  = std::max(end_lsn_, pos);

    enqueue_send(xlog_data(chunk_lsn, data));
  }
}

// ── keepalive ─────────────────────────────────────────────────────────────────

void WalSender::schedule_keepalive() {
  keepalive_timer_.expires_after(std::chrono::seconds(kKeepaliveSeconds));
  keepalive_timer_.async_wait(
      asio::bind_executor(strand_, [this, s = self()](error_code ec) {
        if (ec || shutdown_) return;
        enqueue_send(keepalive_msg());
        schedule_keepalive();
      }));
}
