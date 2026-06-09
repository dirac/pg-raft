#include "src/status/pg_client.h"

#include "src/base/io_threads.h"
#include "src/base/logger.h"

using error_code = boost::system::error_code;
using tcp        = asio::ip::tcp;

// ---- wire encoding/decoding ----

namespace {

void put16(Bytes& b, uint16_t v) {
  v = htons(v);
  b.insert(b.end(), reinterpret_cast<uint8_t*>(&v), reinterpret_cast<uint8_t*>(&v) + 2);
}

void put32(Bytes& b, uint32_t v) {
  v = htonl(v);
  b.insert(b.end(), reinterpret_cast<uint8_t*>(&v), reinterpret_cast<uint8_t*>(&v) + 4);
}

void put_str(Bytes& b, string const& s) {
  b.insert(b.end(), s.begin(), s.end());
  b.push_back(0);
}

uint16_t get16(uint8_t const* p) {
  uint16_t v;
  memcpy(&v, p, 2);
  return ntohs(v);
}

uint32_t get32(uint8_t const* p) {
  uint32_t v;
  memcpy(&v, p, 4);
  return ntohl(v);
}

int32_t geti32(uint8_t const* p) {
  uint32_t v;
  memcpy(&v, p, 4);
  return static_cast<int32_t>(ntohl(v));
}

// Append a framed message (type byte + big-endian length + body) to dst.
void append_msg(Bytes& dst, char type, Bytes const& body) {
  dst.push_back(static_cast<uint8_t>(type));
  put32(dst, static_cast<uint32_t>(body.size()) + 4u);
  dst.insert(dst.end(), body.begin(), body.end());
}

// Build the startup packet (no type byte — length is the first field).
Bytes make_startup(string const& user) {
  Bytes body;
  put32(body, 0x00030000u);  // protocol 3.0
  put_str(body, "user");
  put_str(body, user);
  put_str(body, "database");
  put_str(body, "postgres");  // always connect to the default maintenance database
  body.push_back(0);          // terminator

  Bytes out;
  put32(out, static_cast<uint32_t>(body.size()) + 4u);
  out.insert(out.end(), body.begin(), body.end());
  return out;
}

// Extract the human-readable message ('M' field) from an ErrorResponse payload.
string pg_error_msg(Bytes const& payload) {
  size_t i = 0;
  while (i < payload.size() && payload[i] != 0) {
    char   field = static_cast<char>(payload[i++]);
    string val;
    while (i < payload.size() && payload[i] != 0)
      val += static_cast<char>(payload[i++]);
    if (i < payload.size())
      ++i;
    if (field == 'M')
      return val;
  }
  return "unknown PostgreSQL error";
}

}  // namespace

// ---- PgClient ----

PgClient::PgClient(tcp::socket socket) : socket_(std::move(socket)), strand_(io_context()) {}

// ---- synchronous I/O used during connect() and prepare() ----

Result<void> PgClient::sync_write(Bytes const& buf) {
  error_code ec;
  asio::write(socket_, asio::buffer(buf), ec);
  return ec ? Result<void>(ec) : ok();
}

Result<std::pair<char, Bytes>> PgClient::sync_read_msg() {
  error_code ec;
  asio::read(socket_, asio::buffer(hdr_.data(), 5), ec);
  if (ec)
    return ec;

  char     type = static_cast<char>(hdr_[0]);
  uint32_t len  = get32(hdr_.data() + 1);
  if (len < 4)
    return std::make_error_code(std::errc::protocol_error);

  Bytes body(len - 4);
  if (!body.empty()) {
    asio::read(socket_, asio::buffer(body), ec);
    if (ec)
      return ec;
  }
  return std::make_pair(type, std::move(body));
}

// Consume server messages until ReadyForQuery ('Z'); fail on auth or error responses.
Result<void> PgClient::sync_drain() {
  for (;;) {
    auto r = sync_read_msg();
    if (!r)
      return elog(r.error(), "Error reading the next packet");
    auto [type, body] = std::move(*r);

    if (type == 'Z')
      return ok();

    if (type == 'E') {
      log(ERROR, "pg: {}", pg_error_msg(body));
      return std::make_error_code(std::errc::protocol_error);
    }

    if (type == 'R' && (body.size() < 4 || get32(body.data()) != 0)) {
      log(ERROR, "pg: authentication required");
      return std::make_error_code(std::errc::permission_denied);
    }
  }
}

// ---- public API ----

Result<shared_ptr<PgClient>> PgClient::connect(uint16_t port) {
  error_code  ec;
  tcp::socket socket(io_context());
  socket.connect(tcp::endpoint(asio::ip::address_v4::loopback(), port), ec);
  if (ec)
    return elog(ec, "Connect failed for port={}", port);
  socket.set_option(tcp::no_delay(true), ec);

  auto client = shared_ptr<PgClient>(new PgClient(std::move(socket)));

  char const* user = getenv("USER");
  if (auto r = client->sync_write(make_startup(user ? user : "postgres")); !r)
    return elog(r.error(), "startup failed");
  if (auto r = client->sync_drain(); !r)
    return elog(r.error(), "failure waiting for ReadyForQuery message");

  return client;
}

Result<PgStmt> PgClient::prepare(string const& sql) {
  string name = to_string(next_id_++);

  // Parse ('P') + Describe ('D') + Sync ('S')
  Bytes parse_body;
  put_str(parse_body, name);
  put_str(parse_body, sql);
  put16(parse_body, uint16_t{0});  // no parameter type OIDs

  Bytes desc_body;
  desc_body.push_back(static_cast<uint8_t>('S'));
  put_str(desc_body, name);

  Bytes buf;
  append_msg(buf, 'P', parse_body);
  append_msg(buf, 'D', desc_body);
  append_msg(buf, 'S', {});

  if (auto r = sync_write(buf); !r)
    return r.error();

  for (;;) {
    auto mr = sync_read_msg();
    if (!mr)
      return mr.error();
    auto [type, body] = std::move(*mr);

    switch (type) {
      case '1':  // ParseComplete
      case 't':  // ParameterDescription
      case 'T':  // RowDescription
      case 'n':  // NoData
        continue;
      case 'Z':  // ReadyForQuery
        return PgStmt{std::move(name)};
      case 'E':
        log(ERROR, "pg prepare: {}", pg_error_msg(body));
        return std::make_error_code(std::errc::invalid_argument);
      default:
        continue;
    }
  }
}

void PgClient::close() {
  Bytes term;
  append_msg(term, 'X', {});
  if (auto r = sync_write(term); !r)
    log(DEBUG, "pg close: {}", r.error().message());

  error_code ec;
  socket_.close(ec);
}

// ---- async exec ----

void PgClient::async_exec(PgStmt const& stmt, std::function<void(Result<Rows>)> cb) {
  enqueue({stmt.name, std::move(cb)});
}

void PgClient::enqueue(Req r) {
  std::unique_lock lk(mu_);
  queue_.push_back(std::move(r));
  if (!running_) {
    running_ = true;
    lk.unlock();
    // Post to the strand so start_exec runs in the async context.
    asio::post(strand_, [this, s = shared_from_this()]() mutable { start_exec(std::move(s)); });
  }
}

void PgClient::start_exec(shared_ptr<PgClient> self) {
  Req req;
  {
    std::unique_lock lk(mu_);
    req = std::move(queue_.front());
    queue_.pop_front();
  }

  exec_cb_     = std::move(req.cb);
  exec_failed_ = false;
  rows_.clear();

  // Build Bind ('B') + Execute ('E') + Sync ('S') into send_buf_.
  send_buf_.clear();

  Bytes bind_body;
  put_str(bind_body, "");         // portal name (unnamed)
  put_str(bind_body, req.name);   // statement name
  put16(bind_body, uint16_t{0});  // no parameter format codes
  put16(bind_body, uint16_t{0});  // no parameters
  put16(bind_body, uint16_t{0});  // no result-column format codes (text)
  append_msg(send_buf_, 'B', bind_body);

  Bytes exec_body;
  put_str(exec_body, "");         // portal name
  put32(exec_body, uint32_t{0});  // max rows: unlimited
  append_msg(send_buf_, 'E', exec_body);

  append_msg(send_buf_, 'S', {});

  do_send(std::move(self));
}

void PgClient::do_send(shared_ptr<PgClient> self) {
  auto handler = [this, s = std::move(self)](error_code ec, size_t) mutable {
    if (ec) {
      finish_exec(std::move(s), ec);
      return;
    }
    do_read_hdr(std::move(s));
  };
  asio::async_write(
      socket_, asio::buffer(send_buf_), asio::bind_executor(strand_, std::move(handler)));
}

void PgClient::do_read_hdr(shared_ptr<PgClient> self) {
  auto handler = [this, s = std::move(self)](error_code ec, size_t) mutable {
    if (ec) {
      finish_exec(std::move(s), ec);
      return;
    }
    char     type = static_cast<char>(hdr_[0]);
    uint32_t len  = get32(hdr_.data() + 1);
    if (len < 4) {
      finish_exec(std::move(s), std::make_error_code(std::errc::protocol_error));
      return;
    }
    do_read_body(std::move(s), type, len - 4);
  };
  asio::async_read(
      socket_, asio::buffer(hdr_.data(), 5), asio::bind_executor(strand_, std::move(handler)));
}

void PgClient::do_read_body(shared_ptr<PgClient> self, char type, uint32_t body_len) {
  body_.resize(body_len);
  if (body_len == 0) {
    on_msg(std::move(self), type);
    return;
  }
  auto handler = [this, s = std::move(self), type](error_code ec, size_t) mutable {
    if (ec) {
      finish_exec(std::move(s), ec);
      return;
    }
    on_msg(std::move(s), type);
  };
  asio::async_read(socket_, asio::buffer(body_), asio::bind_executor(strand_, std::move(handler)));
}

void PgClient::on_msg(shared_ptr<PgClient> self, char type) {
  switch (type) {
    case '2':  // BindComplete
    case 'C':  // CommandComplete
      do_read_hdr(std::move(self));
      break;

    case 'D': {  // DataRow
      if (body_.size() < 2) {
        finish_exec(std::move(self), std::make_error_code(std::errc::protocol_error));
        return;
      }
      uint16_t ncols = get16(body_.data());
      Row      row;
      row.reserve(ncols);
      size_t off = 2;
      for (uint16_t c = 0; c < ncols; ++c) {
        if (off + 4 > body_.size()) {
          finish_exec(std::move(self), std::make_error_code(std::errc::protocol_error));
          return;
        }
        int32_t clen = geti32(body_.data() + off);
        off += 4;
        if (clen < 0) {
          row.push_back(std::nullopt);
        } else {
          size_t ulen = static_cast<size_t>(clen);
          if (off + ulen > body_.size()) {
            finish_exec(std::move(self), std::make_error_code(std::errc::protocol_error));
            return;
          }
          row.emplace_back(string(reinterpret_cast<char const*>(body_.data() + off), ulen));
          off += ulen;
        }
      }
      rows_.push_back(std::move(row));
      do_read_hdr(std::move(self));
      break;
    }

    case 'E':  // ErrorResponse — continue draining until ReadyForQuery
      log(ERROR, "pg exec: {}", pg_error_msg(body_));
      exec_failed_ = true;
      do_read_hdr(std::move(self));
      break;

    case 'Z':  // ReadyForQuery — exec cycle complete
      if (exec_failed_)
        finish_exec(std::move(self), std::make_error_code(std::errc::io_error));
      else
        finish_exec(std::move(self), std::move(rows_));
      break;

    default:
      do_read_hdr(std::move(self));
      break;
  }
}

void PgClient::finish_exec(shared_ptr<PgClient> self, Result<Rows> result) {
  auto cb = std::move(exec_cb_);
  rows_.clear();

  bool more;
  {
    std::unique_lock lk(mu_);
    more = !queue_.empty();
    if (!more)
      running_ = false;
  }

  cb(std::move(result));

  if (more)
    start_exec(std::move(self));
}
