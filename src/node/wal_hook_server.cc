#include "src/node/wal_hook_server.h"

#include "src/base/io_threads.h"
#include "src/base/logger.h"

namespace fs = std::filesystem;
using unix_proto = asio::local::stream_protocol;

namespace {

struct Request {
  uint8_t  type;
  uint32_t tli;
  uint64_t segno;
  uint32_t offset;
  uint32_t size;
};

Request decode(std::array<uint8_t, WalHookServer::kRequestSize> const& b) {
  Request r{};
  r.type = b[0];
  std::memcpy(&r.tli, b.data() + 4, 4);
  std::memcpy(&r.segno, b.data() + 8, 8);
  std::memcpy(&r.offset, b.data() + 16, 4);
  std::memcpy(&r.size, b.data() + 20, 4);
  return r;
}

}  // namespace

// One PostgreSQL process's connection: a request at a time.
struct WalHookServer::Conn : public std::enable_shared_from_this<Conn> {
  Conn(shared_ptr<WalHookServer> server, unix_proto::socket socket)
      : server_(std::move(server)), socket_(std::move(socket)) {}

  void read() {
    asio::async_read(socket_,
                     asio::buffer(header_),
                     [self = shared_from_this()](boost::system::error_code ec, size_t) {
                       if (!ec)
                         self->handle();
                     });
  }

  void answer(bool ok) {
    answer_ = ok ? 0 : 1;
    asio::async_write(socket_,
                      asio::buffer(&answer_, 1),
                      [self = shared_from_this()](boost::system::error_code ec, size_t) {
                        if (!ec)
                          self->read();
                      });
  }

  void handle() {
    auto const req = decode(header_);
    auto const lsn =
        LogSeqNumber{.timeline = req.tli, .lsn = req.segno * kWalSegmentSize};
    auto& tracker = server_->tracker_;

    switch (req.type) {
      case 'W': {
        auto seg = server_->segment(lsn);
        if (!seg || req.offset + uint64_t{req.size} > kWalSegmentSize) {
          log(ERROR, "WAL hook: the WAL at {} of {}: no segment", req.offset, lsn);
          return answer(false);
        }
        tracker.async_publish(*seg, static_cast<off_t>(req.offset), req.size);
        return answer(true);
      }
      case 'F': {
        auto seg = server_->segment(lsn);
        if (!seg || req.offset > kWalSegmentSize) {
          log(ERROR, "WAL hook: the flush up to {} of {}: no segment", req.offset, lsn);
          return answer(false);
        }
        // Answered on the io threads: the completion may run with the raft server's lock held.
        tracker.async_fsync(*seg,
                            static_cast<off_t>(req.offset),
                            0,
                            [self = shared_from_this()](Result<void> r) {
                              asio::post(self->socket_.get_executor(),
                                         [self, ok = r.has_value()] { self->answer(ok); });
                            });
        return;
      }
      case 'T': {
        if (req.size > (1U << 20U)) {
          log(ERROR, "WAL hook: a history file of {} bytes", req.size);
          return answer(false);
        }
        content_.resize(req.size);
        asio::async_read(socket_,
                         asio::buffer(content_),
                         [self = shared_from_this(), tli = req.tli](boost::system::error_code ec,
                                                                     size_t) {
                           if (ec)
                             return;
                           self->server_->tracker_.async_timeline(tli, self->content_);
                           self->answer(true);
                         });
        return;
      }
      default:
        log(ERROR, "WAL hook: an unknown request {}", req.type);
        socket_.close();
    }
  }

  void close() {
    boost::system::error_code ec;
    socket_.close(ec);
  }

  shared_ptr<WalHookServer>                       server_;
  unix_proto::socket                              socket_;
  std::array<uint8_t, WalHookServer::kRequestSize> header_{};
  string                                          content_;
  uint8_t                                         answer_ = 0;
};

WalHookServer::WalHookServer(fs::path socket, fs::path pg_wal, WalTracker& tracker)
    : socket_(std::move(socket)),
      pg_wal_(std::move(pg_wal)),
      tracker_(tracker),
      cache_(make_shared<WalSegmentCache>()),
      acceptor_(io_context()) {}

WalHookServer::~WalHookServer() {
  stop();
}

Result<void> WalHookServer::start() {
  boost::system::error_code ec;
  fs::remove(socket_);  // a previous run's
  acceptor_.open(unix_proto(), ec);
  if (!ec)
    acceptor_.bind(unix_proto::endpoint(socket_.string()), ec);
  if (!ec)
    acceptor_.listen(64, ec);
  if (ec)
    return elog(system_error_code(ec.value()), "WAL hook: unable to listen on {}", socket_);
  log(INFO, "WAL hook: listening on {}", socket_);
  accept();
  return ok();
}

void WalHookServer::accept() {
  acceptor_.async_accept([self = shared_from_this()](boost::system::error_code ec,
                                                     unix_proto::socket socket) {
    if (ec)
      return;  // closed
    auto conn = make_shared<Conn>(self, std::move(socket));
    {
      auto lk = std::unique_lock{self->conns_mtx_};
      std::erase_if(self->conns_, [](auto const& w) { return w.expired(); });
      self->conns_.push_back(conn);
    }
    conn->read();
    self->accept();
  });
}

void WalHookServer::stop() {
  boost::system::error_code ec;
  if (acceptor_.is_open()) {
    acceptor_.close(ec);
    fs::remove(socket_, ec);
  }
  auto lk = std::unique_lock{conns_mtx_};
  for (auto const& w : conns_)
    if (auto c = w.lock())
      asio::post(c->socket_.get_executor(), [c] { c->close(); });
  conns_.clear();
}

void WalHookServer::reset_segments() {
  auto lk = std::unique_lock{mtx_};
  segments_.clear();
  cache_ = make_shared<WalSegmentCache>();  // no mapping of a replaced file is reused
}

Result<shared_ptr<WalSegment>> WalHookServer::segment(LogSeqNumber lsn) {
  auto lk = std::unique_lock{mtx_};
  if (auto pos = segments_.find(lsn); pos != segments_.end())
    return pos->second;
  auto seg = make_shared<WalSegment>(cache_, pg_wal_, lsn);
  if (auto r = seg->open(); !r)
    return r.error();
  segments_.emplace(lsn, seg);
  while (segments_.size() > 8)
    segments_.erase(segments_.begin());  // the oldest (the WAL moves forward)
  return seg;
}
