#include "src/wal_sender/wal_sender_server.h"

#include "src/base/io_threads.h"
#include "src/base/logger.h"

using error_code = boost::system::error_code;

WalSenderServer::WalSenderServer(
    std::filesystem::path socket_dir, uint16_t port,
    std::filesystem::path wal_dir, uint64_t system_id,
    uint32_t timeline, uint64_t flush_lsn,
    SenderCb on_sender, WalSender::StatusCb status_cb, uint32_t wal_segment_size)
    : socket_dir_(std::move(socket_dir)),
      port_(port),
      wal_dir_(std::move(wal_dir)),
      system_id_(system_id),
      timeline_(timeline),
      flush_lsn_(flush_lsn),
      on_sender_(std::move(on_sender)),
      status_cb_(std::move(status_cb)),
      wal_segment_size_(wal_segment_size),
      acceptor_(io_context()),
      next_socket_(io_context()) {}

shared_ptr<WalSenderServer> WalSenderServer::create(
    std::filesystem::path socket_dir, uint16_t port,
    std::filesystem::path wal_dir, uint64_t system_id,
    uint32_t timeline, uint64_t flush_lsn,
    SenderCb on_sender, WalSender::StatusCb status_cb, uint32_t wal_segment_size) {
  return shared_ptr<WalSenderServer>(new WalSenderServer(
      std::move(socket_dir), port, std::move(wal_dir), system_id, timeline,
      flush_lsn, std::move(on_sender), std::move(status_cb), wal_segment_size));
}

bool WalSenderServer::start() {
  error_code ec;

  for (uint16_t try_port = port_; ; ++try_port) {
    auto candidate = socket_dir_ / (".s.PGSQL." + std::to_string(try_port));
    if (std::filesystem::exists(candidate))
      continue;

    acceptor_.open(unix_proto(), ec);
    if (ec) {
      log(ERROR, "wal_sender_server: open: {}", ec.message());
      return false;
    }

    acceptor_.bind(unix_proto::endpoint(candidate.string()), ec);
    if (!ec) {
      socket_path_ = candidate;
      bound_port_  = try_port;
      break;
    }

    acceptor_.close(ec);
  }

  acceptor_.listen(16, ec);
  if (ec) {
    log(ERROR, "wal_sender_server: listen: {}", ec.message());
    return false;
  }

  do_accept();
  return true;
}

void WalSenderServer::shutdown() {
  error_code ec;
  acceptor_.close(ec);
  std::filesystem::remove(socket_path_, ec);
}

void WalSenderServer::do_accept() {
  acceptor_.async_accept(next_socket_, [this, s = self()](error_code ec) {
    if (ec) return;  // acceptor was closed

    auto sender = WalSender::create(std::move(next_socket_), wal_dir_,
                                    system_id_, timeline_, flush_lsn_,
                                    status_cb_, wal_segment_size_);
    sender->start();
    on_sender_(sender);

    next_socket_ = unix_proto::socket(io_context());
    do_accept();
  });
}
