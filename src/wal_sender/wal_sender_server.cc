#include "src/wal_sender/wal_sender_server.h"

#include "src/base/io_threads.h"
#include "src/base/logger.h"

using error_code = boost::system::error_code;

WalSenderServer::WalSenderServer(
    uint16_t port, std::filesystem::path wal_dir,
    uint64_t system_id, uint32_t timeline, uint64_t flush_lsn,
    SenderCb on_sender, uint32_t wal_segment_size)
    : port_(port),
      wal_dir_(std::move(wal_dir)),
      system_id_(system_id),
      timeline_(timeline),
      flush_lsn_(flush_lsn),
      on_sender_(std::move(on_sender)),
      wal_segment_size_(wal_segment_size),
      acceptor_(io_context()),
      next_socket_(io_context()) {}

shared_ptr<WalSenderServer> WalSenderServer::create(
    uint16_t port, std::filesystem::path wal_dir,
    uint64_t system_id, uint32_t timeline, uint64_t flush_lsn,
    SenderCb on_sender, uint32_t wal_segment_size) {
  return shared_ptr<WalSenderServer>(new WalSenderServer(
      port, std::move(wal_dir), system_id, timeline, flush_lsn,
      std::move(on_sender), wal_segment_size));
}

bool WalSenderServer::start() {
  error_code ec;

  acceptor_.open(tcp::v4(), ec);
  if (ec) {
    log(ERROR, "wal_sender_server: open: {}", ec.message());
    return false;
  }

  acceptor_.set_option(tcp::acceptor::reuse_address(true), ec);
  acceptor_.set_option(tcp::no_delay(true), ec);

  acceptor_.bind(tcp::endpoint(asio::ip::address_v4::any(), port_), ec);
  if (ec) {
    log(ERROR, "wal_sender_server: bind: {}", ec.message());
    return false;
  }

  acceptor_.listen(16, ec);
  if (ec) {
    log(ERROR, "wal_sender_server: listen: {}", ec.message());
    return false;
  }

  bound_port_ = acceptor_.local_endpoint(ec).port();

  do_accept();
  return true;
}

void WalSenderServer::shutdown() {
  error_code ec;
  acceptor_.close(ec);
}

void WalSenderServer::do_accept() {
  acceptor_.async_accept(next_socket_, [this, s = self()](error_code ec) {
    if (ec) return;  // acceptor was closed

    error_code ec2;
    next_socket_.set_option(tcp::no_delay(true), ec2);

    auto sender = WalSender::create(std::move(next_socket_), wal_dir_,
                                    system_id_, timeline_, flush_lsn_,
                                    {}, wal_segment_size_);
    sender->start();
    on_sender_(sender);

    next_socket_ = tcp::socket(io_context());
    do_accept();
  });
}
