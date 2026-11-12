#pragma once
#include "src/base/base.h"
#include "src/wal_sender/wal_sender.h"

// Async Unix-domain-socket accept loop.  Creates one WalSender per connection
// and calls on_sender with it immediately after start() is called on the sender.
// Every sender reports the standby's StandbyStatusUpdates to status_cb, if set.
//
// start() tries socket_dir/.s.PGSQL.<port>; if that path already exists it
// increments port until a free slot is found.  The port actually bound is
// available via bound_port() after a successful start() and should be used in
// the standby's primary_conninfo='host=<socket_dir> port=<bound_port>'.
class WalSenderServer
    : public std::enable_shared_from_this<WalSenderServer> {
 public:
  using SenderCb = std::function<void(shared_ptr<WalSender>)>;

  static shared_ptr<WalSenderServer> create(
      std::filesystem::path socket_dir,
      uint16_t              port,
      std::filesystem::path wal_dir,
      uint64_t              system_id,
      uint32_t              timeline,
      uint64_t              flush_lsn,
      SenderCb              on_sender,
      WalSender::StatusCb   status_cb        = {},
      uint32_t              wal_segment_size = 16u * 1024u * 1024u);

  // Bind, listen, and start accepting.  Returns false on error.
  bool start();

  // Cancel the acceptor (existing senders keep running) and remove the socket file.
  void shutdown();

  std::filesystem::path socket_path() const { return socket_path_; }
  uint16_t              bound_port() const { return bound_port_; }

 private:
  using unix_proto = asio::local::stream_protocol;

  WalSenderServer(std::filesystem::path socket_dir, uint16_t port,
                  std::filesystem::path wal_dir, uint64_t system_id,
                  uint32_t timeline, uint64_t flush_lsn, SenderCb on_sender,
                  WalSender::StatusCb status_cb, uint32_t wal_segment_size);

  shared_ptr<WalSenderServer> self() { return shared_from_this(); }
  void do_accept();

  std::filesystem::path socket_dir_;
  uint16_t              port_;
  std::filesystem::path socket_path_;
  uint16_t              bound_port_{0};
  std::filesystem::path wal_dir_;
  uint64_t              system_id_;
  uint32_t              timeline_;
  uint64_t              flush_lsn_;
  SenderCb              on_sender_;
  WalSender::StatusCb   status_cb_;
  uint32_t              wal_segment_size_;

  unix_proto::acceptor acceptor_;
  unix_proto::socket   next_socket_;
};
