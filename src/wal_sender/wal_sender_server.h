#pragma once
#include "src/base/base.h"
#include "src/wal_sender/wal_sender.h"

// Asio-based TCP accept loop.  Creates one WalSender per connection and
// calls on_sender with it immediately after start() is called on the sender.
class WalSenderServer
    : public std::enable_shared_from_this<WalSenderServer> {
 public:
  using SenderCb = std::function<void(shared_ptr<WalSender>)>;

  static shared_ptr<WalSenderServer> create(
      uint16_t              port,
      std::filesystem::path wal_dir,
      uint64_t              system_id,
      uint32_t              timeline,
      uint64_t              flush_lsn,
      SenderCb              on_sender,
      uint32_t              wal_segment_size = 16u * 1024u * 1024u);

  // Bind, listen, and start accepting.  Returns false on error.
  bool start();

  // Cancel the acceptor (existing senders keep running).
  void shutdown();

  uint16_t bound_port() const { return bound_port_; }

 private:
  using tcp = asio::ip::tcp;

  WalSenderServer(uint16_t port, std::filesystem::path wal_dir,
                  uint64_t system_id, uint32_t timeline, uint64_t flush_lsn,
                  SenderCb on_sender, uint32_t wal_segment_size);

  shared_ptr<WalSenderServer> self() { return shared_from_this(); }
  void do_accept();

  uint16_t              port_;
  std::filesystem::path wal_dir_;
  uint64_t              system_id_;
  uint32_t              timeline_;
  uint64_t              flush_lsn_;
  SenderCb              on_sender_;
  uint32_t              wal_segment_size_;
  uint16_t              bound_port_{0};

  tcp::acceptor acceptor_;
  tcp::socket   next_socket_;
};
