#pragma once

#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "src/wal_sender/wal_sender.h"

// Listens on a TCP port and spawns a WalSender for each incoming replication
// connection.
//
// For each accepted connection, on_sender is called in a dedicated thread with
// a reference to the newly created WalSender.  The callback must call
// sender.Run() (possibly after starting a producer thread that feeds
// sender.Send()).  Run() blocks until the connection closes; the thread exits
// when on_sender returns.
class WalSenderServer {
 public:
  using SenderCallback = std::function<void(WalSender& sender)>;

  // |on_sender| is called once per accepted connection, in its own thread.
  WalSenderServer(uint16_t port, uint64_t system_id, uint32_t timeline,
                  uint64_t end_lsn, SenderCallback on_sender,
                  WalSender::StatusCallback status_cb = {});
  ~WalSenderServer();

  WalSenderServer(const WalSenderServer&) = delete;
  WalSenderServer& operator=(const WalSenderServer&) = delete;

  // Bind, listen, and start the accept loop thread. Returns false on error.
  bool Start();

  // Stop accepting new connections. Existing connections continue until they
  // finish naturally (call Shutdown() on individual senders to force them out).
  void Stop();

  // Port that the server is actually listening on (useful when port 0 was
  // requested).
  uint16_t bound_port() const { return bound_port_; }

 private:
  void AcceptLoop();

  uint16_t port_;
  uint64_t system_id_;
  uint32_t timeline_;
  uint64_t end_lsn_;
  SenderCallback on_sender_;
  WalSender::StatusCallback status_cb_;

  int listen_fd_ = -1;
  int wakeup_[2] = {-1, -1};
  uint16_t bound_port_ = 0;

  std::thread accept_thread_;
  std::mutex conn_mu_;
  std::vector<std::thread> conn_threads_;
};
