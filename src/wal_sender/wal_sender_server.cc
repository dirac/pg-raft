#include "src/wal_sender/wal_sender_server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

WalSenderServer::WalSenderServer(uint16_t port, uint64_t system_id,
                                 uint32_t timeline, uint64_t end_lsn,
                                 SenderCallback on_sender,
                                 WalSender::StatusCallback status_cb)
    : port_(port),
      system_id_(system_id),
      timeline_(timeline),
      end_lsn_(end_lsn),
      on_sender_(std::move(on_sender)),
      status_cb_(std::move(status_cb)) {}

WalSenderServer::~WalSenderServer() {
  Stop();
}

bool WalSenderServer::Start() {
  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) return false;

  int one = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  // Disable Nagle so the standby sees keepalives promptly.
  setsockopt(listen_fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

  struct sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port_);

  if (bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr),
           sizeof(addr)) < 0) {
    close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }

  // Discover the actual port when port_ == 0.
  socklen_t addrlen = sizeof(addr);
  getsockname(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), &addrlen);
  bound_port_ = ntohs(addr.sin_port);

  if (listen(listen_fd_, 16) < 0) {
    close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }

  if (pipe(wakeup_) != 0) {
    close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }
  fcntl(wakeup_[0], F_SETFL, O_NONBLOCK);
  fcntl(wakeup_[1], F_SETFL, O_NONBLOCK);

  accept_thread_ = std::thread(&WalSenderServer::AcceptLoop, this);
  return true;
}

void WalSenderServer::Stop() {
  if (wakeup_[1] >= 0) {
    char b = 1;
    write(wakeup_[1], &b, 1);
  }
  if (accept_thread_.joinable()) accept_thread_.join();

  // Reap any completed connection threads.
  std::vector<std::thread> threads;
  {
    std::lock_guard<std::mutex> lk(conn_mu_);
    threads = std::move(conn_threads_);
  }
  for (auto& t : threads) {
    if (t.joinable()) t.join();
  }

  if (listen_fd_ >= 0) { close(listen_fd_); listen_fd_ = -1; }
  if (wakeup_[0] >= 0) { close(wakeup_[0]); wakeup_[0] = -1; }
  if (wakeup_[1] >= 0) { close(wakeup_[1]); wakeup_[1] = -1; }
}

void WalSenderServer::AcceptLoop() {
  while (true) {
    struct pollfd fds[2];
    fds[0].fd = listen_fd_; fds[0].events = POLLIN;
    fds[1].fd = wakeup_[0]; fds[1].events = POLLIN;

    if (poll(fds, 2, -1) < 0) break;
    if (fds[1].revents & POLLIN) break;  // Stop() signal
    if (!(fds[0].revents & POLLIN)) continue;

    struct sockaddr_in client{};
    socklen_t clen = sizeof(client);
    int conn_fd = accept(listen_fd_,
                         reinterpret_cast<struct sockaddr*>(&client), &clen);
    if (conn_fd < 0) continue;

    // Disable Nagle on the accepted socket too.
    int one = 1;
    setsockopt(conn_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    uint64_t system_id = system_id_;
    uint32_t timeline = timeline_;
    uint64_t end_lsn = end_lsn_;
    SenderCallback cb = on_sender_;
    WalSender::StatusCallback scb = status_cb_;

    std::thread t([conn_fd, system_id, timeline, end_lsn,
                   cb = std::move(cb), scb = std::move(scb)]() {
      WalSender sender(conn_fd, system_id, timeline, end_lsn, std::move(scb));
      cb(sender);
    });

    std::lock_guard<std::mutex> lk(conn_mu_);
    // Reap any already-finished threads before appending.
    conn_threads_.erase(
        std::remove_if(conn_threads_.begin(), conn_threads_.end(),
                       [](std::thread& th) {
                         if (th.joinable()) {
                           // non-blocking check not directly available; defer.
                           return false;
                         }
                         return true;
                       }),
        conn_threads_.end());
    conn_threads_.push_back(std::move(t));
  }
}
