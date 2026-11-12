#pragma once

#include "src/base/logger.h"
#include "src/net/connection.h"
#include "src/raft/client_types.h"

/// A client's connection to a raft server (the proxy, or a raft server fetching a backup). The
/// other end is a RaftClientConnection.
class RaftServerConnection : public Connection {
 public:
  RaftServerConnection(string const&             self,
                       RaftNodeId                node_id,
                       std::chrono::milliseconds timeout,
                       socket_type               socket)
      : Connection(std::format("{}:{}", self, INT(node_id)), std::move(socket), timeout),
        id_(node_id),
        leader_(0) {}

  void close() { shutdown(); }

  /// The status byte of the server's heartbeat responses (see RaftClientConnection::status()).
  void set_peer_status(uint8_t l) final { set_leader(l); }

  /// Request a status round-trip; the check is called with (is_leader(), is_serving()).
  void async_leadership_check(std::function<void(Result<std::tuple<bool, bool>>)> check) {
    {
      auto lk = std::unique_lock{leadership_checks_mtx_};
      leadership_checks_.emplace_back(std::move(check));
    }
    request_peer_status();
  }

  bool is_leader() const { return leader_.load(std::memory_order_relaxed) != 0 && !closed(); }

  bool is_serving() const {
    return leader_.load(std::memory_order_relaxed) == static_cast<uint8_t>(0x3) && !closed();
  }

  RaftNodeId node_id() const { return id_; }

  /// The status of the server (waits up to 2 seconds).
  Result<StatusResponse> node_status();

  /// A backup of the server's PostgreSQL instance, written into dir (which must not exist). Fails
  /// if the server refuses (see SnapshotRequest::applied_idx).
  Result<void> backup(std::filesystem::path const& dir, RaftIndex applied_idx);

 private:
  void set_leader(uint8_t v) {
    uint8_t old_v = leader_.exchange(v, std::memory_order_relaxed);

    if (old_v != v)
      log(INFO, "Leadership for {} changed from {} to {}", id_, old_v, v);

    std::unique_lock<std::mutex> lk{leadership_checks_mtx_};
    std::ranges::for_each(leadership_checks_,
                          [this](auto& c) { c(std::make_tuple(is_leader(), is_serving())); });
    leadership_checks_.clear();
  }

  /// Read the next message (waits up to timeout).
  Result<Data> read(std::chrono::seconds timeout);

  RaftNodeId const id_;

  std::atomic<uint8_t> leader_;

  std::deque<std::function<void(Result<std::tuple<bool, bool>>)>> leadership_checks_;
  std::mutex                                                      leadership_checks_mtx_;

  mutable std::mutex mtx_;  /// Make sure there is only one outstanding request.
};
