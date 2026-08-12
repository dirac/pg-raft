#pragma once

#include "src/base/logger.h"
#include "src/net/connection.h"
#include "src/raft/client_types.h"

/// A raft server creates a connection to the Raft Server. The other end if RaftClientConnection.
class RaftServerConnection : public Connection {
 public:
  RaftServerConnection(string const&             self,
                       RaftNodeId                node_id,
                       std::chrono::milliseconds timeout,
                       socket_type               socket)
      : Connection(std::format("{}:{}", self, INT(node_id)), std::move(socket), timeout),
        id_(node_id),
        leader_(false) {}

  void close() { shutdown(); }

  void set_peer_status(uint8_t l) final { set_leader(l ? true : false); }

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

  Result<StatusResponse> node_status();

  Result<bool> backup(std::filesystem::path dir);

 protected:
  virtual void set_leader(Result<bool> v) {
    bool new_v = v && v.value();
    bool old_v = leader_.exchange(new_v, std::memory_order_relaxed);

    if (old_v != new_v)
      log(INFO, "Leadership for {}  changed from {} to {}", id_, old_v, new_v);

    std::unique_lock<std::mutex> lk{leadership_checks_mtx_};
    std::for_each(std::begin(leadership_checks_), std::end(leadership_checks_), [this](auto& c) {
      c(std::make_tuple(is_leader(), is_serving()));
    });
    leadership_checks_.clear();
  }

 private:
  RaftNodeId const id_;

  std::atomic<uint8_t> leader_;

  std::deque<std::function<void(Result<std::tuple<bool, bool>>)>> leadership_checks_;
  std::mutex                                                      leadership_checks_mtx_;

  mutable std::mutex mtx_;  /// Make sure there is only one outstanding request.
};
