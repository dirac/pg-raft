#pragma once

#include "src/raft/raft_server.h"

// An ensemble of raft servers in one process, for the tests.

/// A RaftServer that can simulate a crash: the shutdown skips the leadership transfer and closes
/// every accepted connection, as the OS does when the process dies.
class TestRaftServer : public RaftServer {
 public:
  using RaftServer::RaftServer;

  void new_connection(string peer, socket_type socket) override;
  void on_shutdown() override;

  void set_crash_on_shutdown() { crash_.test_and_set(); }

 private:
  std::atomic_flag crash_ = ATOMIC_FLAG_INIT;

  std::mutex                        accepted_mtx_;
  std::vector<weak_ptr<Connection>> accepted_;
};

struct RaftTestService {
  /// A role change, recorded by each server's observer.
  struct RoleEvent {
    unsigned node;
    RaftTerm term;
    bool     became_leader;  // or stepped down
  };

  RaftTestService(boost::asio::io_context& io_context,
                  std::filesystem::path    dir,
                  Config&                  config,
                  Options                  options,
                  unsigned                 nodes = 3);

  ~RaftTestService();

  void start_all();

  void wait_for_leader() const;

  void shutdown();

  /// The leader.
  std::optional<unsigned> leader() const;

  /// The running server, nullptr if it is stopped.
  shared_ptr<TestRaftServer> server(unsigned idx) const {
    auto lk = std::unique_lock{servers_mtx_};
    return servers_.at(idx);
  }

  void start(unsigned idx);

  /// Stop the server; with crash, simulate a process crash (see TestRaftServer); with delete_files
  /// remove its log and its WAL.
  void stop(unsigned idx, bool delete_files, bool crash);

  /// Step down as a leader.
  bool stepdown();

  /// The role changes so far, in order.
  vector<RoleEvent> role_events() const {
    auto lk = std::unique_lock{events_mtx_};
    return events_;
  }

  void record(RoleEvent event) {
    auto lk = std::unique_lock{events_mtx_};
    events_.push_back(event);
  }

  /// The highest commit index of the running servers.
  RaftIndex commit_idx() const;

  /// The directories of a node.
  std::filesystem::path raft_dir(unsigned idx) const { return dir_ / std::format("node{}", idx); }
  std::filesystem::path wal_dir(unsigned idx) const { return raft_dir(idx) / "pg_wal"; }

  /// A write acknowledged by the leader: the entry and the xxhash32 of its bytes.
  struct Write {
    RaftIndex    idx;
    LogSeqNumber segment;
    WriteOp      op;
    uint32_t     checksum;
  };

  struct Consistency {
    /// What is wrong, empty if nothing.
    vector<string> violations;

    /// The committed entries that the servers hold (they agree on the ones that more than one
    /// holds).
    map<RaftIndex, Entry> log;
  };

  /// Waits until the running servers have applied all of their log, then checks that the committed
  /// entries that two servers hold are the same (log matching), that their states are the same,
  /// and that every server has the bytes of the acknowledged writes that it applied.
  Consistency check_consistency(vector<Write> const& writes,
                                std::chrono::seconds timeout = std::chrono::seconds(60)) const;

  boost::asio::io_context& io_context() { return io_context_; }

 private:
  boost::asio::io_context& io_context_;

  std::filesystem::path const dir_;

  unsigned const nodes_;

  Config&       config_;
  Options const options_;

  /// A copy of the servers (the writer thread reads them while the test starts and stops them).
  std::vector<shared_ptr<TestRaftServer>> servers() const {
    auto lk = std::unique_lock{servers_mtx_};
    return servers_;
  }

  mutable std::mutex events_mtx_;
  vector<RoleEvent>  events_;

  mutable std::mutex                                         servers_mtx_;
  std::vector<shared_ptr<TestRaftServer>>                    servers_;
  std::vector<boost::asio::ip::tcp::endpoint>                endpoints_;
  std::vector<std::optional<boost::asio::ip::tcp::acceptor>> acceptors_;
};
