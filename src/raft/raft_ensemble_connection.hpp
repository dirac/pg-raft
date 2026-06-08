#ifndef LIB_SERVERS_ZOOKEEPER_RAFT_ENSEMBLE_CONNECTION_HPP_
#define LIB_SERVERS_ZOOKEEPER_RAFT_ENSEMBLE_CONNECTION_HPP_

#include "lib/base/certificates.hpp"
#include "lib/base/config.hpp"
#include "lib/net/ssl_context.hpp"
#include "lib/servers/zookeeper/raft_server_connection.hpp"

namespace banyandb::servers::zookeeper {

using namespace banyandb::base;
using namespace banyandb::net;

class RaftEnsembleConnection : public std::enable_shared_from_this<RaftEnsembleConnection> {
 public:
  static constexpr unsigned kPingSeconds           = 1;
  static constexpr unsigned kSessionTimeoutSeconds = 8;

  RaftEnsembleConnection(boost::asio::io_context& io_context,
                         Config const&            config,
                         Role                     self_role,
                         string                   self_id,
                         Certificates const&      certs,
                         std::chrono::seconds     timeout = std::chrono::seconds(5));

  virtual ~RaftEnsembleConnection();

  string const& name() const { return name_; }

  bool connect();

  ZkResponse exec(ZkRequest request, unsigned tries_on_close = 0);

  bool closed() const { return closed_.test(); }

  void close() {
    if (!closed_.test_and_set())
      close_session();
  }

  unsigned nodes() const { return nodes_; }

  void virtual watch_event(WatchEvent const& we);

  shared_ptr<RaftEnsembleConnection> self() { return shared_from_this(); }

 private:
  void close_session();

  void async_connect(unsigned idx, std::function<void(Result<bool>)> leadership_check);

  ZkResponse process_watches(ZkResponse response);

  void session_ping();
  void ping_all(std::function<void(Result<bool>)> callback);

  shared_ptr<RaftServerConnection> set_leader();
  shared_ptr<RaftServerConnection> get_leader();

  shared_ptr<RaftServerConnection> leader();

  void set_connection(unsigned idx, shared_ptr<RaftServerConnection> conn);

  void set_connection(unsigned                          idx,
                      shared_ptr<RaftServerConnection>  conn,
                      std::function<void(Result<bool>)> leadership_check);

  std::unique_lock<std::mutex> lock() { return std::unique_lock<std::mutex>{exec_fn_mtx_}; }

  boost::asio::io_context& io_context() { return io_context_; }

  boost::asio::io_context& io_context_;

  unsigned const nodes_;

  std::chrono::milliseconds const timeout_;

  // Ping every second so that the session does not expire.
  unsigned last_request_epoch_msecs_;

  uint64_t session_id_{0};
  int64_t  request_no_{1};

  // execute function mutex.
  std::mutex exec_fn_mtx_;

  weak_ptr<RaftServerConnection> leader_;
  std::mutex                     leader_mtx_;

  vector<HostAddress> const                addresses_;
  vector<std::mutex>                       mutexes_;
  vector<ssl::Context>                     ssl_contexts_;
  vector<shared_ptr<RaftServerConnection>> connections_;

  unsigned                  ping_duration_msecs_ = 1000;
  boost::asio::steady_timer timer_;

  std::atomic_flag closed_;
  std::atomic_flag session_ping_started_;

  string const name_;
  string const log_prefix_;
};

}  // namespace banyandb::servers::zookeeper

#endif
