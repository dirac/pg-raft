#ifndef LIB_SERVERS_ZOOKEEPER_RAFT_FOLLOWER_CONNECTION_HPP_
#define LIB_SERVERS_ZOOKEEPER_RAFT_FOLLOWER_CONNECTION_HPP_

#include "lib/net/connection.hpp"
#include "lib/servers/zookeeper/raft_types.hpp"

namespace banyandb::servers::zookeeper {

using namespace banyandb::base;
using namespace banyandb::net;

struct FollowerConnectionApi {
  virtual RaftResponse handle_peer_request(RaftNodeId from_id, RaftTerm term, std::monostate) = 0;

  virtual RaftResponse handle_peer_request(RaftNodeId      candidate,
                                           RaftTerm        term,
                                           RequestElection request) = 0;

  virtual RaftResponse handle_peer_request(RaftNodeId  candidate,
                                           RaftTerm    term,
                                           RequestVote request) = 0;

  virtual RaftResponse handle_peer_request(RaftNodeId    leader,
                                           RaftTerm      term,
                                           AppendEntries request) = 0;

  virtual RaftResponse handle_peer_request(RaftNodeId            leader,
                                           RaftTerm              term,
                                           InstallSnapshot       request,
                                           std::filesystem::path snapshot_file) = 0;

  [[noreturn]] virtual void system_error(std::error_code ec) const = 0;

  virtual std::shared_lock<std::shared_mutex> usage_lock() const = 0;
};

class RaftFollowerConnection : public Connection<> {
 public:
  static Logger logger;

  RaftFollowerConnection(RaftNodeId                        self_id,
                         string                            peer,
                         socket_type                       socket,
                         shared_ptr<FollowerConnectionApi> server,
                         std::filesystem::path             tmpdir)
      : Connection<>(to_string(Role::kZookeeper),
                   std::move(peer),
                   std::move(socket),
                   std::move(tmpdir),
                   logger),
        self_id_(self_id),
        server_(std::move(server)) {
    set_log_prefix(fn::format("({0} <- {1}) ", INT(self_id_), Connection<>::peer()));
  }

  void start() final;

 private:
  std::tuple<std::shared_lock<std::shared_mutex>, shared_ptr<FollowerConnectionApi>> lock() {
    if (auto s = server_.lock(); s)
      return {s->usage_lock(), s};
    return {};
  }

  void read_request();

  void write_response(RequestResponseInfo info, RaftNodeId from_id, RaftResponse response);

  void received(RaftRequest&& request);

  void handle(RequestResponseInfo info, RaftTerm term, RaftNodeId from_id, std::monostate);

  void handle(RequestResponseInfo info, RaftTerm term, RaftNodeId from_id, RequestElection request);

  void handle(RequestResponseInfo info, RaftTerm term, RaftNodeId from_id, RequestVote request);

  void handle(RequestResponseInfo info, RaftTerm term, RaftNodeId from_id, AppendEntries request);

  void handle(RequestResponseInfo info, RaftTerm term, RaftNodeId from_id, InstallSnapshot request);

  void install_snapshot_file(RequestResponseInfo   info,
                             RaftTerm              term,
                             RaftNodeId            from_id,
                             InstallSnapshot       request,
                             std::filesystem::path file);

  /// Check the sha256 value for the file.
  bool validate_snapshot_file(std::filesystem::path const& file, InstallSnapshot const& request);

  void system_error(std::error_code      ec,
                    std::source_location loc = std::source_location::current()) {
    shutdown(ec, loc);
    if (auto server = server_.lock(); server)
      server->system_error(ec);
  }

 private:
  RaftNodeId const                      self_id_;
  weak_ptr<FollowerConnectionApi> const server_;
};

}  // namespace banyandb::servers::zookeeper

#endif
