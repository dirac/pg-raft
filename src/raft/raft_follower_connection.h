#pragma once

#include "src/net/connection.h"
#include "src/raft/raft_types.h"

// Ported from sparrowdb lib/servers/zqlite/raft_follower_connection.hpp. Difference: a request is
// read with the WAL bytes that follow it, which are given to the server with AppendEntries and
// InstallSnapshot (the server writes them only once it has accepted the request).

struct FollowerConnectionApi {
  virtual ~FollowerConnectionApi() = default;

  virtual RaftResponse handle_peer_request(RaftNodeId from_id, RaftTerm term, std::monostate) = 0;

  virtual RaftResponse handle_peer_request(RaftNodeId      candidate,
                                           RaftTerm        term,
                                           RequestElection request) = 0;

  virtual RaftResponse handle_peer_request(RaftNodeId  candidate,
                                           RaftTerm    term,
                                           RequestVote request) = 0;

  virtual RaftResponse handle_peer_request(RaftNodeId                         leader,
                                           RaftTerm                           term,
                                           AppendEntries                      request,
                                           inplace_vector<MemBlock, 2> const& blocks,
                                           Bytes const&                       bytes) = 0;

  virtual RaftResponse handle_peer_request(RaftNodeId                         leader,
                                           RaftTerm                           term,
                                           InstallSnapshot                    request,
                                           inplace_vector<MemBlock, 2> const& blocks,
                                           Bytes const&                       bytes) = 0;

  [[noreturn]] virtual void system_error(std::error_code ec) const = 0;

  virtual std::shared_lock<std::shared_mutex> usage_lock() const = 0;
};

class RaftFollowerConnection : public Connection {
 public:
  RaftFollowerConnection(RaftNodeId                               self_id,
                         string                                   peer,
                         socket_type                              socket,
                         shared_ptr<FollowerConnectionApi> const& server)
      : Connection(std::move(peer), std::move(socket)), self_id_(self_id), server_(server) {
    set_log_prefix(std::format("({0} <- {1}) ", INT(self_id_), Connection::peer()));
  }

  void start() final;

 private:
  std::tuple<std::shared_lock<std::shared_mutex>, shared_ptr<FollowerConnectionApi>> lock() {
    if (auto s = server_.lock(); s)
      return {s->usage_lock(), s};
    return {};
  }

  void read_request();

  void write_response(RequestResponseInfo info, RaftResponse response);

  void received(RaftRequest&& request, Bytes&& bytes);

  /// The total size of the blocks is the size of the bytes.
  static bool valid(inplace_vector<MemBlock, 2> const& blocks, Bytes const& bytes);

  RaftNodeId const                      self_id_;
  weak_ptr<FollowerConnectionApi> const server_;
};
