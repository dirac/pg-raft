#pragma once

#include "src/base/wal_segment.h"
#include "src/net/connection.h"
#include "src/raft/raft_types.h"

struct FollowerConnectionApi {
  virtual std::optional<RaftResponse> handle_peer_request(RaftNodeId from_id,
                                                          RaftTerm   term,
                                                          std::monostate) = 0;

  virtual std::optional<RaftResponse> handle_peer_request(RaftNodeId      candidate,
                                                          RaftTerm        term,
                                                          RequestElection request) = 0;

  virtual std::optional<RaftResponse> handle_peer_request(RaftNodeId  candidate,
                                                          RaftTerm    term,
                                                          RequestVote request) = 0;

  virtual std::optional<RaftResponse> handle_peer_request(RaftNodeId    leader,
                                                          RaftTerm      term,
                                                          AppendEntries request) = 0;

  virtual std::optional<RaftResponse> handle_peer_request(RaftNodeId      leader,
                                                          RaftTerm        term,
                                                          InstallSnapshot request) = 0;

  virtual Result<shared_ptr<WalSegment>> segment(LogSeqNumber const& lsn) const = 0;

  [[noreturn]] virtual void system_error(std::error_code ec) const = 0;

  virtual std::shared_lock<std::shared_mutex> usage_lock() const = 0;
};

class RaftFollowerConnection : public Connection {
 public:
  RaftFollowerConnection(RaftNodeId                        self_id,
                         string                            peer,
                         socket_type                       socket,
                         shared_ptr<FollowerConnectionApi> server)
      : Connection(std::move(peer), std::move(socket)),
        self_id_(self_id),
        server_(std::move(server)) {
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

  void write_response(RequestResponseInfo         info,
                      RaftNodeId                  from_id,
                      std::optional<RaftResponse> response);

  void received(RaftRequest&& request, Bytes&& bytes);

  void system_error(std::error_code      ec,
                    std::source_location loc = std::source_location::current()) {
    shutdown(ec, loc);
    if (auto server = server_.lock(); server)
      server->system_error(ec);
  }

  Result<void> write(shared_ptr<FollowerConnectionApi> const& server,
                     inplace_vector<MemBlock, 2> const&       blocks,
                     Bytes const&                             bytes);

 private:
  RaftNodeId const                      self_id_;
  weak_ptr<FollowerConnectionApi> const server_;
};
