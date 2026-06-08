#include "lib/servers/zookeeper/raft_follower_connection.hpp"

#include "lib/base/cert_functions.hpp"

namespace banyandb::servers::zookeeper {

using namespace banyandb::base;

Logger RaftFollowerConnection::logger{"raft-follower-conn"};

void RaftFollowerConnection::start() {
  Connection::start();
  read_request();
}

void RaftFollowerConnection::read_request() {
  read_data([this, s = self()](Result<Data> msg) {
    if (!msg)
      return shutdown(msg.error());

    if (auto r = from_data<RaftRequest>(std::move(msg.value())); r)
      received(std::move(r.value()));
    else
      return shutdown(r.error());
  });
}

void RaftFollowerConnection::received(RaftRequest&& req) {
  if (req.info.log) {
    LOG_INFO << log_prefix() << req;
  }

  auto info    = req.info;
  auto term    = req.term;
  auto from_id = req.node_id;
  auto request = std::move(req.request);

  std::visit([this, info, term, from_id](
                 auto&& v) mutable -> void { this->handle(info, term, from_id, v); },
             std::move(request));
}

void RaftFollowerConnection::write_response(RequestResponseInfo info,
                                            RaftNodeId,
                                            RaftResponse response) {
  response.info    = info;
  response.node_id = self_id_;

  if (response.info.log) {
    LOG_INFO << log_prefix() << response;
  }

  write_data(to_data(response), [this, s = self()](Result<void> r) {
    if (!r)
      shutdown(r.error());
  });
  read_request();
}

void RaftFollowerConnection::handle(RequestResponseInfo info,
                                    RaftTerm            term,
                                    RaftNodeId          from_id,
                                    std::monostate      request) {
  auto [lk, server] = lock();
  if (!lk)
    return shutdown();

  write_response(info, from_id, server->handle_peer_request(from_id, term, request));
}

void RaftFollowerConnection::handle(RequestResponseInfo info,
                                    RaftTerm            term,
                                    RaftNodeId          from_id,
                                    RequestElection     request) {
  auto [lk, server] = lock();
  if (!lk)
    return shutdown();

  write_response(info, from_id, server->handle_peer_request(from_id, term, request));
}

void RaftFollowerConnection::handle(RequestResponseInfo info,
                                    RaftTerm            term,
                                    RaftNodeId          from_id,
                                    RequestVote         request) {
  auto [lk, server] = lock();
  if (!lk)
    return shutdown();

  write_response(info, from_id, server->handle_peer_request(from_id, term, request));
}

void RaftFollowerConnection::handle(RequestResponseInfo info,
                                    RaftTerm            term,
                                    RaftNodeId          from_id,
                                    AppendEntries       request) {
  auto [lk, server] = lock();
  if (!lk)
    return shutdown();

  write_response(info, from_id, server->handle_peer_request(from_id, term, request));
}

void RaftFollowerConnection::handle(RequestResponseInfo info,
                                    RaftTerm            term,
                                    RaftNodeId          from_id,
                                    InstallSnapshot     request) {
  // Fetch the size of the file.
  auto handler = [this, s = self(), info, term, from_id, request = std::move(request)](
                     Result<std::filesystem::path> file) {
    if (!file)
      return shutdown(file.error());

    install_snapshot_file(info, term, from_id, std::move(request), std::move(file.value()));
  };

  read_file(std::move(handler));
}

void RaftFollowerConnection::install_snapshot_file(RequestResponseInfo   info,
                                                   RaftTerm              term,
                                                   RaftNodeId            from_id,
                                                   InstallSnapshot       request,
                                                   std::filesystem::path file) {
  auto [lk, server] = lock();
  if (!lk)
    return shutdown();

  if (validate_snapshot_file(file, request)) {
    write_response(
        info, from_id, server->handle_peer_request(from_id, term, request, std::move(file)));
  } else
    shutdown();
}

bool RaftFollowerConnection::validate_snapshot_file(std::filesystem::path const& file,
                                                    InstallSnapshot const&       request) {
  if (auto r = CertFunctions::sha256(file); r)
    return request.checksum == r.value();
  else
    system_error(system_error_code(EINVAL));

  return false;
}

}  // namespace banyandb::servers::zookeeper
