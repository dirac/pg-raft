#include "src/raft/raft_follower_connection.h"

void RaftFollowerConnection::start() {
  Connection::start();
  read_request();
}

void RaftFollowerConnection::read_request() {
  read_data([this, s = self()](Result<Data> msg) {
    if (!msg)
      return shutdown(msg.error());

    Bytes bytes;
    if (auto r = from_data<RaftRequest>(std::move(msg.value()), bytes); r)
      received(std::move(r.value()), std::move(bytes));
    else
      return shutdown(r.error());
  });
}

void RaftFollowerConnection::received(RaftRequest&& req, Bytes&& bytes) {
  auto [lk, server] = lock();
  if (!lk)
    return shutdown();

  if (req.info.log)
    log(INFO, "{}{}", log_prefix(), req);

  if (auto r = write(server, req.data, bytes); !r)
    return shutdown(r.error());

  auto info    = req.info;
  auto term    = req.term;
  auto from_id = req.node_id;
  auto request = std::move(req.request);

  std::visit(
      [this, info, term, from_id, server](auto&& v) mutable -> void {
        this->write_response(
            info, from_id, server->handle_peer_request(from_id, term, std::move(v)));
      },
      std::move(request));
}

void RaftFollowerConnection::write_response(RequestResponseInfo info,
                                            RaftNodeId,
                                            std::optional<RaftResponse> response) {
  assert(info.respond == response.has_value());

  if (info.respond) {
    response->info    = info;
    response->node_id = self_id_;

    if (response->info.log)
      log(INFO, "{}{}", log_prefix(), response);

    write_data(to_data(response.value()), [this, s = self()](Result<void> r) {
      if (!r)
        shutdown(r.error());
    });
  }
  read_request();
}

Result<void> RaftFollowerConnection::write(shared_ptr<FollowerConnectionApi> const& server,
                                           inplace_vector<MemBlock, 2> const&       blocks,
                                           Bytes const&                             bytes) {
  uint8_t const* data = bytes.data();
  for (auto const& b : blocks) {
    if (auto r = server->segment(b.lsn); !r)
      return elog(r.error(), "Unable to find the segment {}", b.lsn);
    else {
      r.value()->write(data, b.offset, b.size);
      if (auto f = r.value()->fsync(b.offset, b.size); !f)
        return elog(f.error(), "fsync failed for lsn={} off={} sz={}", b.lsn, b.offset, b.size);
    }
    data += b.size;
  }
  return ok();
}
