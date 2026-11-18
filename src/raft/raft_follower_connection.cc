#include "src/raft/raft_follower_connection.h"

#include "src/base/logger.h"

void RaftFollowerConnection::start() {
  Connection::start();
  read_request();
}

void RaftFollowerConnection::read_request() {
  read_data_and_buff([this, s = self()](Result<std::tuple<Data, Bytes>> msg) {
    if (!msg)
      return shutdown(msg.error());

    auto& [data, bytes] = msg.value();
    auto r              = from_data<RaftRequest>(data);
    if (!r)
      return shutdown(r.error());

    received(std::move(r.value()), std::move(bytes));
  });
}

void RaftFollowerConnection::received(RaftRequest&& req, Bytes&& bytes) {
  if (req.info.log)
    log(INFO, "{}{}", log_prefix(), req);

  auto [lk, server] = lock();
  if (!lk)
    return shutdown();

  auto const info    = req.info;
  auto const term    = req.term;
  auto const from_id = req.node_id;

  auto response = std::visit(
      [&](auto&& v) -> RaftResponse {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, AppendEntries> || std::is_same_v<T, InstallSnapshot>)
          return server->handle_peer_request(from_id, term, std::move(v), bytes);
        else
          return server->handle_peer_request(from_id, term, std::move(v));
      },
      std::move(req.request));

  write_response(info, std::move(response));
}

void RaftFollowerConnection::write_response(RequestResponseInfo info, RaftResponse response) {
  response.info    = info;
  response.node_id = self_id_;

  if (response.info.log)
    log(INFO, "{}{}", log_prefix(), response);

  write_data(to_data(response), [this, s = self()](Result<void> r) {
    if (!r)
      shutdown(r.error());
  });
  read_request();
}
