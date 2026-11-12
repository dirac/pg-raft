#include "src/raft/raft_follower_connection.h"

#include "src/base/logger.h"

// Ported from sparrowdb lib/servers/zqlite/raft_follower_connection.cpp.

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

bool RaftFollowerConnection::valid(inplace_vector<MemBlock, 2> const& blocks, Bytes const& bytes) {
  auto size = std::accumulate(std::begin(blocks),
                              std::end(blocks),
                              size_t{0},
                              [](size_t result, MemBlock const& b) { return result + b.size; });
  return size == bytes.size() &&
         std::ranges::all_of(blocks, [](MemBlock const& b) {
           return b.offset >= 0 && b.offset + static_cast<off_t>(b.size) <= kWalSegmentSize;
         });
}

void RaftFollowerConnection::received(RaftRequest&& req, Bytes&& bytes) {
  if (req.info.log)
    log(INFO, "{}{}", log_prefix(), req);

  if (!valid(req.data, bytes)) {
    log(ERROR, "{}The WAL blocks do not match the {} bytes received", log_prefix(), bytes.size());
    return shutdown(system_error_code(EBADMSG));
  }

  auto [lk, server] = lock();
  if (!lk)
    return shutdown();

  auto const info    = req.info;
  auto const term    = req.term;
  auto const from_id = req.node_id;
  auto const blocks  = req.data;

  auto response = std::visit(
      [&](auto&& v) -> RaftResponse {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, AppendEntries> || std::is_same_v<T, InstallSnapshot>)
          return server->handle_peer_request(from_id, term, std::move(v), blocks, bytes);
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
