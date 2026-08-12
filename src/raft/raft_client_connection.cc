#include "src/raft/raft_client_connection.h"

uint8_t RaftClientConnection::status() const {
  uint8_t x = 0;
  if (auto s = server_.lock(); s) {
    if (s->is_leader() && !s->resigning())
      x = s->serving() ? static_cast<uint8_t>(0x3) : static_cast<uint8_t>(0x1);
  }
  return x;
}

void RaftClientConnection::start() {
  Connection::start();
  read_request();
}

void RaftClientConnection::read_request() {
  read_data([this, s = self()](Result<Data> msg) {
    if (!msg)
      return shutdown(msg.error());

    Bytes bytes;
    if (auto r = from_data<ClientRequest>(std::move(msg.value())); r)
      std::visit([this](auto&& v) { received(server_.lock(), v); }, r.value());
    else
      return shutdown(r.error());
  });
}

void RaftClientConnection::received(shared_ptr<ClientConnectionApi> s, StatusRequest) {
  if (!s)
    return shutdown();

  write_data(to_data(StatusResponse{
                 .leader              = s->is_leader(),
                 .applied_idx         = s->applied_idx(),
                 .primary_flushed_lsn = s->primary_flushed_lsn(),
                 .standby_applied_lsn = s->standby_applied_lsn(),
             }),
             [this, _ = self()](Result<void> r) {
               if (r)
                 read_request();
               else
                 shutdown(r.error());
             });
}

void RaftClientConnection::received(shared_ptr<ClientConnectionApi> s, SnapshotRequest req) {
  if (!s)
    return shutdown();

  if (req.applied_idx > s->applied_idx()) {
    write_data(to_data(SnapshotResponse{.success = false, .files = {}, .links = {}}),
               [this, _ = self()](Result<void> r) {
                 if (r)
                   read_request();
                 else
                   shutdown(r.error());
               });
  }
}
