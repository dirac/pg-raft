#include "src/raft/raft_client_connection.h"

Logger RaftClientConnection::logger{"raft-client-conn"};

std::atomic<unsigned> RaftClientConnection::counter{0};

bool RaftClientConnection::is_leader() const {
  if (auto s = server_.lock(); s)
    return s->is_leader() && !s->resigning();
  return false;
}

void RaftClientConnection::start() {
  Connection::start();
  read_request();
}

void RaftClientConnection::read_request() {
  read_data([this, s = self()](Result<Data> r) {
    if (!r)
      return shutdown(r.error());

    counter.fetch_add(1);

    if (auto req = from_data<ZkRequest>(std::move(r.value())); !req)
      return shutdown(req.error());
    else
      process(std::move(req.value()));
  });
}

void RaftClientConnection::process(ZkRequest request) {
  if (auto s = server_.lock(); s)
    s->exec(request,
            [this, c = self()](ZkResponse response) { write_response(std::move(response)); });
  else
    shutdown();
}

void RaftClientConnection::write_response(ZkResponse response) {
  counter.fetch_sub(1);
  write_data(to_data(response), [this, s = self()](Result<void> r) {
    if (!r)
      return shutdown(r.error());
    read_request();
  });
}

