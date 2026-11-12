#include "src/raft/raft_client_connection.h"

#include "src/base/logger.h"

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

    auto r = from_data<ClientRequest>(msg.value());
    if (!r)
      return shutdown(r.error());

    std::visit([this](auto&& v) { received(server_.lock(), std::move(v)); }, std::move(r.value()));
  });
}

void RaftClientConnection::received(shared_ptr<ClientConnectionApi> const& s, StatusRequest) {
  if (!s)
    return shutdown();

  write_data(to_data(StatusResponse{
                 .leader              = s->is_leader(),
                 .applied_idx         = s->applied_idx(),
                 .primary_flushed_lsn = s->primary_flushed_lsn(),
                 .standby_applied_lsn = s->standby_applied_lsn(),
             }),
             [this, _ = self()](Result<void> r) {
               if (!r)
                 shutdown(r.error());
             });
  read_request();
}

void RaftClientConnection::received(shared_ptr<ClientConnectionApi> const& s,
                                    SnapshotRequest                        req) {
  if (!s)
    return shutdown();

  // A server that has not applied what the requester has cannot give it a newer backup.
  if (req.applied_idx > s->applied_idx()) {
    write_data(to_data(SnapshotResponse{.success = false, .files = {}, .links = {}}),
               [this, _ = self()](Result<void> r) {
                 if (!r)
                   shutdown(r.error());
               });
    return read_request();
  }

  s->backup(std::move(req),
            [this, _ = self()](SnapshotResponse response, vector<std::filesystem::path> files) {
              if (!response.success)
                files.clear();
              assert(files.size() == response.files.size());

              write_data(to_data(response), [this, _ = self()](Result<void> r) {
                if (!r)
                  shutdown(r.error());
              });
              send_files(std::move(files));
            });
}

void RaftClientConnection::send_files(vector<std::filesystem::path> files) {
  // The writes are queued in order; the next request is read once the files are queued.
  for (auto& file : files)
    write_file(std::move(file), [this, _ = self()](Result<void> r) {
      if (!r)
        shutdown(r.error());
    });
  read_request();
}
