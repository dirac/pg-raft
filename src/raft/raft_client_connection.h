#pragma once

#include "src/net/connection.h"
#include "src/raft/client_types.h"

/// The backup of the PostgreSQL instance for a SnapshotRequest: the response and the files to send
/// after it (absolute paths, in the order of SnapshotResponse::files).
using BackupHandler =
    std::function<void(SnapshotResponse response, vector<std::filesystem::path> files)>;

struct ClientConnectionApi {
  virtual ~ClientConnectionApi() = default;

  virtual bool is_leader() const = 0;
  virtual bool serving() const   = 0;
  virtual bool resigning() const = 0;

  virtual RaftIndex applied_idx() const = 0;

  virtual std::optional<uint64_t> primary_flushed_lsn() const = 0;
  virtual std::optional<uint64_t> standby_applied_lsn() const = 0;

  /// Take a backup of the PostgreSQL instance; the handler is called when it is ready (or failed).
  virtual void backup(SnapshotRequest request, BackupHandler handler) = 0;
};

/// The server side of a client connection (the proxy, or a raft server fetching a backup). The
/// other end is a RaftServerConnection.
class RaftClientConnection : public Connection {
 public:
  RaftClientConnection(shared_ptr<ClientConnectionApi> const& server,
                       string                                 peer,
                       socket_type                            socket)
      : Connection(std::move(peer), std::move(socket)), server_(server) {}

  void close() { shutdown(); }

  /// The leadership, sent with every heartbeat: 0x3 leader and serving, 0x1 leader, 0x0 not.
  uint8_t status() const final;

  void start() final;

 private:
  void read_request();

  void received(shared_ptr<ClientConnectionApi> const& s, StatusRequest req);
  void received(shared_ptr<ClientConnectionApi> const& s, SnapshotRequest req);

  /// Send the files one after the other, then read the next request.
  void send_files(vector<std::filesystem::path> files);

  weak_ptr<ClientConnectionApi> const server_;
};
