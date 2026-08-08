#pragma once

#include "src/net/connection.h"
#include "src/raft/raft_types.h"

struct ClientConnectionApi {
  virtual bool is_leader() const = 0;
  virtual bool serving() const   = 0;
  virtual bool resigning() const = 0;

  virtual RaftIndex applied_index() const = 0;

  virtual std::optional<uint64_t> primary_flushed_lsn() const = 0;
  virtual std::optional<uint64_t> standby_applied_lsn() const = 0;
};

class RaftClientConnection : public Connection {
 public:
  RaftClientConnection(shared_ptr<ClientConnectionApi> server, string peer, socket_type socket)
      : Connection(std::move(peer), std::move(socket)), server_(server) {}

  void close() { shutdown(); }

  // Is the server a leader.
  uint8_t status() const final;

 private:
  void read_request();

  weak_ptr<ClientConnectionApi> const server_;
};
