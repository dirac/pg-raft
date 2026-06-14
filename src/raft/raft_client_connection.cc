#include "src/raft/raft_client_connection.h"


uint8_t RaftClientConnection::status() const {
  uint8_t x = 0;
  if (auto s = server_.lock(); s) {
    if (s->is_leader() && !s->resigning())
      x = s->serving() ? static_cast<uint8_t>(0x3) : static_cast<uint8_t>(0x1);
  }
  return x;
}
