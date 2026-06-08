#ifndef LIB_SERVERS_ZOOKEEPER_RAFT_CLIENT_CONNECTION_HPP_
#define LIB_SERVERS_ZOOKEEPER_RAFT_CLIENT_CONNECTION_HPP_

#include "lib/net/connection.hpp"
#include "lib/servers/zookeeper/raft_types.hpp"

namespace banyandb::servers::zookeeper {

using namespace banyandb::base;
using namespace banyandb::net;

struct ClientConnectionApi {
  virtual bool is_leader() const                                                = 0;
  virtual bool resigning() const                                                = 0;
  virtual void exec(ZkRequest request, std::function<void(ZkResponse)> handler) = 0;
};

class RaftClientConnection : public Connection<> {
 public:
  static Logger logger;
  static std::atomic<unsigned> counter;

  RaftClientConnection(shared_ptr<ClientConnectionApi> server,
                       char const*                     role,
                       string                          peer,
                       socket_type                     socket)
      : Connection<>(role, std::move(peer), std::move(socket), std::filesystem::path{}, logger),
        server_(server) {}

  void start() final;

  void close() { shutdown(); }

  // Is the server a leader.
  uint8_t status() const { return is_leader() ? 1 : 0; }

 private:
  bool is_leader() const;

  void read_request();
  void process(ZkRequest request);
  void write_response(ZkResponse response);

  weak_ptr<ClientConnectionApi> const server_;
};

}  // namespace banyandb::servers::zookeeper

#endif
