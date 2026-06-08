#ifndef LIB_SERVERS_ZOOKEEPER_SNAPSHOT_HPP_
#define LIB_SERVERS_ZOOKEEPER_SNAPSHOT_HPP_

#include "lib/servers/zookeeper/raft_log.hpp"

namespace banyandb::servers::zookeeper {
class Snapshot {
 public:
  Snapshot(std::filesystem::path dir, RaftIndex idx) : dir_(dir), log_index_(idx) {}

  ~Snapshot();

  RaftIndex log_index() const { return log_index_; }

  // It will construct the snapshot, if one doesn't already exist. The snapshot file is deleted on
  // destructor.
  // Returns the filename, checksum and file size.
  Result<std::tuple<std::filesystem::path, string, size_t>> details(RaftLog const& log);

 private:
  std::mutex mtx_;

  std::filesystem::path dir_;
  std::filesystem::path file_;

  size_t size_{0};
  string checksum_;

  RaftIndex const log_index_;
};
}  // namespace banyandb::servers::zookeeper

#endif
