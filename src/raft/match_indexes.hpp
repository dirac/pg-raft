#ifndef LIB_SERVERS_ZOOKEEPER_MATCH_INDEXES_HPP_
#define LIB_SERVERS_ZOOKEEPER_MATCH_INDEXES_HPP_

#include "lib/base/base.hpp"

namespace banyandb::servers::zookeeper {

using namespace banyandb::base;

struct MatchIndexes {
 public:
  MatchIndexes(RaftNodeId self_id, size_t n)
      : self_id_(self_id), quorum_(static_cast<unsigned>(n) >> 1), match_indexes_(n, RaftIndex{0}) {
    tmp_.reserve(n);
  }

  // Reset on leader election.
  void reset() {
    std::for_each(std::begin(match_indexes_), std::end(match_indexes_), [](auto& v) {
      v.store(RaftIndex{0});
    });
  }

  void match_index(RaftNodeId const id, RaftIndex const idx) {
    auto& node_index = match_indexes_.at(INT(id));

    RaftIndex curr = node_index.load(std::memory_order_relaxed);
    while (curr < idx && !node_index.compare_exchange_strong(curr, idx))
      ;
  }

  RaftIndex commit_index(RaftIndex const current_commit_idx);

 private:
  RaftNodeId const self_id_;

  // Votes required to update the commit idx.
  unsigned const quorum_;

  // Volatile state on leaders.
  // The index of the highest log entry known.
  vector<std::atomic<RaftIndex>> match_indexes_;

  // Working copy to avoid reallocation for the computation of has_new_commit_idx
  vector<std::tuple<RaftIndex, unsigned>> tmp_;

  std::mutex mtx_;
};

}  // namespace banyandb::servers::zookeeper

#endif
