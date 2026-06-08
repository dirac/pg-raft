#include "src/raft/match_indexes.h"

RaftIndex MatchIndexes::commit_index(RaftIndex const commit_idx) {
  std::unique_lock<std::mutex> lk{mtx_};

  tmp_.clear();
  std::for_each(std::begin(match_indexes_), std::end(match_indexes_), [this](auto const& idx) {
    tmp_.emplace_back(idx.load(std::memory_order_relaxed), 1U);
  });
  std::sort(std::begin(tmp_), std::end(tmp_));

  unsigned c = 0;
  std::for_each(std::rbegin(tmp_), std::rend(tmp_), [&c](std::tuple<RaftIndex, unsigned>& elem) {
    c = (std::get<unsigned>(elem) += c);
  });

  while (!tmp_.empty() && std::get<unsigned>(tmp_.back()) < quorum_)
    tmp_.pop_back();

  if (tmp_.empty() || std::get<RaftIndex>(tmp_.back()) < commit_idx)
    return commit_idx;

  return std::get<RaftIndex>(tmp_.back());
}

