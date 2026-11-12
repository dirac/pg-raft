#pragma once

#include "src/base/base.h"

struct VoteTally {
  VoteTally(size_t n);

  /// Returns true if the votes have increased.
  bool set_vote(RaftNodeId node, bool vote_granted);

  /// The votes granted and votes received.
  std::tuple<unsigned, unsigned> votes_tally() const;

  void clear(RaftNodeId self_id, RaftTerm term);

  RaftTerm term() const {
    std::unique_lock<std::mutex> lk{mtx_};
    return term_;
  }

 private:
  // The voting is for this term
  RaftTerm term_;

  vector<bool>       responses_recvd_;
  vector<bool>       votes_granted_;
  vector<bool> const all_false_;

  mutable std::mutex mtx_;
};
