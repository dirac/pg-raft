#include "src/raft/vote_tally.h"

VoteTally::VoteTally(size_t n)
    : responses_recvd_(n, false), votes_granted_(n, false), all_false_(n, false) {}

bool VoteTally::set_vote(RaftNodeId node, bool vote_granted) {
  std::unique_lock<std::mutex> lk{mtx_};

  if (responses_recvd_.at(INT(node)) && !vote_granted)
    return false;

  responses_recvd_.at(INT(node)) = true;
  votes_granted_.at(INT(node))   = vote_granted;
  return true;
}

/// The votes granted and votes received.
std::tuple<unsigned, unsigned> VoteTally::votes_tally() const {
  std::unique_lock<std::mutex> lk{mtx_};

  return {std::accumulate(votes_granted_.begin(),
                          votes_granted_.end(),
                          0U,
                          [](unsigned cnt, bool v) -> unsigned { return cnt + (v ? 1U : 0U); }),
          std::accumulate(responses_recvd_.begin(),
                          responses_recvd_.end(),
                          0U,
                          [](unsigned cnt, bool v) -> unsigned { return cnt + (v ? 1U : 0U); })};
}

void VoteTally::clear(RaftNodeId node, RaftTerm term) {
  std::unique_lock<std::mutex> lk{mtx_};

  votes_granted_   = all_false_;
  responses_recvd_ = all_false_;

  responses_recvd_.at(INT(node)) = true;
  votes_granted_.at(INT(node))   = true;

  term_ = term;
}

