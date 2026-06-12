#include "src/raft/raft_types.h"

std::ostream& operator<<(std::ostream& out, RequestElection const& r) {
  return out << std::format("{}", r);
}

std::ostream& operator<<(std::ostream& out, RequestElectionResponse const& r) {
  return out << std::format("{}", r);
}

std::ostream& operator<<(std::ostream& out, RequestVote const& r) {
  return out << std::format("{}", r);
}

std::ostream& operator<<(std::ostream& out, RequestVoteResponse const& r) {
  return out << std::format("{}", r);
}

std::ostream& operator<<(std::ostream& out, Entry const& e) {
  return out << std::format("{}", e);
}

std::ostream& operator<<(std::ostream& out, AppendEntries const& ae) {
  return out << std::format("{}", ae);
}

std::ostream& operator<<(std::ostream& out, AppendEntriesResponse const& r) {
  return out << std::format("{}", r);
}

std::ostream& operator<<(std::ostream& out, InstallSnapshot const& s) {
  return out << std::format("{}", s);
}

std::ostream& operator<<(std::ostream& out, InstallSnapshotResponse const& s) {
  return out << std::format("{}", s);
}

std::ostream& operator<<(std::ostream& out, RaftRequest const& r) {
  return out << std::format("{}", r);
}

std::ostream& operator<<(std::ostream& out, RaftResponse const& r) {
  return out << std::format("{}", r);
}
