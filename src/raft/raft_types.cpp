#include "lib/servers/zookeeper/raft_types.hpp"

namespace banyandb::servers::zookeeper {

std::ostream& operator<<(std::ostream& out, RequestElection const& r) {
  return out << "RequestElection { term = " << std::get<RaftTerm>(r.last_entry)
             << " index = " << std::get<RaftIndex>(r.last_entry) << " }";
}

std::ostream& operator<<(std::ostream& out, RequestElectionResponse const& r) {
  return out << "RequestElectionResponse { success = " << r.success << " }";
}

std::ostream& operator<<(std::ostream& out, RequestVote const& r) {
  return out << "RequestVote { term = " << std::get<RaftTerm>(r.last_entry)
             << " index = " << std::get<RaftIndex>(r.last_entry) << " }";
}

std::ostream& operator<<(std::ostream& out, RequestVoteResponse const& r) {
  return out << "RequestVoteResponse { vote_granted = " << r.vote_granted << " } ";
}

std::ostream& operator<<(std::ostream& out, Entry const& e) {
  return out << "E{ time = " << e.time << " term = " << e.term << " cmd = " << e.cmd << " }";
}

std::ostream& operator<<(std::ostream& out, AppendEntries const& ae) {
  return out << "AppendEntries { prev_log_idx = " << ae.prev_log_idx
             << " prev_log_term = " << ae.prev_log_term << " commit_idx = " << ae.commit_idx
             << " Entries = " << ae.entries << " }";
}

std::ostream& operator<<(std::ostream& out, AppendEntriesResponse const& r) {
  if (r.success)
    return out << "AppendEntriesResponse { success = true }";
  out << "AppendEntriesResponse { idx = " << r.idx;
  if (r.term)
    out << " term = " << *r.term;
  if (r.n)
    out << " n = " << *r.n;
  return out << " }";
}

std::ostream& operator<<(std::ostream& out, InstallSnapshot const& s) {
  return out << "InstallSnapshot { size = " << s.size << " checksum = " << s.checksum << " }";
}

std::ostream& operator<<(std::ostream& out, InstallSnapshotResponse const& s) {
  return out << "InstallSnapshotResponse { success = " << s.success << " idx = " << s.idx << " }";
}

std::ostream& operator<<(std::ostream& out, RaftRequest const& r) {
  return out << "RaftRequest { " << r.info.epoch << ':' << r.info.request_seq
             << " term = " << r.term << " " << r.request << " }";
}

std::ostream& operator<<(std::ostream& out, RaftResponse const& r) {
  return out << "RaftResponse { " << r.info.epoch << ':' << r.info.request_seq
             << " term = " << r.term << " " << r.response << " }";
}

}  // namespace banyandb::servers::zookeeper
