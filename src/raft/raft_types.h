#pragma once

#include "src/base/base.h"
#include "src/raft/wal_types.h"

struct RequestElection {
  std::tuple<RaftTerm, RaftIndex> last_entry;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & last_entry;
  }
};
std::ostream& operator<<(std::ostream& out, RequestElection const& r);

struct RequestElectionResponse {
  bool success;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & success;
  }
};
std::ostream& operator<<(std::ostream& out, RequestElectionResponse const& r);

struct RequestVote {
  std::tuple<RaftTerm, RaftIndex> last_entry;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & last_entry;
  }
};
std::ostream& operator<<(std::ostream& out, RequestVote const& r);

struct RequestVoteResponse {
  bool vote_granted;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & vote_granted;
  }
};
std::ostream& operator<<(std::ostream& out, RequestVoteResponse const& r);

struct Entry {
  std::time_t time;  // The timestamp when the entry was created.
  RaftTerm    term;  // The term this entry was created.

  std::variant<std::monostate, WalCommand> cmd;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & time;
    ar & term;
    ar & cmd;
  }
};
std::ostream& operator<<(std::ostream& out, Entry const& e);

struct AppendEntries {
  RaftTerm  prev_log_term;
  RaftIndex prev_log_idx;

  RaftIndex commit_idx;

  vector<Entry> entries;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & prev_log_term;
    ar & prev_log_idx;
    ar & commit_idx;
    ar & entries;
  }
};
std::ostream& operator<<(std::ostream& out, AppendEntries const& ae);

struct AppendEntriesResponse {
  /** true if follower contained entry matching prev_log_idx and prev_log_term */
  bool success;

  /**
   * On success it is the index of the last entry.
   * Otherwise the min of prev_log_idx and the index of the last entry.
   */
  RaftIndex idx;

  /** The term at the idx. */
  std::optional<RaftTerm> term;

  /** n or more entries with the same term (and not committed). */
  std::optional<uint64_t> n;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & success;
    ar & idx;
    ar & term;
    ar & n;
  }
};
std::ostream& operator<<(std::ostream& out, AppendEntriesResponse const& r);

struct InstallSnapshot {
  /// Snapshot size, the snapshot will be send after sending this request.
  uint64_t size;

  /// The sha256 of the file.
  string checksum;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & size;
    ar & checksum;
  }
};
std::ostream& operator<<(std::ostream& out, InstallSnapshot const& s);

struct InstallSnapshotResponse {
  /** the current log index. */
  RaftIndex idx;

  /** true */
  bool success;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & idx;
    ar & success;
  }
};
std::ostream& operator<<(std::ostream& out, InstallSnapshotResponse const& s);

struct RequestResponseInfo {
  /* The epoch when the request got created. Useful to track round trip timing. */
  uint32_t epoch;

  /* The term that the server sent. */
  RaftTerm request_term;

  /* Index into outstanding requests array. */
  uint16_t request_idx;

  /* The request sequence. */
  uint32_t request_seq;

  /* log */
  bool log;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & epoch;
    ar & request_term;
    ar & request_idx;
    ar & request_seq;
    ar & log;
  }
};

struct RaftRequest {
  /* The current term in the server. */
  RaftTerm term;

  /* Node that generated the request. */
  RaftNodeId node_id;

  std::variant<std::monostate, RequestVote, AppendEntries, InstallSnapshot, RequestElection>
      request;

  /* Copied from request to associated response. */
  RequestResponseInfo info;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & term;
    ar & node_id;
    ar & request;
    ar & info;
  }
};
std::ostream& operator<<(std::ostream& out, RaftRequest const& r);

struct RaftResponse {
  /** currentTerm, to force other leader/candidate to step down */
  RaftTerm term;

  /** The node that generated the response. */
  RaftNodeId node_id;

  /** the response. */
  std::variant<std::monostate,
               RequestVoteResponse,
               AppendEntriesResponse,
               InstallSnapshotResponse,
               RequestElectionResponse>
      response;

  /* Copied from request to associated response. */
  RequestResponseInfo info;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & term;
    ar & node_id;
    ar & response;
    ar & info;
  }
};
std::ostream& operator<<(std::ostream& out, RaftResponse const& r);

