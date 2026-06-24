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

struct RequestElectionResponse {
  bool success;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & success;
  }
};

struct RequestVote {
  std::tuple<RaftTerm, RaftIndex> last_entry;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & last_entry;
  }
};

struct RequestVoteResponse {
  bool vote_granted;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & vote_granted;
  }
};

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

struct AppendEntriesResponse {
  /** true if follower contained entry matching prev_log_idx and prev_log_term */
  bool success;

  /** true if the standby is consuming the logs. The vote is counted only when we have success and
   * running as both true. */
  bool running;

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

struct InstallSnapshot {
  RaftIndex log_idx;

  /// About a 1M entries max.
  vector<Entry> entries;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & log_idx;
    ar & entries;
  }
};

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

struct SnapshotRequest {
  RaftIndex applied_idx;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & applied_idx;
  }
};

struct SnapshotResponse {
  bool success;

  /** files that follow this response. */
  vector<string> files;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & success;
    ar & files;
  }
};

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

  /* respond to the request. */
  bool respond;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & epoch;
    ar & request_term;
    ar & request_idx;
    ar & request_seq;
    ar & log;
    ar & respond;
  }
};

struct MemBlock {
  LogSeqNumber lsn;     // The file lsn.
  off_t        offset;  // The offset into the segment.
  size_t       size;    // The size of the block.

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & lsn;
    ar & offset;
    ar & size;
  }
};

struct RaftRequest {
  /* The current term in the server. */
  RaftTerm term;

  /* Node that generated the request. */
  RaftNodeId node_id;

  std::variant<std::monostate,
               RequestVote,
               AppendEntries,
               InstallSnapshot,
               RequestElection,
               SnapshotRequest>
      request;

  /* Copied from request to associated response. */
  RequestResponseInfo info;

  /* Segment mem blocks. */
  inplace_vector<MemBlock, 2> data;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & term;
    ar & node_id;
    ar & request;
    ar & info;
    ar & data;
  }
};

struct RaftResponse {
  /** currentTerm, to force other leader/candidate to step down */
  RaftTerm term;

  /** The node that generated the response. */
  RaftNodeId node_id;

  /** Leader node */
  std::optional<RaftNodeId> leader;

  /** The following two are extracted using the statement:
      SELECT
        CASE
            WHEN pg_is_in_recovery() THEN NULL
            ELSE pg_current_wal_flush_lsn()::numeric
        END AS primary_flushed_lsn,
        CASE
            WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()::numeric
            ELSE NULL
        END AS standby_applied_lsn;
  */
  std::optional<uint64_t> primary_flushed_lsn;
  std::optional<uint64_t> standby_applied_lsn;

  /** the response. */
  std::variant<std::monostate,
               RequestVoteResponse,
               AppendEntriesResponse,
               InstallSnapshotResponse,
               RequestElectionResponse,
               SnapshotResponse>
      response;

  /* Copied from request to associated response. */
  RequestResponseInfo info;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & term;
    ar & node_id;
    ar & leader;
    ar & primary_flushed_lsn;
    ar & standby_applied_lsn;
    ar & response;
    ar & info;
  }
};

// Formatters

template <>
struct std::formatter<RequestElection> : public parse_fn {
  auto format(RequestElection const& r, std::format_context& ctx) const {
    return std::format_to(
        ctx.out(), "RequestElection {{ (term, index) = {} }}", std::get<0>(r.last_entry));
  }
};
inline std::ostream& operator<<(std::ostream& out, RequestElection const& r) {
  return out << std::format("{}", r);
}

template <>
struct std::formatter<RequestElectionResponse> : public parse_fn {
  auto format(RequestElectionResponse const& r, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "RequestElectionResponse {{ success={} }}", r.success);
  }
};
inline std::ostream& operator<<(std::ostream& out, RequestElectionResponse const& r) {
  return out << std::format("{}", r);
}

template <>
struct std::formatter<RequestVote> : public parse_fn {
  auto format(RequestVote const& r, std::format_context& ctx) const {
    return std::format_to(ctx.out(),
                          "RequestVote {{ term={} index={} }}",
                          std::get<RaftTerm>(r.last_entry),
                          std::get<RaftIndex>(r.last_entry));
  }
};
inline std::ostream& operator<<(std::ostream& out, RequestVote const& r) {
  return out << std::format("{}", r);
}

template <>
struct std::formatter<RequestVoteResponse> : public parse_fn {
  auto format(RequestVoteResponse const& r, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "RequestVoteResponse {{ vote_granted={} }}", r.vote_granted);
  }
};
inline std::ostream& operator<<(std::ostream& out, RequestVoteResponse const& r) {
  return out << std::format("{}", r);
}

template <>
struct std::formatter<Entry> : public parse_fn {
  auto format(Entry const& e, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "E{{ time={} term={} cmd={} }}", e.time, e.term, e.cmd);
  }
};
inline std::ostream& operator<<(std::ostream& out, Entry const& e) {
  return out << std::format("{}", e);
}

template <>
struct std::formatter<AppendEntries> : public parse_fn {
  auto format(AppendEntries const& ae, std::format_context& ctx) const {
    return std::format_to(
        ctx.out(),
        "AppendEntries {{ prev_log_idx={} prev_log_term={} commit_idx={} entries={} }}",
        ae.prev_log_idx,
        ae.prev_log_term,
        ae.commit_idx,
        ae.entries);
  }
};
inline std::ostream& operator<<(std::ostream& out, AppendEntries const& ae) {
  return out << std::format("{}", ae);
}

template <>
struct std::formatter<AppendEntriesResponse> : public parse_fn {
  auto format(AppendEntriesResponse const& r, std::format_context& ctx) const {
    auto out = std::format_to(ctx.out(),
                              "AppendEntriesResponse {{ success={} running={} idx={}",
                              r.success,
                              r.running,
                              r.idx);
    if (r.term)
      out = std::format_to(std::move(out), " term={}", *r.term);
    if (r.n)
      out = std::format_to(std::move(out), " n={}", *r.n);
    return std::format_to(std::move(out), " }}");
  }
};
inline std::ostream& operator<<(std::ostream& out, AppendEntriesResponse const& r) {
  return out << std::format("{}", r);
}

template <>
struct std::formatter<InstallSnapshot> : public parse_fn {
  auto format(InstallSnapshot const& s, std::format_context& ctx) const {
    return std::format_to(
        ctx.out(), "InstallSnapshot {{ log_idx={} entries={} }}", s.log_idx, s.entries.size());
  }
};
inline std::ostream& operator<<(std::ostream& out, InstallSnapshot const& s) {
  return out << std::format("{}", s);
}

template <>
struct std::formatter<InstallSnapshotResponse> : public parse_fn {
  auto format(InstallSnapshotResponse const& s, std::format_context& ctx) const {
    return std::format_to(
        ctx.out(), "InstallSnapshotResponse {{ idx={} success={} }}", s.idx, s.success);
  }
};
inline std::ostream& operator<<(std::ostream& out, InstallSnapshotResponse const& s) {
  return out << std::format("{}", s);
}

template <>
struct std::formatter<SnapshotRequest> : public parse_fn {
  auto format(SnapshotRequest const& s, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "SnapshotRequest {{ log_idx={} }}", s.applied_idx);
  }
};
inline std::ostream& operator<<(std::ostream& out, SnapshotRequest const& s) {
  return out << std::format("{}", s);
}

template <>
struct std::formatter<SnapshotResponse> : public parse_fn {
  auto format(SnapshotResponse const& s, std::format_context& ctx) const {
    return std::format_to(
        ctx.out(), "SnapshotResponse {{ success={} files={} }}", s.success, s.files);
  }
};
inline std::ostream& operator<<(std::ostream& out, SnapshotResponse const& s) {
  return out << std::format("{}", s);
}

template <>
struct std::formatter<RequestResponseInfo> : public parse_fn {
  auto format(RequestResponseInfo const& r, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "{}:{}", r.epoch, r.request_seq);
  }
};

template <>
struct std::formatter<MemBlock> : public parse_fn {
  auto format(MemBlock const& m, std::format_context& ctx) const {
    return std::format_to(
        ctx.out(), "MemBlock {{ lsn={} offset={} size={} }}", m.lsn, m.offset, m.size);
  }
};

template <>
struct std::formatter<RaftRequest> : public parse_fn {
  auto format(RaftRequest const& r, std::format_context& ctx) const {
    return std::format_to(
        ctx.out(), "RaftRequest {{ {} term={} {} data={} }}", r.info, r.term, r.request, r.data);
  }
};
inline std::ostream& operator<<(std::ostream& out, RaftRequest const& r) {
  return out << std::format("{}", r);
}

template <>
struct std::formatter<RaftResponse> : public parse_fn {
  auto format(RaftResponse const& r, std::format_context& ctx) const {
    return std::format_to(
        ctx.out(), "RaftResponse {{ {} term={} {} }}", r.info, r.term, r.response);
  }
};
inline std::ostream& operator<<(std::ostream& out, RaftResponse const& r) {
  return out << std::format("{}", r);
}
