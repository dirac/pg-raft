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

template <>
struct std::formatter<RequestElection> : public parse_fn {
  auto format(RequestElection const& r, std::format_context& ctx) const {
    return std::format_to(
        ctx.out(), "RequestElection {{ (term, index) = {} }}", std::get<0>(r.last_entry));
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

template <>
struct std::formatter<RequestElectionResponse> : public parse_fn {
  auto format(RequestElectionResponse const& r, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "RequestElectionResponse {{ success={} }}", r.success);
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

template <>
struct std::formatter<RequestVote> : public parse_fn {
  auto format(RequestVote const& r, std::format_context& ctx) const {
    return std::format_to(ctx.out(),
                          "RequestVote {{ term={} index={} }}",
                          std::get<RaftTerm>(r.last_entry),
                          std::get<RaftIndex>(r.last_entry));
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

template <>
struct std::formatter<RequestVoteResponse> : public parse_fn {
  auto format(RequestVoteResponse const& r, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "RequestVoteResponse {{ vote_granted={} }}", r.vote_granted);
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

template <>
struct std::formatter<Entry> : public parse_fn {
  auto format(Entry const& e, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "E{{ time={} term={} cmd={} }}", e.time, e.term, e.cmd);
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
std::ostream& operator<<(std::ostream& out, AppendEntries const& ae);

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
std::ostream& operator<<(std::ostream& out, AppendEntriesResponse const& r);

struct InstallSnapshot {
  RaftIndex prev_log_idx;

  /// About a 1M entries max.
  vector<Entry> entries;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & prev_log_idx;
    ar & entries;
  }
};

template <>
struct std::formatter<InstallSnapshot> : public parse_fn {
  auto format(InstallSnapshot const& s, std::format_context& ctx) const {
    return std::format_to(ctx.out(),
                          "InstallSnapshot {{ prev_log_idx={} entries={} }}",
                          s.prev_log_idx,
                          s.entries.size());
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

template <>
struct std::formatter<InstallSnapshotResponse> : public parse_fn {
  auto format(InstallSnapshotResponse const& s, std::format_context& ctx) const {
    return std::format_to(
        ctx.out(), "InstallSnapshotResponse {{ idx={} success={} }}", s.idx, s.success);
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

template <>
struct std::formatter<RequestResponseInfo> : public parse_fn {
  auto format(RequestResponseInfo const& r, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "{}:{}", r.epoch, r.request_seq);
  }
};

struct MemBlock {
  LogSeqNumber lsn;     // The file lsn.
  off_t        offset;  // The offset into the segment.
  size_t       length;  // The size of the block.

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & lsn;
    ar & offset;
    ar & length;
  }
};

template <>
struct std::formatter<MemBlock> : public parse_fn {
  auto format(MemBlock const& m, std::format_context& ctx) const {
    return std::format_to(
        ctx.out(), "MemBlock {{ lsn={} offset={} length={} }}", m.lsn, m.offset, m.length);
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

template <>
struct std::formatter<RaftRequest> : public parse_fn {
  auto format(RaftRequest const& r, std::format_context& ctx) const {
    return std::format_to(
        ctx.out(), "RaftRequest {{ {} term={} {} data={} }}", r.info, r.term, r.request, r.data);
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

template <>
struct std::formatter<RaftResponse> : public parse_fn {
  auto format(RaftResponse const& r, std::format_context& ctx) const {
    return std::format_to(
        ctx.out(), "RaftResponse {{ {} term={} {} }}", r.info, r.term, r.response);
  }
};
std::ostream& operator<<(std::ostream& out, RaftResponse const& r);
