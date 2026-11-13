#pragma once

#include "src/base/base.h"

// The command of a raft log entry. The primary generates the commands itself, from the WAL writes
// that the FUSE layer intercepts.

struct TimelineOp {
  uint64_t timeline;
  string   content;

  bool operator==(TimelineOp const&) const = default;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar& timeline;
    ar& content;
  }
};

struct OpenOp {
  bool operator==(OpenOp const&) const = default;

  template <typename Archive>
  void serialize(Archive& /*ar*/, unsigned int /* version */) {}
};

/// The bytes [offset, offset + size) of the segment. The bytes are not in the entry: they are sent
/// with the AppendEntries request (see MemBlock).
struct WriteOp {
  off_t  offset;
  size_t size;

  bool operator==(WriteOp const&) const = default;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar& offset;
    ar& size;
  }
};

/// PostgreSQL's fsync of the segment: the durability barrier. PostgreSQL's fsync returns when the
/// entry is committed. It carries the WAL written since the previous flush: the bytes
/// [offset, offset + size) of the segment, sent with the AppendEntries request like a WriteOp's
/// (size 0: nothing new). The node sends the writes with the flush, one entry per fsync, instead of
/// a WriteOp per write.
struct FlushOp {
  off_t  offset;
  size_t size;

  bool operator==(FlushOp const&) const = default;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar& offset;
    ar& size;
  }
};

struct CloseOp {
  bool operator==(CloseOp const&) const = default;

  template <typename Archive>
  void serialize(Archive& /*ar*/, unsigned int /* version */) {}
};

struct WalCommand;

/// The WAL bytes an entry carries, [offset, offset + size) of cmd.segment: a WriteOp's, or a
/// FlushOp's (see FlushOp); nothing for the other commands or without a segment.
std::optional<std::pair<off_t, size_t>> wal_bytes(WalCommand const& cmd);

struct WalCommand {
  /// The data before flushed_lsn is ready to be consumed.
  /// This is the current flushed lsn at the primary. Given this is computed by polling, the value
  /// is stale but safe. When this entry is committed at the follower, the wal sender is advanced to
  /// the flushed_lsn.
  LogSeqNumber flushed_lsn{};

  /// The op is w.r.t the segment.
  std::optional<LogSeqNumber> segment;

  /// Command; monostate for a tick entry.
  std::variant<std::monostate, FlushOp, OpenOp, WriteOp, CloseOp, TimelineOp> command;

  bool operator==(WalCommand const&) const = default;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar& flushed_lsn;
    ar& segment;
    ar& command;
  }
};

inline std::optional<std::pair<off_t, size_t>> wal_bytes(WalCommand const& cmd) {
  if (!cmd.segment)
    return std::nullopt;
  if (auto const* w = std::get_if<WriteOp>(&cmd.command); w && w->size)
    return std::pair{w->offset, w->size};
  if (auto const* f = std::get_if<FlushOp>(&cmd.command); f && f->size)
    return std::pair{f->offset, f->size};
  return std::nullopt;
}

// Formatters

template <>
struct std::formatter<OpenOp> : public parse_fn {
  auto format(OpenOp const&, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "open_op");
  }
};

template <>
struct std::formatter<WriteOp> : public parse_fn {
  auto format(WriteOp const& w, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "write_op {{ offset = {} size = {} }}", w.offset, w.size);
  }
};

template <>
struct std::formatter<CloseOp> : public parse_fn {
  auto format(CloseOp const&, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "close_op");
  }
};

template <>
struct std::formatter<FlushOp> : public parse_fn {
  auto format(FlushOp const& f, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "flush_op {{ offset = {} size = {} }}", f.offset, f.size);
  }
};

template <>
struct std::formatter<TimelineOp> : public parse_fn {
  auto format(TimelineOp const& t, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "timeline_op {}", t.timeline);
  }
};

template <>
struct std::formatter<WalCommand> : public parse_fn {
  auto format(WalCommand const& p, std::format_context& ctx) const {
    return std::format_to(
        ctx.out(), "flushed_lsn={} segment={} cmd={}", p.flushed_lsn, p.segment, p.command);
  }
};
