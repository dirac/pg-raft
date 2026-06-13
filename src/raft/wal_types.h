#pragma once

#include "src/base/base.h"

struct OpenOp {
  template <typename Archive>
  void serialize(Archive& /*ar*/, unsigned int /* version */) {}
};

struct WriteOp {
  off_t  offset;
  size_t size;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & offset;
    ar & size;
  }
};

struct FlushOp {
  off_t  offset;
  size_t size;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & offset;
    ar & size;
  }
};

struct CloseOp {
  template <typename Archive>
  void serialize(Archive& /*ar*/, unsigned int /* version */) {}
};

struct WalCommand {
  /// The data before flushed_lsn is ready to be consumed.
  LogSeqNumber flushed_lsn;

  /// The op is w.r.t the segment.
  std::optional<LogSeqNumber> segment;

  /// Command.
  std::variant<std::monostate, FlushOp, OpenOp, WriteOp, CloseOp> command;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & flushed_lsn;
    ar & segment;
    ar & command;
  }
};

// Formatters

template <>
struct std::formatter<OpenOp> : public parse_fn {
  auto format(OpenOp const&, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "open_op");
  }
};
inline std::ostream& operator<<(std::ostream& out, OpenOp const& r) {
  return out << std::format("{}", r);
}

template <>
struct std::formatter<WriteOp> : public parse_fn {
  auto format(WriteOp const&, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "write_op");
  }
};
inline std::ostream& operator<<(std::ostream& out, WriteOp const& r) {
  return out << std::format("{}", r);
}

template <>
struct std::formatter<CloseOp> : public parse_fn {
  auto format(CloseOp const&, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "close_op");
  }
};
inline std::ostream& operator<<(std::ostream& out, CloseOp const& r) {
  return out << std::format("{}", r);
}

template <>
struct std::formatter<FlushOp> : public parse_fn {
  auto format(FlushOp const&, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "flush_op");
  }
};
inline std::ostream& operator<<(std::ostream& out, FlushOp const& r) {
  return out << std::format("{}", r);
}

template <>
struct std::formatter<WalCommand> : public parse_fn {
  auto format(WalCommand const& p, std::format_context& ctx) const {
    return std::format_to(
        ctx.out(), "flushed_lsn={} segment={} cmd={}", p.flushed_lsn, p.segment, p.command);
  }
};
inline std::ostream& operator<<(std::ostream& out, WalCommand const& r) {
  return out << std::format("{}", r);
}
