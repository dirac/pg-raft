#pragma once

#include "src/base/base.h"

struct OpenOp {
  LogSeqNumber lsn;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & lsn;
  }
};

struct WriteOp {
  LogSeqNumber lsn;
  uint32_t     offset;
  uint32_t     length;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & lsn;
    ar & offset;
    ar & length;
  }
};

struct CloseOp {
  LogSeqNumber lsn;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & lsn;
  }
};

struct FlushOp {
  LogSeqNumber lsn;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & lsn;
  }
};

struct WalCommand {
  /// The data before flushed_lsn is ready to be consumed.
  LogSeqNumber flushed_lsn;

  /// Command.
  std::variant<std::monostate, FlushOp, OpenOp, WriteOp, CloseOp> command;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & flushed_lsn;
    ar & command;
  }
};

template <>
struct std::formatter<WalCommand> {
  constexpr auto parse(std::format_parse_context& ctx) {
    auto it = ctx.begin();
    if (it != ctx.end() && *it != '}') {
      throw std::format_error("Invalid format args for WalCommand.");
    }
    return it;
  }

  auto format(WalCommand const& p, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "flushed_lsn={}", p.flushed_lsn);
  }
};

inline std::ostream& operator<<(std::ostream& out, WalCommand const& r) {
  return out << std::format("{}", r);
}
