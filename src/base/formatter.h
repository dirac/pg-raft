#include "src/base/base.h"

#ifndef SRC_BASE_FORMATTER_H_
#define SRC_BASE_FORMATTER_H_

template <>
struct std::formatter<std::filesystem::path> {
  constexpr auto parse(std::format_parse_context& ctx) {
    auto it = ctx.begin();
    if (it != ctx.end() && *it != '}') {
      throw std::format_error("Invalid format args for std::filesystem::path.");
    }
    return it;
  }

  auto format(std::filesystem::path const& p, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "{}", p.c_str());
  }
};

template <>
struct std::formatter<LogSeqNumber> {
  constexpr static auto XLogSegmentsPerXLogId =
      1ULL << 32;  //(static_cast<uint64_t>(0x100000000ULL) / kWalSegmentSize);

  constexpr auto parse(std::format_parse_context& ctx) {
    auto it = ctx.begin();
    if (it != ctx.end() && *it != '}') {
      throw std::format_error("Invalid format args for LogSeqNumber.");
    }
    return it;
  }

  auto format(LogSeqNumber const& p, std::format_context& ctx) const {
    return std::format_to(ctx.out(),
                          "{}/{:X}/{:X}",
                          p.timeline,
                          p.lsn / XLogSegmentsPerXLogId,
                          p.lsn % XLogSegmentsPerXLogId);
  }
};

template <>
struct std::formatter<struct stat> {
  constexpr auto parse(std::format_parse_context& ctx) {
    auto it = ctx.begin();
    if (it != ctx.end() && *it != '}') {
      throw std::format_error("Invalid format args for struct stat.");
    }
    return it;
  }

  auto format(struct stat const& p, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "{}st_size={}{}", "{", p.st_size, "}");
  }
};

#endif
