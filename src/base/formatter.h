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

#endif
