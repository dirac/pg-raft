#include "src/base/base.h"

#ifndef SRC_BASE_FORMATTER_H_
#define SRC_BASE_FORMATTER_H_

struct parse_fn {
  constexpr auto parse(std::format_parse_context& ctx) {
    auto it = ctx.begin();
    if (it != ctx.end() && *it != '}') {
      throw std::format_error("Invalid format args");
    }
    return it;
  }
};

template <>
struct std::formatter<std::filesystem::path> : public parse_fn {
  auto format(std::filesystem::path const& p, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "{}", p.c_str());
  }
};

template <>
struct std::formatter<LogSeqNumber> : public parse_fn {
  constexpr static auto XLogSegmentsPerXLogId =
      1ULL << 32;  //(static_cast<uint64_t>(0x100000000ULL) / kWalSegmentSize);

  auto format(LogSeqNumber const& p, std::format_context& ctx) const {
    return std::format_to(ctx.out(),
                          "{}/{:X}/{:X}",
                          p.timeline,
                          p.lsn / XLogSegmentsPerXLogId,
                          p.lsn % XLogSegmentsPerXLogId);
  }
};

inline std::ostream& operator<<(std::ostream& out, LogSeqNumber const& lsn) {
  return out << std::format("{}", lsn);
}

template <>
struct std::formatter<struct stat> : public parse_fn {
  auto format(struct stat const& p, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "{}st_size={}{}", "{", p.st_size, "}");
  }
};

template <>
struct std::formatter<std::monostate> : public parse_fn {
  auto format(std::monostate const&, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "<>");
  }
};

inline std::ostream& operator<<(std::ostream& out, std::monostate const& s) {
  return out << std::format("{}", s);
}

template <typename T, typename... A>
struct std::formatter<std::variant<T, A...>> : public parse_fn {
  auto format(std::variant<T, A...> const& v, std::format_context& ctx) const {
    return std::visit([&](auto const& a) { return std::format_to(ctx.out(), "{}", a); }, v);
  }
};

template <typename T, typename... A>
inline std::ostream& operator<<(std::ostream& out, std::variant<T, A...> const& v) {
  return out << std::format("{}", v);
}

template <typename T>
struct std::formatter<std::vector<T>> : public parse_fn {
  auto format(std::vector<T> const& v, std::format_context& ctx) const {
    auto        out = ctx.out();
    auto const* del = "{";
    for (auto const& e : v) {
      out = std::format_to(std::move(out), "{}{}", del, e);
      del = ", ";
    }
    return v.empty() ? std::format_to(std::move(out), "{}", "{}")
                     : std::format_to(std::move(out), "{}", "}");
  }
};

template <typename T>
std::ostream& operator<<(std::ostream& out, std::vector<T> const& vec) {
  return out << std::format("{}", vec);
}

template <typename T, size_t N>
struct std::formatter<inplace_vector<T, N>> : public parse_fn {
  auto format(inplace_vector<T, N> const& v, std::format_context& ctx) const {
    auto        out = ctx.out();
    auto const* del = "{";
    for (auto const& e : v) {
      out = std::format_to(std::move(out), "{}{}", del, e);
      del = ", ";
    }
    return v.empty() ? std::format_to(std::move(out), "{}", "{}")
                     : std::format_to(std::move(out), "{}", "}");
  }
};

template <typename T, size_t N>
std::ostream& operator<<(std::ostream& out, inplace_vector<T, N> const& vec) {
  return out << std::format("{}", vec);
}

template <typename T>
struct std::formatter<std::optional<T>> : public parse_fn {
  auto format(std::optional<T> const& v, std::format_context& ctx) const {
    return v ? std::format_to(ctx.out(), "{}", *v) : std::format_to(ctx.out(), "nil");
  }
};

template <typename T>
std::ostream& operator<<(std::ostream& out, std::optional<T> const& v) {
  return out << std::format("{}", v);
}

#endif
