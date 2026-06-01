#pragma once

#include "src/base/base.h"

struct HostAddress {
  string const   address;
  uint16_t const port{0};

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & address;
    ar & port;
  }

  bool operator==(HostAddress const& that) const {
    return address == that.address && port == that.port;
  }

  bool operator<(HostAddress const& that) const {
    return address == that.address ? port < that.port : address < that.address;
  }

  string to_string() const { return format("{0}:{1}", address, port); }

  HostAddress with_address(string const& address) const {
    return HostAddress{
        .address = address,
        .port    = port,
    };
  }
};

template <>
struct std::formatter<HostAddress> {
  constexpr auto parse(std::format_parse_context& ctx) {
    auto it = ctx.begin();
    if (it != ctx.end() && *it != '}') {
      throw std::format_error("Invalid format args for HostAddress.");
    }
    return it;
  }

  auto format(HostAddress const& p, std::format_context& ctx) const {
    return std::format_to(ctx.out(), "{}:{}", p.address, p.port);
  }
};

inline std::ostream& operator<<(std::ostream& out, HostAddress const& address) {
  return out << "{" << address.address << ":" << address.port << "}";
}
