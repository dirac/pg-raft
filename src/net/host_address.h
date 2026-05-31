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

inline std::ostream& operator<<(std::ostream& out, HostAddress const& address) {
  return out << "{" << address.address << ":" << address.port << "}";
}
