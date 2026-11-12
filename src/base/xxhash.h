#pragma once

#include "src/base/base.h"

/// xxHash32 (https://github.com/Cyan4973/xxHash).
/// Used as the frame checksum in src/net.

uint32_t xxhash32(::boost::asio::const_buffer const& input, uint32_t seed = 0);

/// The hash of the concatenation of the buffers.
uint32_t xxhash32(std::vector<::boost::asio::const_buffer> const& input, uint32_t seed = 0);

inline uint32_t xxhash32(void const* data, size_t size, uint32_t seed = 0) {
  return xxhash32(::boost::asio::const_buffer(data, size), seed);
}

inline uint32_t xxhash32(Bytes const& bytes, uint32_t seed = 0) {
  return xxhash32(::boost::asio::const_buffer(bytes.data(), bytes.size()), seed);
}
