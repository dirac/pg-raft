#include "src/base/xxhash.h"

namespace {

/* Magic prime constants used by xxHash32 */
constexpr uint32_t kPrime32_1 = 2654435761U;
constexpr uint32_t kPrime32_2 = 2246822519U;
constexpr uint32_t kPrime32_3 = 3266489917U;
constexpr uint32_t kPrime32_4 = 668265263U;
constexpr uint32_t kPrime32_5 = 374761393U;

constexpr uint32_t rotl32(uint32_t x, int r) {
  return std::rotl(x, r);
}

/* Read a 32-bit little-endian integer */
uint32_t read_le32(uint8_t const* ptr) {
  return static_cast<uint32_t>(ptr[0]) | (static_cast<uint32_t>(ptr[1]) << 8U) |
         (static_cast<uint32_t>(ptr[2]) << 16U) | (static_cast<uint32_t>(ptr[3]) << 24U);
}

size_t length(std::vector<::boost::asio::const_buffer> const& buffers) {
  return std::accumulate(std::begin(buffers),
                         std::end(buffers),
                         static_cast<size_t>(0),
                         [](size_t result, auto const& b) { return result + b.size(); });
}

size_t length(::boost::asio::const_buffer const& buffer) {
  return buffer.size();
}

/// Walks a sequence of buffers, returning |len| contiguous bytes at a time. When the bytes span
/// buffers they are copied into a temporary.
struct IterV {
  size_t elem;
  size_t offset;
  size_t length;

  std::vector<::boost::asio::const_buffer> const& buffers;

  std::array<uint8_t, 16> tmp;

  uint8_t const* next(unsigned len) {
    assert(len > 0 && len <= tmp.size());

    if (length < len)
      return nullptr;

    // Skip exhausted (and empty) buffers.
    while (offset == buffers.at(elem).size()) {
      ++elem;
      offset = 0U;
    }

    auto const& buff = buffers.at(elem);
    if ((offset + len) <= buff.size()) [[likely]] {
      uint8_t const* result = reinterpret_cast<uint8_t const*>(buff.data()) + offset;
      offset += len;
      length -= len;
      return result;
    }

    // Spans buffers: copy byte by byte.
    for (unsigned idx = 0; idx < len; ++idx)
      tmp.at(idx) = *next(1U);

    return tmp.data();
  }
};

IterV begin(std::vector<::boost::asio::const_buffer> const& buffers) {
  return IterV{.elem = 0, .offset = 0, .length = length(buffers), .buffers = buffers, .tmp = {}};
}

struct Iter {
  size_t offset;
  size_t length;

  ::boost::asio::const_buffer buff;

  uint8_t const* next(unsigned len) {
    assert(len > 0);

    if (length < len)
      return nullptr;

    uint8_t const* result = reinterpret_cast<uint8_t const*>(buff.data()) + offset;

    offset += len;
    length -= len;

    return result;
  }
};

Iter begin(::boost::asio::const_buffer buff) {
  return Iter{.offset = 0, .length = length(buff), .buff = buff};
}

template <typename T>
uint32_t xxhash32_t(T const& input, uint32_t seed) {
  uint32_t h32;

  auto len  = length(input);
  auto iter = begin(input);

  uint8_t const* p = nullptr;
  if (p = iter.next(16); p) {
    /* Initialize internal states using the primes and the seed */
    uint32_t v1 = seed + kPrime32_1 + kPrime32_2;
    uint32_t v2 = seed + kPrime32_2;
    uint32_t v3 = seed + 0;
    uint32_t v4 = seed - kPrime32_1;

    /* Bulk loop: process data in chunks of 16 bytes */
    do {
      v1 += read_le32(p) * kPrime32_2;
      v1 = rotl32(v1, 13);
      v1 *= kPrime32_1;
      p += 4;
      v2 += read_le32(p) * kPrime32_2;
      v2 = rotl32(v2, 13);
      v2 *= kPrime32_1;
      p += 4;
      v3 += read_le32(p) * kPrime32_2;
      v3 = rotl32(v3, 13);
      v3 *= kPrime32_1;
      p += 4;
      v4 += read_le32(p) * kPrime32_2;
      v4 = rotl32(v4, 13);
      v4 *= kPrime32_1;
    } while ((p = iter.next(16), p));

    /* Converge internal states into a single hash accumulator */
    h32 = rotl32(v1, 1) + rotl32(v2, 7) + rotl32(v3, 12) + rotl32(v4, 18);
  } else {
    /* Input is smaller than 16 bytes */
    h32 = seed + kPrime32_5;
  }

  h32 += static_cast<uint32_t>(len);

  /* Remaining 4-byte fields */
  while ((p = iter.next(4), p)) {
    h32 += read_le32(p) * kPrime32_3;
    h32 = rotl32(h32, 17) * kPrime32_4;
  }

  /* Remaining single bytes */
  while ((p = iter.next(1), p)) {
    h32 += (*p) * kPrime32_5;
    h32 = rotl32(h32, 11) * kPrime32_1;
  }

  /* Final avalanche */
  h32 ^= h32 >> 15U;
  h32 *= kPrime32_2;
  h32 ^= h32 >> 13U;
  h32 *= kPrime32_3;
  h32 ^= h32 >> 16U;

  return h32;
}

}  // namespace

uint32_t xxhash32(::boost::asio::const_buffer const& input, uint32_t seed) {
  return xxhash32_t(input, seed);
}

uint32_t xxhash32(std::vector<::boost::asio::const_buffer> const& input, uint32_t seed) {
  return xxhash32_t(input, seed);
}
