#define BOOST_TEST_MODULE xxhash
#include "src/base/xxhash.h"

#include "src/base/testing.h"

namespace {

Bytes sequential(size_t n, unsigned modulo = 251) {
  Bytes result(n);
  for (size_t idx = 0; idx < n; ++idx)
    result[idx] = static_cast<uint8_t>(idx % modulo);
  return result;
}

}  // namespace

/* Reference values cross-checked against the reference xxHash32 implementation
 * (https://github.com/Cyan4973/xxHash). */
BOOST_AUTO_TEST_CASE(known_vectors) {
  BOOST_TEST(xxhash32("", 0, 0) == 0x02CC5D05U);
  BOOST_TEST(xxhash32("", 0, 42) == 0xD5BE6EB8U);
  BOOST_TEST(xxhash32("a", 1, 0) == 0x550D7456U);
  BOOST_TEST(xxhash32("ab", 2, 0) == 0x4999FC53U);
  BOOST_TEST(xxhash32("abc", 3, 0) == 0x32D153FFU);
  BOOST_TEST(xxhash32("abcd", 4, 0) == 0xA3643705U);
  BOOST_TEST(xxhash32("abcde", 5, 0) == 0x9738F19BU);
  BOOST_TEST(xxhash32("abcdefghijklmno", 15, 0) == 0xB918A375U);
  BOOST_TEST(xxhash32("abcdefghijklmnop", 16, 0) == 0x9D2D8B62U);
  BOOST_TEST(xxhash32("abcdefghijklmnopq", 17, 0) == 0xB3B873E1U);
  BOOST_TEST(xxhash32("abcdefghijklmnopqrstuvwxyz", 26, 0) == 0x63A14D5FU);

  auto hundred = sequential(100);
  BOOST_TEST(xxhash32(hundred, 0) == 0x7F89BA44U);
  BOOST_TEST(xxhash32(hundred, 123) == 0xC463B411U);

  auto thousand = sequential(1000);
  BOOST_TEST(xxhash32(thousand, 999) == 0x0EB33313U);
}

BOOST_AUTO_TEST_CASE(different_seeds_and_lengths_differ) {
  auto data = sequential(64);
  BOOST_TEST(xxhash32(data, 0) != xxhash32(data, 1));
  BOOST_TEST(xxhash32(sequential(10), 0) != xxhash32(sequential(11), 0));
}

/* Splitting the same bytes across buffers at every offset must give the hash of the
 * concatenation (splits inside a 16-byte block and inside a trailing 4-byte field). */
BOOST_AUTO_TEST_CASE(multi_buffer_matches_single_buffer) {
  for (size_t len : {0UL, 1UL, 4UL, 15UL, 16UL, 17UL, 31UL, 32UL, 100UL}) {
    auto data     = sequential(len);
    auto expected = xxhash32(data, 55);

    for (size_t split = 0; split <= len; ++split) {
      std::vector<::boost::asio::const_buffer> parts;
      parts.emplace_back(data.data(), split);
      parts.emplace_back(data.data() + split, len - split);

      BOOST_TEST(xxhash32(parts, 55) == expected);
    }
  }
}

BOOST_AUTO_TEST_CASE(multi_buffer_single_byte_chunks) {
  auto data     = sequential(50);
  auto expected = xxhash32(data, 3);

  std::vector<::boost::asio::const_buffer> parts;
  for (size_t idx = 0; idx < data.size(); ++idx)
    parts.emplace_back(data.data() + idx, 1);

  BOOST_TEST(xxhash32(parts, 3) == expected);
}

BOOST_AUTO_TEST_CASE(multi_buffer_empty_buffers_ignored) {
  auto data     = sequential(20);
  auto expected = xxhash32(data, 9);

  std::vector<::boost::asio::const_buffer> parts;
  parts.emplace_back(data.data(), 0);
  parts.emplace_back(data.data(), 5);
  parts.emplace_back(data.data() + 5, 0);
  parts.emplace_back(data.data() + 5, 15);
  parts.emplace_back(data.data() + 20, 0);

  BOOST_TEST(xxhash32(parts, 9) == expected);
}
