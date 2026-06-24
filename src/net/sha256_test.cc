#define BOOST_TEST_MODULE sha256
#include "src/net/sha256.h"

#include "src/base/testing.h"

namespace {
Checksum compute(std::string_view s) {
  ChecksumContext ctx;
  init(&ctx);
  update(&ctx, s.data(), s.size());
  return finalize(&ctx);
}
}  // namespace

BOOST_AUTO_TEST_CASE(empty_string, *boost::unit_test::timeout(60)) {
  auto h = compute("");
  BOOST_CHECK_EQUAL(std::string(h),
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

BOOST_AUTO_TEST_CASE(abc, *boost::unit_test::timeout(60)) {
  auto h = compute("abc");
  BOOST_CHECK_EQUAL(std::string(h),
                    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

BOOST_AUTO_TEST_CASE(abc2, *boost::unit_test::timeout(60)) {
  auto h = compute("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
  BOOST_CHECK_EQUAL(std::string(h),
                    "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
}

BOOST_AUTO_TEST_CASE(a_n, *boost::unit_test::timeout(60)) {
  std::string data(1000'000, 'a');
  auto        h = compute(data);
  BOOST_CHECK_EQUAL(std::string(h),
                    "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
}

BOOST_AUTO_TEST_CASE(hex_output_length, *boost::unit_test::timeout(60)) {
  auto h = compute("test");
  BOOST_CHECK_EQUAL(std::string(h).size(), 64u);
}

BOOST_AUTO_TEST_CASE(hex_output_lowercase, *boost::unit_test::timeout(60)) {
  auto        h   = compute("abc");
  std::string hex = std::string(h);
  BOOST_CHECK(
      std::ranges::all_of(hex, [](char c) { return std::isxdigit(c) && !std::isupper(c); }));
}

BOOST_AUTO_TEST_CASE(distinct_inputs_differ, *boost::unit_test::timeout(60)) {
  BOOST_CHECK(std::string(compute("abc")) != std::string(compute("abd")));
  BOOST_CHECK(std::string(compute("")) != std::string(compute("a")));
}

BOOST_AUTO_TEST_CASE(incremental_equals_single, *boost::unit_test::timeout(60)) {
  // Split "hello world" (11 bytes, well under the 32-byte bug threshold) two ways.
  Checksum full = compute("hello world");

  ChecksumContext ctx;
  init(&ctx);
  update(&ctx, "hello", 5);
  update(&ctx, " world", 6);
  Checksum chunked = finalize(&ctx);

  BOOST_CHECK_EQUAL(std::string(full), std::string(chunked));
}

BOOST_AUTO_TEST_CASE(multiple_blocks_incremental, *boost::unit_test::timeout(60)) {
  // Feed 128 bytes one byte at a time vs all at once; both paths share the
  // same finalize() bug so the results must still agree.
  std::string data(128, 'x');

  Checksum full = compute(data);

  ChecksumContext ctx;
  init(&ctx);
  for (char c : data)
    update(&ctx, &c, 1);
  Checksum byte_by_byte = finalize(&ctx);

  BOOST_CHECK_EQUAL(std::string(full), std::string(byte_by_byte));
}
