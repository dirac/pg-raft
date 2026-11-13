#define BOOST_TEST_MODULE base
#include "src/base/base.h"

#include "src/base/testing.h"

// log_seq_number(): a WAL segment's file name to its timeline and start LSN.

BOOST_AUTO_TEST_CASE(log_seq_number_round_trip) {
  for (auto lsn :
       {LogSeqNumber{.timeline = 1, .lsn = 0},
        LogSeqNumber{.timeline = 2, .lsn = 3 * kWalSegmentSize},
        LogSeqNumber{.timeline = 0xABCDEF01, .lsn = (uint64_t{0x12} << 32U) + 0xFF000000}}) {
    auto name = filename(lsn);
    BOOST_TEST(name.size() == 24U);
    auto parsed = log_seq_number(name.c_str());
    BOOST_REQUIRE(parsed.has_value());
    BOOST_TEST((*parsed == lsn));
    BOOST_TEST((log_seq_number(("/" + name).c_str()) == lsn));  // a FUSE path
  }
}

BOOST_AUTO_TEST_CASE(log_seq_number_parses_hex) {
  auto p = log_seq_number("00000003000000010000002a");  // lower case too
  BOOST_REQUIRE(p.has_value());
  BOOST_TEST(p->timeline == 3U);
  BOOST_TEST(p->lsn == (uint64_t{1} << 32U) + 0x2a * kWalSegmentSize);
}

BOOST_AUTO_TEST_CASE(log_seq_number_rejects) {
  for (char const* name : {"",
                           "/",
                           "00000001000000000000000",    // 23 digits
                           "0000000100000000000000001",  // 25
                           "000000010000000000000003.partial",
                           ".000000010000000000000003",
                           "00000001.history",
                           "0000000G0000000000000003",
                           "00000001 000000000000003",
                           "xlogtemp.123"})
    BOOST_TEST(!log_seq_number(name).has_value(), name);
}
