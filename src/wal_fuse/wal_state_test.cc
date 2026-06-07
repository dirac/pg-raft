#define BOOST_TEST_MODULE wal_state_test

#include "src/base/testing.h"
#include "src/wal_fuse/wal_segment.h"

std::string_view         wal_data();
std::vector<std::string> wal_data_exp();

template <typename A, typename B>
std::tuple<off_t, size_t, bool> t(A a, B b) {
  return std::tuple<off_t, size_t, bool>{static_cast<off_t>(a), static_cast<size_t>(b), false};
}

template <typename A, typename B>
std::tuple<off_t, size_t, bool> r(A a, B b) {
  return std::tuple<off_t, size_t, bool>{static_cast<off_t>(a), static_cast<size_t>(b), true};
}

namespace std {
ostream& operator<<(ostream& out, tuple<off_t, size_t, bool> const& t) {
  return out << "{" << get<0>(t) << ", " << get<1>(t) << "}";
}
ostream& operator<<(ostream& out, tuple<off_t, size_t, int> const& t) {
  return out << get<2>(t) << ": {" << get<0>(t) << ", " << get<1>(t) << "}";
}
}  // namespace std

using WalState = WalSegment::WalState;

std::vector<std::tuple<off_t, size_t, int>> expected(std::string base) {
  std::vector<std::tuple<off_t, size_t, int>> result;

  auto compute_lsn = [](std::string const& lsn) -> off_t {
    off_t off = 0;
    for (auto ch : lsn) {
      off = off * 16 + (ch >= '1' && ch <= '9' ? (ch - '0') : 0) +
            (ch >= 'A' && ch <= 'F' ? (ch - 'A' + 10) : 0);
    }
    return off;
  };

  // "rmgr: XLOG        len (rec/tot):    114/   114, tx:          0, lsn: 0/01000028",
  int n = 0;
  for (auto line : wal_data_exp()) {
    auto lsn = line.substr(line.rfind('/') + 1);
    auto sz  = line.substr(line.find('/', line.find('/') + 1) + 1);
    sz       = sz.substr(0, sz.find(','));

    off_t off = compute_lsn(lsn) - compute_lsn(base);

    size_t size = 0;
    for (auto ch : sz) {
      if (ch >= '0' && ch <= '9')
        size = size * 10U + static_cast<size_t>(ch - '0');
    }
    result.emplace_back(off, size, n++);
  }
  return result;
}

template <size_t n = 8>
size_t round_up(size_t len) {
  return ((len + n - 1) / n) * n;
}

BOOST_AUTO_TEST_CASE(wal_state_update) {
  WalState s;
  auto*    ptr  = reinterpret_cast<unsigned char const*>(wal_data().data());
  size_t   size = wal_data().size();

  BOOST_TEST(t(0, 40) == s.update(ptr, size, 0));
  BOOST_TEST(r(40, 120) == s.update(ptr, size, 0));
  BOOST_TEST(r(160, 32) == s.update(ptr, size, 0));
  BOOST_TEST(r(192, 144) == s.update(ptr, size, 0));
}

BOOST_AUTO_TEST_CASE(compare_with_pg_waldump) {
  auto exp = expected("01000000");

  WalState s;
  auto*    ptr  = reinterpret_cast<unsigned char const*>(wal_data().data());
  size_t   size = wal_data().size();

  size_t bytes     = 0;
  bool   rec_start = false;
  off_t  next      = 0;
  do {
    std::tie(next, bytes, rec_start) = s.update(ptr, size, 0);
    if (rec_start) {
      BOOST_TEST_REQUIRE(next == std::get<0>(exp.front()));
      BOOST_TEST_REQUIRE((bytes + s.rsize()) == round_up(std::get<1>(exp.front())));

      exp.erase(exp.begin());
    }
  } while (bytes && !exp.empty());
}

BOOST_AUTO_TEST_CASE(compare_with_pg_waldump_2) {
  auto exp = expected("01000000");

  WalState s;
  auto*    ptr = reinterpret_cast<unsigned char const*>(wal_data().data());

  size_t bytes     = 0;
  bool   rec_start = false;
  off_t  next      = 0;

  off_t offset = 0;
  while (!exp.empty()) {
    do {
      std::tie(next, bytes, rec_start) = s.update(ptr + offset, 4096, offset);

      if (rec_start) {
        BOOST_TEST_REQUIRE(next == std::get<0>(exp.front()));
        BOOST_TEST_REQUIRE((bytes + s.rsize()) == round_up(std::get<1>(exp.front())));

        exp.erase(exp.begin());
      }
    } while (bytes && !exp.empty());
    offset += 4096;
  }
}
