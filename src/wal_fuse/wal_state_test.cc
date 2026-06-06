#define BOOST_TEST_MODULE wal_state_test

#include "src/base/testing.h"
#include "src/wal_fuse/wal_segment.h"

std::string_view wal_data();

template <typename A, typename B>
std::tuple<off_t, size_t> t(A a, B b) {
  return std::tuple<off_t, size_t>{static_cast<off_t>(a), static_cast<size_t>(b)};
}

namespace std {
ostream& operator<<(ostream& out, tuple<off_t, size_t> const& t) {
  return out << "{" << get<0>(t) << ", " << get<1>(t) << "}";
}
}  // namespace std

using WalState = WalSegment::WalState;

BOOST_AUTO_TEST_CASE(wal_state_update) {
  WalState    s;
  char const* ptr  = wal_data().data();
  size_t      size = wal_data().size();

  BOOST_TEST(t(0, 160) == s.update(ptr, size, 0));
  BOOST_TEST(t(160, 32) == s.update(ptr, size, 0));
  BOOST_TEST(t(192, 144) == s.update(ptr, size, 0));
}
