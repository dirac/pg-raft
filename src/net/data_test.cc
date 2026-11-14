#define BOOST_TEST_MODULE data
#include "src/net/data.h"

#include "src/base/testing.h"

namespace {
struct T {
  std::vector<unsigned> elements;

  bool operator==(T const& that) const { return elements == that.elements; }

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & elements;
  }
};
}  // namespace

BOOST_AUTO_TEST_CASE(text_serialization, * boost::unit_test::timeout(60)) {
  T v;
  v.elements.reserve(kPageSize * 3);

  // Text is ~50x slower than binary (debug build): a prime step still lands the end at varied
  // offsets across the Data pages, in a few seconds rather than close to the timeout.
  for (unsigned idx = 0; idx < kPageSize * 3; idx += 17) {
    while (v.elements.size() < idx)
      v.elements.emplace_back(idx);

    auto copy = from_data<T>(to_data(v, false)).value();
    BOOST_TEST((copy == v));
  }
}

BOOST_AUTO_TEST_CASE(binary_serialization, * boost::unit_test::timeout(60)) {
  T v;
  v.elements.reserve(kPageSize * 3);

  for (unsigned idx = 0; idx < kPageSize * 3; idx += 3) {
    while (v.elements.size() < idx)
      v.elements.emplace_back(idx);

    auto copy = from_data<T>(to_data(v, true)).value();
    BOOST_TEST((copy == v));
  }
}
