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

BOOST_AUTO_TEST_CASE(text_serialization) {
  T v;
  v.elements.reserve(kPageSize * 3);

  for (unsigned idx = 0; idx < kPageSize * 3; idx += 3) {
    while (v.elements.size() < idx)
      v.elements.emplace_back(idx);

    auto copy = from_data<T>(to_data(v, false)).value();
    BOOST_TEST((copy == v));
  }
}

BOOST_AUTO_TEST_CASE(binary_serialization) {
  T v;
  v.elements.reserve(kPageSize * 3);

  for (unsigned idx = 0; idx < kPageSize * 3; idx += 3) {
    while (v.elements.size() < idx)
      v.elements.emplace_back(idx);

    auto copy = from_data<T>(to_data(v, true)).value();
    BOOST_TEST((copy == v));
  }
}
