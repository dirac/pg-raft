#define BOOST_TEST_MODULE archive
#include "src/base/archive.h"

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/optional.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <sstream>

#include "src/base/testing.h"

// RawOArchive / RawIArchive write the bytes of binary_[io]archive with no_header, without a
// std::locale.

namespace {

struct Inner {
  uint64_t                                           a{};
  std::optional<uint32_t>                            b;
  std::variant<std::monostate, int64_t, std::string> c;

  bool operator==(Inner const&) const = default;

  template <class Ar>
  void serialize(Ar& ar, unsigned int) {
    ar& a;
    ar& b;
    ar& c;
  }
};

struct Outer {
  std::string                name;
  bool                       flag{};
  std::vector<uint32_t>      numbers;  // bitwise serializable: one block
  std::vector<Inner>         inners;
  std::map<std::string, int> m;
  double                     d{};

  bool operator==(Outer const&) const = default;

  template <class Ar>
  void serialize(Ar& ar, unsigned int) {
    ar& name;
    ar& flag;
    ar& numbers;
    ar& inners;
    ar& m;
    ar& d;
  }
};

Outer sample() {
  return Outer{
      .name    = "segment 000000010000000000000003",
      .flag    = true,
      .numbers = {1, 2, 3, 0xFFFFFFFF},
      .inners  = {Inner{.a = 7, .b = 9, .c = int64_t{-5}},
                  Inner{.a = 8, .b = std::nullopt, .c = std::string("history")},
                  Inner{.a = 0, .b = 0, .c = std::monostate{}}},
      .m       = {{"x", 1}, {"y", -2}},
      .d       = 3.25,
  };
}

std::string boost_bytes(Outer const& o) {
  std::ostringstream              s;
  boost::archive::binary_oarchive oa(s, boost::archive::no_header);
  oa << o;
  return s.str();
}

std::string hex(std::string const& s) {
  std::string r;
  for (char c : s)
    r += std::format("{:02x}", static_cast<unsigned char>(c));
  return r;
}

}  // namespace

BOOST_AUTO_TEST_CASE(same_bytes_as_binary_oarchive) {
  auto const o = sample();

  std::string raw;
  {
    RawOArchive oa(raw);
    oa << o;
  }
  BOOST_TEST(hex(raw) == hex(boost_bytes(o)));

  std::stringbuf sb;
  {
    RawOArchive oa(sb);
    oa << o;
  }
  BOOST_TEST(sb.str() == raw);
}

BOOST_AUTO_TEST_CASE(round_trips) {
  auto const o     = sample();
  auto const bytes = boost_bytes(o);

  // From memory, from a streambuf, and Boost reading ours.
  {
    Outer       r;
    RawIArchive ia(bytes.data(), bytes.size());
    ia >> r;
    BOOST_TEST((r == o));
  }
  {
    Outer          r;
    std::stringbuf sb(bytes);
    RawIArchive    ia(sb);
    ia >> r;
    BOOST_TEST((r == o));
  }
  {
    std::string raw;
    RawOArchive oa(raw);
    oa << o;
    Outer                           r;
    std::istringstream              s(raw);
    boost::archive::binary_iarchive ia(s, boost::archive::no_header);
    ia >> r;
    BOOST_TEST((r == o));
  }
}

BOOST_AUTO_TEST_CASE(truncated_input_throws) {
  auto const bytes = boost_bytes(sample());
  for (std::size_t n : {std::size_t{0}, std::size_t{3}, bytes.size() / 2, bytes.size() - 1}) {
    Outer r;
    BOOST_CHECK_THROW(
        {
          RawIArchive ia(bytes.data(), n);
          ia >> r;
        },
        boost::archive::archive_exception);
  }
}
