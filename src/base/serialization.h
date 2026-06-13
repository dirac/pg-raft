#include "src/base/base.h"

#ifndef SRC_BASE_SERIALIZATION_H_
#define SRC_BASE_SERIALIZATION_H_

namespace boost::serialization {

template <typename Archive, typename... Types>
void serialize(Archive& ar, std::tuple<Types...>& t, unsigned int const) {
  std::apply([&](auto&... element) { ((ar & element), ...); }, t);
}

template <class Archive, class T, size_t N>
void load(Archive& ar, inplace_vector<T, N>& g, unsigned int const /*version */) {
  size_t size = g.size();

  ar & size;
  for (auto i = 0U; i < size; ++i) {
    T t;
    ar & t;
    g.emplace_back(std::move(t));
  }
}

template <class Archive, class T, size_t N>
void save(Archive& ar, inplace_vector<T, N> const& g, unsigned int const /*version */) {
  size_t size = g.size();

  ar & size;
  for (auto i = 0U; i < size; ++i) {
    ar & g.at(i);
  }
}

template <class Archive, class T, size_t N>
void serialize(Archive& ar, inplace_vector<T, N>& t, unsigned int const file_version) {
  split_free(ar, t, file_version);
}

}  // namespace boost::serialization

#endif
