#include "src/base/base.h"

#ifndef BASE_USING_H_
#define BASE_USING_H_

using std::format;
using std::make_shared;
using std::make_tuple;
using std::make_unique;
using std::to_string;

namespace asio = ::boost::asio;

using string      = std::basic_string<char>;
using string_view = std::string_view;

template <size_t RequestedSize>
using MemoryPool = boost::singleton_pool<boost::pool_allocator_tag,
                                         RequestedSize,
                                         boost::default_user_allocator_malloc_free>;

template <class... Args>
using tuple = std::tuple<Args...>;

template <class T, size_t N>
using array = std::array<T, N>;

template <typename T>
using unique_ptr = std::unique_ptr<T>;

template <typename T>
using shared_ptr = std::shared_ptr<T>;

template <typename T>
using weak_ptr = std::weak_ptr<T>;

template <typename T>
using vector = std::vector<T>;

template <typename T>
using list = std::list<T>;

template <typename K, typename V, class Compare = std::less<K>>
using map = std::map<K, V, Compare>;

template <typename K, class Compare = std::less<K>>
using set = std::set<K, Compare>;

template <typename K, typename V, class Hash = std::hash<K>, class KeyEqual = std::equal_to<K>>
using unordered_map = std::unordered_map<K, V, Hash, KeyEqual>;

template <typename K, typename V, class Hash = std::hash<K>, class KeyEqual = std::equal_to<K>>
using unordered_multimap = std::unordered_multimap<K, V, Hash, KeyEqual>;

template <typename K, class Hash = std::hash<K>, class KeyEqual = std::equal_to<K>>
using unordered_set = std::unordered_set<K, Hash, KeyEqual>;

template <typename K,
          typename V,
          typename Hash     = boost::hash<K>,
          typename KeyEqual = std::equal_to<K>>
using unordered_flat_map = ::boost::unordered::unordered_flat_map<K, V, Hash, KeyEqual>;

template <typename K, typename Hash = boost::hash<K>, typename KeyEqual = std::equal_to<K>>
using unordered_flat_set = ::boost::unordered::unordered_flat_set<K, Hash, KeyEqual>;

template <typename K,
          typename V,
          typename Hash     = boost::hash<K>,
          typename KeyEqual = std::equal_to<K>>
using concurrent_unordered_map = ::boost::unordered::concurrent_flat_map<K, V, Hash, KeyEqual>;

template <typename K, typename Hash = boost::hash<K>, typename KeyEqual = std::equal_to<K>>
using concurrent_unordered_set = ::boost::unordered::concurrent_flat_set<K, Hash, KeyEqual>;

template <typename T, size_t N>
using inplace_vector = ::boost::container::static_vector<T, N>;

template <typename T>
using stack = std::stack<T, std::deque<T>>;

template <typename T>
using circular_buffer = ::boost::circular_buffer<T>;

using Bytes = vector<uint8_t>;

template <typename X, typename Y>
using unordered_bimap =
    ::boost::bimap<::boost::bimaps::unordered_set_of<X>, ::boost::bimaps::unordered_set_of<Y>>;

template <class CharT>
using basic_stringstream = std::basic_stringstream<CharT, std::char_traits<CharT>>;
using stringstream       = basic_stringstream<char>;

template <class CharT>
using basic_ostringstream = std::basic_ostringstream<CharT, std::char_traits<CharT>>;
using ostringstream       = basic_ostringstream<char>;

template <class CharT>
using basic_istringstream = std::basic_istringstream<CharT, std::char_traits<CharT>>;
using istringstream       = basic_istringstream<char>;

#endif
