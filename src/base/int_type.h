#include "src/base/base.h"

#ifndef BASE_INT_TYPE_H_
#define BASE_INT_TYPE_H_

/// @brief
/// Int is a wrapper template around different types integral types.
///
template <typename T, int L>
class Int {
  using IntType = Int<T, L>;

 public:
  using value_type = T;

  Int()                                  = default;
  Int(Int<T, L>&& that)                  = default;
  Int(Int<T, L> const&)                  = default;
  Int<T, L>& operator=(Int<T, L>&&)      = default;
  Int<T, L>& operator=(Int<T, L> const&) = default;

  /// Construct from the integer.
  explicit Int(T t) : id_(t) {}

  explicit Int(std::atomic<T>& t) : id_(t.fetch_add(static_cast<T>(1))) {}

  /// Support typecast
  explicit operator T() const { return id_; }

  /// Assignment to the primitive type.
  Int& operator=(T t) {
    id_ = t;
    return *this;
  }
  /// Pre-Increment operator.
  Int& operator++() {
    ++id_;
    return *this;
  }
  /// Post increment operator.
  Int operator++(int) { return IntType(id_++); }

  /// Pre-Decrement operator.
  Int& operator--() {
    --id_;
    return *this;
  }
  /// Post decrement operator.
  Int operator--(int) { return IntType(id_--); }

  /// Less than operator.
  bool operator<(Int const& that) const { return id_ < that.id_; }

  /// Less than equal to operator.
  bool operator<=(Int const& that) const { return id_ <= that.id_; }

  /// Equality operator.
  bool operator==(Int const& that) const { return id_ == that.id_; }

  /// Greater than or equals operator.
  bool operator>=(Int const& that) const { return id_ >= that.id_; }

  /// Greater than operator.
  bool operator>(Int const& that) const { return id_ > that.id_; }

  /// Not equals operator.
  bool operator!=(Int const& that) const { return id_ != that.id_; }

  IntType operator/(unsigned int v) const { return IntType(id_ / v); }
  IntType operator%(unsigned int v) const { return IntType(id_ % v); }

  IntType operator+(T x) const { return IntType(id_ + x); }
  IntType operator-(T x) const { return IntType(id_ - x); }

  IntType& operator+=(T x) {
    id_ += x;
    return *this;
  }
  IntType& operator-=(T x) {
    id_ -= x;
    return *this;
  }

  T operator-(Int const& that) const { return id_ - that.id_; }

  T operator()() const { return id_; }

  constexpr static IntType zero() { return IntType(0); }
  constexpr static IntType max() {
    if constexpr (std::is_unsigned_v<T>)
      return IntType(std::numeric_limits<T>::max() - 1U);
    else
      return IntType(std::numeric_limits<T>::max());
  }
  constexpr static IntType min() {
    if constexpr (std::is_unsigned_v<T>)
      return IntType(0);
    else
      return IntType(std::numeric_limits<T>::lowest());
  }
  constexpr static IntType nil() {
    if constexpr (std::is_unsigned_v<T>)
      return IntType(std::numeric_limits<T>::max());
    else
      return IntType(std::numeric_limits<T>::lowest());
  }

  T value() const { return id_; }

 protected:
  T id_ = 0;

  friend class std::atomic<IntType>;

  friend class boost::serialization::access;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & id_;
  }
};

/// Support output operator for Int template.
template <typename T, int N>
std::ostream& operator<<(std::ostream& os, Int<T, N> const& type) {
  return os << static_cast<T>(type);
}

template <typename T, int N>
Int<T, N> operator+(Int<T, N> const& a, T const b) {
  return Int<T, N>(static_cast<T>(a) + b);
}

template <typename T, int N>
Int<T, N> operator-(Int<T, N> const& a, T const b) {
  return Int<T, N>(static_cast<T>(a) - b);
}

template <typename T, int N>
T operator-(Int<T, N> const& a, Int<T, N> const& b) {
  assert(a >= b);
  return (static_cast<T>(a) - static_cast<T>(b));
}
template <typename T, int N>
auto hash(Int<T, N> const& a) {
  return std::hash<T>()(static_cast<T>(a));
}
template <typename T>
T max(T a, T b) {
  return a > b ? a : b;
}
template <typename T>
T min(T a, T b) {
  return a > b ? b : a;
}

#define DEFINE_INT_TYPE(L, nm, t)                       \
  using nm = Int<t, L>;                                 \
  static_assert(std::is_trivially_copyable<nm>::value); \
  static_assert(std::is_copy_constructible<nm>::value); \
  static_assert(std::is_move_constructible<nm>::value); \
  static_assert(std::is_copy_assignable<nm>::value);    \
  static_assert(std::is_move_assignable<nm>::value);    \
  inline char const* type_name(const nm&) {             \
    return #nm;                                         \
  }

#define INT(x) ((x).value())

DEFINE_INT_TYPE(1, RaftTerm, uint64_t);
DEFINE_INT_TYPE(2, RaftIndex, uint64_t);
DEFINE_INT_TYPE(3, RaftNodeId, uint16_t);

// NOLINTBEGIN
namespace std {
template <typename T, int L>
class atomic<Int<T, L>> {
  using IntType = Int<T, L>;
  std::atomic<typename IntType::value_type> v_;

 public:
  atomic(atomic<Int<T, L>> const& that) : v_(that.v_.load(std::memory_order_relaxed)) {}

  atomic<Int<T, L>>& operator=(atomic<Int<T, L>> const& that) {
    v_.store(that.v_.load(std::memory_order_relaxed));
    return *this;
  }

  atomic(IntType v) : v_(v.id_) {}

  atomic(Int<T, L>::value_type v) : v_(v) {}

  IntType load(std::memory_order order = std::memory_order_seq_cst) const noexcept {
    return IntType{v_.load(order)};
  }

  void store(IntType desired, std::memory_order order = std::memory_order_seq_cst) noexcept {
    v_.store(desired.id_, order);
  }

  bool compare_exchange_strong(IntType&          expected,
                               IntType           desired,
                               std::memory_order order = std::memory_order_seq_cst) noexcept {
    return v_.compare_exchange_strong(expected.id_, desired.id_, order);
  }
  bool compare_exchange_weak(IntType&          expected,
                             IntType           desired,
                             std::memory_order order = std::memory_order_seq_cst) noexcept {
    return v_.compare_exchange_weak(expected.id_, desired.id_, order);
  }

  IntType fetch_add(typename IntType::value_type v,
                    std::memory_order            order = std::memory_order_seq_cst) noexcept {
    return IntType{v_.fetch_add(v, order)};
  }

  IntType fetch_sub(typename IntType::value_type v,
                    std::memory_order            order = std::memory_order_seq_cst) noexcept {
    return IntType{v_.fetch_sub(v, order)};
  }
};
}  // namespace std

template <typename T, int L>
struct std::formatter<Int<T, L>> {
  using IntType = Int<T, L>;
  bool verbose  = false;

  constexpr auto parse(std::format_parse_context& ctx) {
    auto it = ctx.begin();
    if (it != ctx.end() && *it == 'v') {
      verbose = true;
      ++it;
    }
    return it;  // Must return iterator past the parsed specifier
  }

  auto format(IntType const& v, std::format_context& ctx) const {
    return verbose ? std::format_to(ctx.out(), "{}({})", type_name(v), INT(v))
                   : std::format_to(ctx.out(), "{}", INT(v));
  }
  // Usage: std::format("{:v}", p); -> "RaftNodeId(10)"
  // Usage: std::format("{}", p); -> "10"
};

template <typename T, int N>
struct std::hash<Int<T, N>> {
  std::size_t operator()(Int<T, N> const& v) const { return std::hash<T>()(INT(v)); }
};

template <typename T, int N>
struct boost::hash<Int<T, N>> {
  std::size_t operator()(Int<T, N> const& v) const { return boost::hash<T>()(v.value()); }
};

template <typename T1, int N1, typename T2, int N2>
struct std::hash<std::tuple<Int<T1, N1>, Int<T2, N2>>> {
  std::size_t operator()(std::tuple<Int<T1, N1>, Int<T2, N2>> const& v) const {
    std::size_t seed = 0;

    boost::hash_combine(seed, std::get<0>(v));
    boost::hash_combine(seed, std::get<1>(v));
    return seed;
  }
};
// NOLINTEND;

#endif
