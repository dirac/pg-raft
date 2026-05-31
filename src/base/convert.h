#include "src/base/base.h"

#ifndef BASE_CONVERT_HPP_
#define BASE_CONVERT_HPP_

namespace {
template <typename T>
struct IsUnsigned {
  static constexpr bool kValue = std::is_unsigned_v<T>;
};

template <>
struct IsUnsigned<uint8_t> {
  static constexpr bool kValue = true;
};
template <>
struct IsUnsigned<uint16_t> {
  static constexpr bool kValue = true;
};
template <>
struct IsUnsigned<uint32_t> {
  static constexpr bool kValue = true;
};
template <>
struct IsUnsigned<uint64_t> {
  static constexpr bool kValue = true;
};
template <>
struct IsUnsigned<int8_t> {
  static constexpr bool kValue = false;
};
template <>
struct IsUnsigned<int16_t> {
  static constexpr bool kValue = false;
};
template <>
struct IsUnsigned<int32_t> {
  static constexpr bool kValue = false;
};
template <>
struct IsUnsigned<int64_t> {
  static constexpr bool kValue = false;
};
template <typename T>
struct IsSigned {
  static constexpr bool kValue = !IsUnsigned<T>::kValue;
};

/// @brief
/// Typecast an integer of one type to another. If the input integer can't be represented as the
/// resulting then the code asserts (i.e., an exception).
///
/// @tparam T the resulting integral type.
/// @tparam F the source integral type
///
/// @param v the input integer.
/// @return integer of resulting integral type.
///
template <typename T, typename F>
T convert(F v) {
  if constexpr (std::is_same_v<T, F>) {
    return v;
  }
  if constexpr (IsUnsigned<T>::kValue == IsUnsigned<F>::kValue) {
    if constexpr (sizeof(F) <= sizeof(T)) {
      return v;
    }
    assert(v <= std::numeric_limits<T>::max());
    assert(v >= std::numeric_limits<T>::min());

    return static_cast<T>(v);
  }
  if constexpr (IsUnsigned<T>::kValue != IsUnsigned<F>::kValue) {
    if constexpr (IsUnsigned<T>::kValue) {
      assert(v >= static_cast<F>(0));
    }
    if constexpr (sizeof(F) < sizeof(T)) {
      return static_cast<T>(v);
    }
    if constexpr (sizeof(F) == sizeof(T)) {
      if constexpr (IsUnsigned<T>::kValue) {
        return static_cast<T>(v);
      }
      if constexpr (IsUnsigned<F>::kValue) {
        assert(v <= (std::numeric_limits<F>::max() << 1));
        return static_cast<T>(v);
      }
    }
    // sizeof(F) > sizeof(T)
    assert(v <= static_cast<F>(std::numeric_limits<T>::max()));
    return static_cast<T>(v);
  }
}
}  // namespace

/// @brief
/// Convert an integer of input type to `uint64_t`.
///
/// @tparam T the source integral type.
/// @return integer of type `uint64_t`.
template <typename T>
uint64_t u8(T a) {
  return convert<uint64_t, T>(a);
}

template <typename T>
uint64_t p8(T const* p) {
  return reinterpret_cast<uint64_t>(p);
}

/// @brief
/// Convert an integer of input type to `int64_t`.
///
/// @tparam T the source integral type.
/// @return integer of type `int64_t`.
template <typename T>
int64_t i8(T a) {
  return convert<int64_t, T>(a);
}

/// @brief
/// Convert an integer of input type to `uint32_t`.
///
/// @tparam T the source integral type.
/// @return integer of type `uint32_t`.
template <typename T>
uint32_t u4(T a) {
  return convert<uint32_t, T>(a);
}

/// @brief
/// Convert an integer of input type to `int32_t`.
///
/// @tparam T the source integral type.
/// @return integer of type `int32_t`.
template <typename T>
int32_t i4(T a) {
  return convert<int32_t, T>(a);
}

/// @brief
/// Convert an integer of input type to `uint16_t`.
///
/// @tparam T the source integral type.
/// @return integer of type `uint16_t`.
template <typename T>
uint16_t u2(T a) {
  return convert<uint16_t, T>(a);
}

/// @brief
/// Convert an integer of input type to `int16_t`.
///
/// @tparam T the source integral type.
/// @return integer of type `int16_t`.
template <typename T>
int16_t i2(T a) {
  return convert<int16_t, T>(a);
}

/// @brief
/// Convert an integer of input type to `uint8_t`.
///
/// @tparam T the source integral type.
/// @return integer of type `uint8_t`.
template <typename T>
uint8_t u1(T a) {
  return convert<uint8_t, T>(a);
}

/// @brief
/// Convert an integer of input type to `int8_t`.
///
/// @tparam T the source integral type.
/// @return integer of type `int8_t`.
template <typename T>
int8_t i1(T a) {
  return convert<int8_t, T>(a);
}

/// @brief
/// Convert an integer from T to U.
///
/// @tparam T the source integral type.
/// @tparam U the resulting integral type.
/// @return integer of type U.
template <typename U, typename T>
U i(T a) {
  return convert<U, T>(a);
}

#endif
