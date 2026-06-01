#ifndef BASE_BASE_H_
#define BASE_BASE_H_

#include "src/base/pch.h"

template <typename T>
struct [[nodiscard]] Result : public std::expected<T, std::error_code> {
  Result(std::error_code&& ec) : std::expected<T, std::error_code>(std::unexpected(ec)) {}
  Result(std::error_code& ec) : std::expected<T, std::error_code>(std::unexpected(ec)) {}
  Result(std::error_code const& ec) : std::expected<T, std::error_code>(std::unexpected(ec)) {}

  Result(boost::system::error_code&& ec) : std::expected<T, std::error_code>(std::unexpected(ec)) {}
  Result(boost::system::error_code& ec) : std::expected<T, std::error_code>(std::unexpected(ec)) {}
  Result(boost::system::error_code const& ec)
      : std::expected<T, std::error_code>(std::unexpected(ec)) {}

  template <typename U>
  Result(U&& u) : std::expected<T, std::error_code>(std::move(u)) {}

  template <class... Args>
  Result(Args&&... args)
      : std::expected<T, std::error_code>(std::in_place_t{},
                                          std::forward<decltype(args)>(args)...) {}

  Result(std::expected<T, std::error_code>&& other) noexcept
      : std::expected<T, std::error_code>(std::move(other)) {}

  Result(Result&&)            = default;
  Result& operator=(Result&&) = default;

  Result(Result const&)            = default;
  Result& operator=(Result const&) = default;

  bool has_error() const { return !std::expected<T, std::error_code>::has_value(); }
};

inline Result<void> ok() {
  return Result<void>(std::expected<void, std::error_code>{});
}

std::error_code system_error_code(int err);

bool is_system_error_code(std::error_code const& ec);

unsigned epoch_msec();
uint64_t pcounter();
uint64_t gcounter(std::optional<uint64_t> g);

#include "src/base/convert.h"
#include "src/base/int_type.h"
#include "src/base/using.h"

#endif
