#pragma once

#include "src/base/base.h"

enum class ErrorCode : uint8_t {
  kProgramBug = 1,
  kNotLeader  = 2,  ///< The raft server is not the leader (or is stepping down, or closed).
};

char const* to_string(ErrorCode ec);

class ErrorCategory : public std::error_category {
 public:
  // Return a short descriptive name for the category
  char const* name() const noexcept final { return "PgRaft"; }

  // Return what each enum means in text
  std::string message(int c) const final { return to_string(static_cast<ErrorCode>(c)); }
};
ErrorCategory const& error_category();

inline std::error_code make_error_code(ErrorCode e) {
  return {static_cast<int>(e), error_category()};
}

namespace std {
template <>
struct is_error_code_enum<ErrorCode> : true_type {};
}  // namespace std
