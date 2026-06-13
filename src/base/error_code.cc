#include "src/base/error_code.h"

ErrorCategory const& error_category() {
  static ErrorCategory c;
  return c;
}

char const* to_string(ErrorCode c) {
#ifdef TO_STR
#error "Redefinition of macro"
#endif
#define TO_STR(x)    \
  case ErrorCode::x: \
    return #x

  switch (c) {
    TO_STR(kProgramBug);
    default:
      return "unknown error";
  }
#undef TO_STR
}
