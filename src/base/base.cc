#include "src/base/base.h"

unsigned epoch_msec() {
  static auto start   = std::chrono::high_resolution_clock::now();
  auto        elapsed = std::chrono::high_resolution_clock::now() - start;
  return static_cast<unsigned>(
      std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());
}

namespace {
class SystemErrorCategory : public std::error_category {
 public:
  // Return a short descriptive name for the category
  char const* name() const noexcept final { return "SystemError"; }

  // Return what each enum means in text
  std::string message(int c) const final {
    return std::string("(") + std::to_string(c) + std::string(") ") + strerror(c);
  }
};
SystemErrorCategory const system_error_category;
}  // namespace

std::error_code system_error_code(int err) {
  return {err, system_error_category};
}

bool is_system_error_code(std::error_code const& ec) {
  return &(ec.category()) == &system_error_category ||
         &(ec.category()) == &(std::system_category()) ||
         &(ec.category()) == &(std::generic_category());
}
