#pragma once

#include <format>
#include <string_view>

namespace logger {

enum class Level { Debug, Info, Warn, Error };

// Suppress messages below |level| (default: Info).
void set_level(Level level);

// Write a pre-formatted message at |level|.
void log(Level level, std::string_view msg);

template <typename... Args>
void debug(std::format_string<Args...> fmt, Args&&... args) {
  thread_local std::string buffer;
  buffer.clear();
  std::format_to(std::back_inserter(buffer), fmt, std::forward<Args>(args)...);
  log(Level::Debug, buffer);
}

template <typename... Args>
void info(std::format_string<Args...> fmt, Args&&... args) {
  thread_local std::string buffer;
  buffer.clear();
  std::format_to(std::back_inserter(buffer), fmt, std::forward<Args>(args)...);
  log(Level::Info, buffer);
}

template <typename... Args>
void warn(std::format_string<Args...> fmt, Args&&... args) {
  thread_local std::string buffer;
  buffer.clear();
  std::format_to(std::back_inserter(buffer), fmt, std::forward<Args>(args)...);
  log(Level::Warn, buffer);
}

template <typename... Args>
void error(std::format_string<Args...> fmt, Args&&... args) {
  thread_local std::string buffer;
  buffer.clear();
  std::format_to(std::back_inserter(buffer), fmt, std::forward<Args>(args)...);
  log(Level::Error, buffer);
}

}  // namespace logger
