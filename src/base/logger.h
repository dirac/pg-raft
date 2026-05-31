#pragma once

#include "src/base/base.h"

namespace logger {

enum class Level { Debug, Info, Warn, Error };

// Suppress messages below |level| (default: Info).
void set_level(Level level);

Level get_level();

string& log_buffer();

// Write a pre-formatted message at |level|.
void _log(Level level, std::string_view msg, std::source_location const& loc);

template <typename... Args>
void _log(Level level, std::source_location loc, std::format_string<Args...> fmt, Args&&... args) {
  auto& buffer = log_buffer();

  if (level < get_level()) [[likely]]
    return;

  std::format_to(std::back_inserter(buffer), fmt, std::forward<Args>(args)...);
  _log(level, buffer, loc);
}

void plog(char const* process, std::string_view log);

}  // namespace logger

#define info(fmt, ...) \
  logger::_log(logger::Level::Info, std::source_location::current(), fmt, ##__VA_ARGS__)

#define debug(fmt, ...) \
  logger::_log(logger::Level::Debug, std::source_location::current(), fmt, ##__VA_ARGS__)

#define warn(fmt, ...) \
  logger::_log(logger::Level::Warn, std::source_location::current(), fmt, ##__VA_ARGS__)

#define error(fmt, ...) \
  logger::_log(logger::Level::Error, std::source_location::current(), fmt, ##__VA_ARGS__)

#define fatal(fmt, ...)                                                                    \
  logger::_log(logger::Level::Error, std::source_location::current(), fmt, ##__VA_ARGS__); \
  abort()
