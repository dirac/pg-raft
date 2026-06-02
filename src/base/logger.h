#pragma once

#include "src/base/base.h"

namespace logger {

enum class Level { Debug, Info, Warn, Error, Fatal };

// Suppress messages below |level| (default: Info).
void set_level(Level level);

Level get_level();

string& log_buffer();

// Write a pre-formatted message at |level|.
void _log(Level level, std::string_view msg, std::source_location const& loc);

struct LogLevel {
  LogLevel(Level lvl, std::source_location loc = std::source_location::current())
      : level(lvl), location(loc) {}

  Level const                level;
  std::source_location const location;
};

template <typename... Args>
void log(LogLevel info, std::format_string<Args...> fmt, Args&&... args) {
  auto& buffer = log_buffer();

  if (info.level < get_level()) [[likely]]
    return;

  std::format_to(std::back_inserter(buffer), fmt, std::forward<Args>(args)...);
  _log(info.level, buffer, info.location);
}

struct ErrorCode : public std::error_code {
  using std::error_code::message;
  using std::error_code::value;

  ErrorCode(std::error_code ec, std::source_location loc = std::source_location::current())
      : std::error_code(ec), location(loc) {}

  ErrorCode(boost::system::error_code const& ec,
            std::source_location             loc = std::source_location::current())
      : std::error_code(ec), location(loc) {}

  std::source_location const location;
};

template <typename... Args>
std::error_code elog(ErrorCode ec, std::format_string<Args...> fmt, Args&&... args) {
  auto& buffer = log_buffer();

  std::format_to(std::back_inserter(buffer), fmt, std::forward<Args>(args)...);
  std::format_to(std::back_inserter(buffer), ", error = {} ({})", ec.message(), ec.value());

  _log(Level::Error, buffer, ec.location);
  return ec;
}

void plog(char const* process, std::string_view log);

}  // namespace logger

constexpr auto INFO  = logger::Level::Info;
constexpr auto WARN  = logger::Level::Warn;
constexpr auto DEBUG = logger::Level::Debug;
constexpr auto ERROR = logger::Level::Error;
constexpr auto FATAL = logger::Level::Fatal;

using logger::elog;
using logger::log;
