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
    log(Level::Debug, std::format(fmt, std::forward<Args>(args)...));
}

template <typename... Args>
void info(std::format_string<Args...> fmt, Args&&... args) {
    log(Level::Info, std::format(fmt, std::forward<Args>(args)...));
}

template <typename... Args>
void warn(std::format_string<Args...> fmt, Args&&... args) {
    log(Level::Warn, std::format(fmt, std::forward<Args>(args)...));
}

template <typename... Args>
void error(std::format_string<Args...> fmt, Args&&... args) {
    log(Level::Error, std::format(fmt, std::forward<Args>(args)...));
}

} // namespace logger
