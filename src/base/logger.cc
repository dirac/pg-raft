#include "src/base/logger.h"

#include <time.h>
#include <unistd.h>

namespace logger {

namespace {

#if defined(__has_feature)
#if __has_feature(address_sanitizer)
__attribute__((no_sanitize("address")))
#endif
#endif
std::mutex& g_mu() {
  // Intential leak.
  static std::mutex* mtx = new std::mutex;
  return *mtx;
}
}  // namespace

Level level(std::optional<Level> l) {
  static std::atomic<Level> lvl;

  if (l) [[unlikely]]
    lvl.store(*l, std::memory_order_relaxed);
  return lvl.load(std::memory_order_relaxed);
}

void set_level(Level lvl) {
  level(lvl);
}

Level get_level() {
  return level(std::nullopt);
}

string& log_buffer() {
  thread_local std::string buffer;
  buffer.reserve(1024);
  buffer.clear();
  return buffer;
}

static std::string_view level_label(Level level) noexcept {
  switch (level) {
    case Level::Debug:
      return "DEBUG";
    case Level::Info:
      return "INFO ";
    case Level::Warn:
      return "WARN ";
    case Level::Error:
      return "ERROR";
    case Level::Fatal:
      return "FATAL";
  }
  std::unreachable();
}

void plog(char const* process, std::string_view line) {
  thread_local std::string buffer;
  buffer.reserve(1024);
  buffer.clear();

  std::format_to(std::back_inserter(buffer), "[{}] {}\n", process, line);

  std::lock_guard lock{g_mu()};
  ::write(STDERR_FILENO, buffer.data(), buffer.size());
}

void _log(Level level, std::string_view msg, std::source_location const& loc) {
  thread_local std::string buffer;
  buffer.reserve(1024);
  buffer.clear();

  if (level < get_level())
    return;

  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  struct tm tm;
  localtime_r(&ts.tv_sec, &tm);

  auto name = loc.file_name();
  if (auto n = strrchr(name, '/'); n)
    name = n + 1;

  std::format_to(std::back_inserter(buffer),
                 "[{}] {:02}:{:02}:{:02}.{:03} [{}:{}] {}\n",
                 level_label(level),
                 tm.tm_hour,
                 tm.tm_min,
                 tm.tm_sec,
                 static_cast<int>(ts.tv_nsec / 1'000'000),
                 name,
                 loc.line(),
                 msg);

  {
    std::lock_guard lock{g_mu()};
    ::write(STDERR_FILENO, buffer.data(), buffer.size());
  }

  if (level == Level::Fatal) [[unlikely]]
    abort();
}

}  // namespace logger
