#include "src/base/logger.h"

#include <time.h>
#include <unistd.h>

namespace logger {

static std::atomic<Level> g_level{Level::Debug};
static std::mutex         g_mu;

void set_level(Level level) {
  g_level.store(level, std::memory_order_relaxed);
}

Level get_level() {
  return g_level.load(std::memory_order_relaxed);
}

string& log_buffer () {
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
  }
  std::unreachable();
}

void plog(char const* process, std::string_view line) {
  thread_local std::string buffer;
  buffer.reserve(1024);
  buffer.clear();

  std::format_to(std::back_inserter(buffer), "[{}] {}\n", process, line);

  std::lock_guard lock{g_mu};
  ::write(STDERR_FILENO, buffer.data(), buffer.size());
}

void log(Level level, std::string_view msg, std::source_location const& loc) {
  thread_local std::string buffer;
  buffer.reserve(1024);
  buffer.clear();

  if (level < get_level())
    return;

  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  struct tm tm;
  localtime_r(&ts.tv_sec, &tm);

  std::format_to(std::back_inserter(buffer),
                 "[{}] {:02}:{:02}:{:02}.{:03} [{}:{}] {}\n",
                 level_label(level),
                 tm.tm_hour,
                 tm.tm_min,
                 tm.tm_sec,
                 static_cast<int>(ts.tv_nsec / 1'000'000),
                 loc.file_name(),
                 loc.line(),
                 msg);

  std::lock_guard lock{g_mu};
  ::write(STDERR_FILENO, buffer.data(), buffer.size());
}

}  // namespace logger
