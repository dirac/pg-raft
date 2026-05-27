#include "src/base/logger.h"

#include <time.h>
#include <unistd.h>

#include <atomic>
#include <mutex>
#include <string>

namespace logger {

static std::atomic<Level> g_level{Level::Debug};
static std::mutex          g_mu;

void set_level(Level level) {
    g_level.store(level, std::memory_order_relaxed);
}

static std::string_view level_label(Level level) noexcept {
    switch (level) {
        case Level::Debug: return "DEBUG";
        case Level::Info:  return "INFO ";
        case Level::Warn:  return "WARN ";
        case Level::Error: return "ERROR";
    }
    std::unreachable();
}

void log(Level level, std::string_view msg) {
    if (level < g_level.load(std::memory_order_relaxed)) return;

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tm;
    localtime_r(&ts.tv_sec, &tm);

    std::string line = std::format(
        "[{}] {:02}:{:02}:{:02}.{:03} {}\n",
        level_label(level),
        tm.tm_hour, tm.tm_min, tm.tm_sec,
        static_cast<int>(ts.tv_nsec / 1'000'000),
        msg);

    std::lock_guard lock{g_mu};
    ::write(STDERR_FILENO, line.data(), line.size());
}

} // namespace logger
