#include "src/base/base.h"

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

unsigned epoch_msec() {
  static auto start   = std::chrono::high_resolution_clock::now();
  auto        elapsed = std::chrono::high_resolution_clock::now() - start;
  return static_cast<unsigned>(
      std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());
}

uint64_t pcounter() {
  static std::atomic_uint64_t v{1};
  return v.fetch_add(1U, std::memory_order_relaxed);
}
uint64_t gcounter(std::optional<uint64_t> g) {
  static std::atomic_uint64_t v{0};
  if (g) [[unlikely]]
    v.store(*g);
  return v.load(std::memory_order_relaxed);
}

std::optional<LogSeqNumber> log_seq_number(char const* path) {
  // On every FUSE operation: no allocation, no locale.
  if (*path == '/')
    ++path;

  // 24 hex digits: timeline, log id, segment in the log id (8 each), and nothing after.
  auto hex8 = [](char const* p) -> std::optional<uint32_t> {
    uint32_t v = 0;
    for (int i = 0; i < 8; ++i) {
      char const c = p[i];
      uint32_t   d = 0;
      if (c >= '0' && c <= '9')
        d = static_cast<uint32_t>(c - '0');
      else if (c >= 'A' && c <= 'F')
        d = static_cast<uint32_t>(c - 'A' + 10);
      else if (c >= 'a' && c <= 'f')
        d = static_cast<uint32_t>(c - 'a' + 10);
      else
        return std::nullopt;  // also a name shorter than 24: its '\0'
      v = (v << 4U) | d;
    }
    return v;
  };
  auto const timeline = hex8(path);
  if (!timeline)
    return std::nullopt;
  auto const logid = hex8(path + 8);
  if (!logid)
    return std::nullopt;
  auto const logseg = hex8(path + 16);
  if (!logseg || path[24] != '\0')
    return std::nullopt;

  // LSN = log id * 2^32 + segment * segment size.
  return LogSeqNumber{
      .timeline = *timeline,
      .lsn      = (static_cast<uint64_t>(*logid) << 32U) +
             (static_cast<uint64_t>(*logseg) * kWalSegmentSize),
  };
}

std::filesystem::path tmp_dir(std::optional<std::filesystem::path> t) {
  static std::mutex            mtx;
  static std::filesystem::path tmp{"/tmp/pg_raft"};

  auto lk = std::unique_lock{mtx};
  if (t)
    tmp = *t;

  std::filesystem::create_directories(tmp);
  return tmp;
}

std::filesystem::path tmp_file(char const* ext) {
  return tmp_dir() / format("{0}-{1}-{2}.{3}", gcounter(), pcounter(), epoch_msec(), ext);
}
