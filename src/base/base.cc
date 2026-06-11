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
  if (*path == '/')
    ++path;

  for (auto i = 0; i < 24; ++i) {
    char c = path[i];
    if (c >= '0' && c <= '9')
      continue;
    if (c >= 'A' && c <= 'F')
      continue;
    if (c >= 'a' && c <= 'f')
      continue;

    return std::nullopt;
  }
  if (path[25] != '\0')
    return std::nullopt;

  // Zero-allocation slicing using std::string_view
  auto timeline_sv = std::string_view{path, 8};
  auto logid_sv    = std::string_view{path + 8, 8};
  auto logseg_sv   = std::string_view{path + 16, 8};

  auto hex_to_int = [](std::string_view hex_sv) -> uint32_t {
    uint32_t          value = 0;
    std::stringstream ss;
    ss << std::hex << hex_sv;
    ss >> value;
    return value;
  };

  // Convert hex segments to integers
  uint32_t timeline = hex_to_int(timeline_sv);
  uint32_t logid    = hex_to_int(logid_sv);
  uint32_t logseg   = hex_to_int(logseg_sv);

  // Reconstruct the 64-bit LSN value
  // Internal formula: LSN = (logid * 0x100000000) + (logseg * WAL_SEGMENT_SIZE)
  uint64_t lsn =
      (static_cast<uint64_t>(logid) << 32) + (static_cast<uint64_t>(logseg) * kWalSegmentSize);

  return LogSeqNumber{.timeline = timeline, .lsn = lsn};
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
