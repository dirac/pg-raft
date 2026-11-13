#ifndef BASE_BASE_H_
#define BASE_BASE_H_

#include "src/base/pch.h"

template <typename T>
struct [[nodiscard]] Result : public std::expected<T, std::error_code> {
  Result(std::error_code&& ec) : std::expected<T, std::error_code>(std::unexpected(ec)) {}
  Result(std::error_code& ec) : std::expected<T, std::error_code>(std::unexpected(ec)) {}
  Result(std::error_code const& ec) : std::expected<T, std::error_code>(std::unexpected(ec)) {}

  Result(boost::system::error_code&& ec) : std::expected<T, std::error_code>(std::unexpected(ec)) {}
  Result(boost::system::error_code& ec) : std::expected<T, std::error_code>(std::unexpected(ec)) {}
  Result(boost::system::error_code const& ec)
      : std::expected<T, std::error_code>(std::unexpected(ec)) {}

  // std::forward, not std::move: an lvalue is copied (returning a member must not empty it).
  template <typename U>
  Result(U&& u) : std::expected<T, std::error_code>(std::forward<U>(u)) {}

  template <class... Args>
  Result(Args&&... args)
      : std::expected<T, std::error_code>(std::in_place_t{},
                                          std::forward<decltype(args)>(args)...) {}

  Result(std::expected<T, std::error_code>&& other) noexcept
      : std::expected<T, std::error_code>(std::move(other)) {}

  Result(Result&&)            = default;
  Result& operator=(Result&&) = default;

  Result(Result const&)            = default;
  Result& operator=(Result const&) = default;

  bool has_error() const { return !std::expected<T, std::error_code>::has_value(); }
};

inline Result<void> ok() {
  return Result<void>(std::expected<void, std::error_code>{});
}

constexpr unsigned kWalBlockSize   = 8192;              // wal_block_size (integer).
constexpr unsigned kWalSegmentSize = 16 * 1024 * 1024;  /// wal_segment_size (integer) = 16MB

struct LogSeqNumber {
  uint32_t timeline;
  uint64_t lsn;

  bool operator<(LogSeqNumber const& that) const {
    return timeline == that.timeline ? lsn < that.lsn : timeline < that.timeline;
  }
  bool operator>(LogSeqNumber const& that) const {
    return timeline == that.timeline ? lsn > that.lsn : timeline > that.timeline;
  }
  bool operator==(LogSeqNumber const& that) const {
    return timeline == that.timeline && lsn == that.lsn;
  }

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & timeline;
    ar & lsn;
  }
};

std::optional<LogSeqNumber> log_seq_number(char const* path);

inline std::string filename(LogSeqNumber const& lsn) {
  return std::format(
      "{:08X}{:08X}{:08X}", lsn.timeline, lsn.lsn >> 32, (lsn.lsn & 0xFFFFFFFFU) / kWalSegmentSize);
}

std::error_code system_error_code(int err);

bool is_system_error_code(std::error_code const& ec);

unsigned epoch_msec();
uint64_t pcounter();
uint64_t gcounter(std::optional<uint64_t> g = std::nullopt);

std::filesystem::path tmp_dir(std::optional<std::filesystem::path> t = std::nullopt);
std::filesystem::path tmp_file(char const* ext = "tmp");

inline mode_t file_mode() {
  // S_IRUSR  00400 user has read permission
  // S_IWUSR  00200 user has write permission
  // S_IRGRP  00040 group has read permission
  // S_IWGRP  00020 group has write permission
  // S_IROTH  00004 others have read permission

  return S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;
}

// clang-format off

#include "src/base/using.h"
#include "src/base/convert.h"
#include "src/base/formatter.h"
#include "src/base/int_type.h"
#include "src/base/serialization.h"

// clang-format on

#endif
