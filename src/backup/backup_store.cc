#include "src/backup/backup_store.h"

#include <charconv>

namespace {

/// Parse |s| as a number of exactly |width| digits (|width| 0: any length up to 5) in |base|.
/// Upper-case hex only, so that a name has one spelling.
template <typename T>
std::optional<T> parse_field(std::string_view s, size_t width, int base) {
  if (width ? s.size() != width : (s.empty() || s.size() > 5))
    return std::nullopt;
  for (char c : s) {
    bool digit = c >= '0' && c <= '9';
    bool hex   = base == 16 && c >= 'A' && c <= 'F';
    if (!digit && !hex)
      return std::nullopt;
  }
  T    value{};
  auto r = std::from_chars(s.data(), s.data() + s.size(), value, base);
  if (r.ec != std::errc{} || r.ptr != s.data() + s.size())
    return std::nullopt;
  return value;
}

}  // namespace

string BackupInfo::name() const {
  return std::format(
      "{:08X}-{:016X}-{:010}-{}", lsn.timeline, lsn.lsn, static_cast<int64_t>(time), node);
}

std::optional<BackupInfo> parse_backup_name(std::string_view name) {
  std::array<std::string_view, 4> parts;
  for (size_t i = 0; i < parts.size(); ++i) {
    auto pos = i + 1 < parts.size() ? name.find('-') : std::string_view::npos;
    if (i + 1 < parts.size() && pos == std::string_view::npos)
      return std::nullopt;
    parts.at(i) = name.substr(0, pos);
    name        = pos == std::string_view::npos ? std::string_view{} : name.substr(pos + 1);
  }

  auto timeline = parse_field<uint32_t>(parts[0], 8, 16);
  auto lsn      = parse_field<uint64_t>(parts[1], 16, 16);
  auto time     = parse_field<int64_t>(parts[2], 10, 10);
  auto node     = parse_field<uint16_t>(parts[3], 0, 10);
  if (!timeline || !lsn || !time || !node)
    return std::nullopt;

  return BackupInfo{
      .lsn  = LogSeqNumber{.timeline = *timeline, .lsn = *lsn},
      .time = static_cast<std::time_t>(*time),
      .node = RaftNodeId(*node),
  };
}

Result<std::optional<BackupInfo>> BackupStore::latest() const {
  auto backups = list();
  if (!backups)
    return backups.error();
  if (backups->empty())
    return std::optional<BackupInfo>{};
  return std::optional<BackupInfo>{backups->back()};
}

bool backup_due(BackupPolicy const&              policy,
                unsigned                         index,
                unsigned                         nodes,
                std::optional<BackupInfo> const& latest,
                std::time_t                      now,
                LogSeqNumber                     wal_end) {
  assert(nodes > 0 && index < nodes);
  if (!latest)
    return false;

  // interval * (1 + index / nodes), and the same for the WAL size.
  auto const interval = policy.interval.count() * (nodes + index) / nodes;
  if (now - latest->time >= interval)
    return true;

  auto const wal_size = policy.wal_size * (nodes + index) / nodes;
  auto const written  = wal_end.lsn > latest->lsn.lsn ? wal_end.lsn - latest->lsn.lsn : 0U;
  return written >= wal_size;
}

bool pending_blocks(BackupPolicy const&          policy,
                    vector<PendingBackup> const& pending,
                    RaftNodeId                   self,
                    std::time_t                  now) {
  return std::ranges::any_of(pending, [&](PendingBackup const& p) {
    return p.node != self && now - p.refreshed < policy.pending_timeout.count();
  });
}

LogSeqNumber keep_from(BackupInfo const& backup) {
  return LogSeqNumber{
      .timeline = backup.lsn.timeline,
      .lsn      = backup.lsn.lsn - backup.lsn.lsn % kWalSegmentSize,
  };
}
