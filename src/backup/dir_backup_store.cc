#include "src/backup/dir_backup_store.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include <charconv>
#include <fstream>

#include "src/base/logger.h"

namespace fs = std::filesystem;

namespace {

constexpr std::string_view kTmpPrefix   = ".tmp-";
constexpr std::string_view kLeasePrefix = ".pending-";

Result<void> fsync_path(fs::path const& path) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd == -1)
    return elog(system_error_code(errno), "Unable to open {} to fsync", path);
  int  rc  = ::fsync(fd);
  auto err = errno;
  ::close(fd);
  if (rc != 0)
    return elog(system_error_code(err), "Unable to fsync {}", path);
  return ok();
}

/// fsync every file and directory under |dir|, and |dir| itself.
Result<void> fsync_tree(fs::path const& dir) {
  std::error_code ec;
  for (auto it = fs::recursive_directory_iterator(dir, ec); !ec && it != fs::end(it);
       it.increment(ec)) {
    if (it->is_symlink(ec))
      continue;
    if (auto r = fsync_path(it->path()); !r)
      return r;
  }
  if (ec)
    return elog(ec, "Unable to walk {}", dir);
  return fsync_path(dir);
}

/// Copy the tree |src| to |dst| (which must not exist), keeping symbolic links as links.
Result<void> copy_tree(fs::path const& src, fs::path const& dst) {
  std::error_code ec;
  if (!fs::is_directory(src, ec))
    return elog(system_error_code(ENOTDIR), "{} is not a directory", src);
  fs::copy(src, dst, fs::copy_options::recursive | fs::copy_options::copy_symlinks, ec);
  if (ec)
    return elog(ec, "Unable to copy {} to {}", src, dst);
  return ok();
}

Result<void> set_mtime(fs::path const& path, std::time_t time) {
  struct timeval times[2] = {{.tv_sec = time, .tv_usec = 0}, {.tv_sec = time, .tv_usec = 0}};
  if (::utimes(path.c_str(), times) != 0)
    return elog(system_error_code(errno), "Unable to set the time of {}", path);
  return ok();
}

}  // namespace

DirBackupStore::DirBackupStore(fs::path root) : root_(std::move(root)) {
  std::error_code ec;
  fs::create_directories(root_, ec);
  if (ec)
    elog(ec, "Unable to create the backup store {}", root_);
}

fs::path DirBackupStore::lease(RaftNodeId node) const {
  return root_ / std::format("{}{}", kLeasePrefix, node);
}

Result<void> DirBackupStore::remove_tmp(RaftNodeId node) {
  std::error_code ec;
  for (auto const& entry : fs::directory_iterator(root_, ec)) {
    auto name = entry.path().filename().string();
    if (!name.starts_with(kTmpPrefix))
      continue;
    auto info = parse_backup_name(std::string_view{name}.substr(kTmpPrefix.size()));
    if (!info || info->node != node)
      continue;
    log(INFO, "Removing the partial backup {}", entry.path());
    fs::remove_all(entry.path(), ec);
    if (ec)
      return elog(ec, "Unable to remove {}", entry.path());
  }
  if (ec)
    return elog(ec, "Unable to list {}", root_);
  return ok();
}

Result<PendingBackup> DirBackupStore::begin(RaftNodeId node, std::time_t time) {
  if (auto r = remove_tmp(node); !r)
    return r.error();

  // Write the lease under a temporary name and rename it, so that pending() never reads a partial
  // one.
  auto path = lease(node);
  auto tmp  = fs::path{path.string() + ".new"};
  {
    std::ofstream out(tmp, std::ios::trunc);
    out << static_cast<int64_t>(time) << '\n';
    if (!out.flush())
      return elog(system_error_code(EIO), "Unable to write {}", tmp);
  }
  if (auto r = set_mtime(tmp, time); !r)
    return r.error();

  std::error_code ec;
  fs::rename(tmp, path, ec);
  if (ec)
    return elog(ec, "Unable to rename {} to {}", tmp, path);

  return PendingBackup{.node = node, .started = time, .refreshed = time};
}

Result<void> DirBackupStore::refresh(PendingBackup& pending, std::time_t time) {
  if (auto r = set_mtime(lease(pending.node), time); !r)
    return r;
  pending.refreshed = time;
  return ok();
}

Result<BackupInfo> DirBackupStore::put(PendingBackup const& pending,
                                       fs::path const&      dir,
                                       LogSeqNumber         lsn) {
  auto info = BackupInfo{.lsn = lsn, .time = pending.started, .node = pending.node};
  auto dest = root_ / info.name();
  auto tmp  = root_ / std::format("{}{}", kTmpPrefix, info.name());

  std::error_code ec;
  if (fs::exists(dest, ec))
    return elog(system_error_code(EEXIST), "The backup {} exists", dest);

  fs::remove_all(tmp, ec);
  if (auto r = copy_tree(dir, tmp); !r)
    return r.error();
  if (auto r = fsync_tree(tmp); !r)
    return r.error();

  // The commit.
  fs::rename(tmp, dest, ec);
  if (ec)
    return elog(ec, "Unable to rename {} to {}", tmp, dest);
  if (auto r = fsync_path(root_); !r)
    return r.error();

  fs::remove(lease(pending.node), ec);
  log(INFO, "Stored the backup {}", dest);
  return info;
}

Result<void> DirBackupStore::abort(RaftNodeId node) {
  std::error_code ec;
  fs::remove(lease(node), ec);
  if (ec)
    return elog(ec, "Unable to remove {}", lease(node));
  return remove_tmp(node);
}

Result<vector<PendingBackup>> DirBackupStore::pending() const {
  vector<PendingBackup> result;
  std::error_code       ec;
  for (auto const& entry : fs::directory_iterator(root_, ec)) {
    auto name = entry.path().filename().string();
    if (!name.starts_with(kLeasePrefix) || name.ends_with(".new"))
      continue;

    uint16_t node = 0;
    auto     id   = std::string_view{name}.substr(kLeasePrefix.size());
    if (auto r = std::from_chars(id.data(), id.data() + id.size(), node);
        r.ec != std::errc{} || r.ptr != id.data() + id.size())
      continue;

    int64_t started = 0;
    {
      std::ifstream in(entry.path());
      if (!(in >> started))
        continue;  // Being replaced, or removed: it is not a lease now.
    }

    struct stat st{};
    if (::stat(entry.path().c_str(), &st) != 0)
      continue;

    result.push_back(PendingBackup{
        .node      = RaftNodeId(node),
        .started   = static_cast<std::time_t>(started),
        .refreshed = st.st_mtime,
    });
  }
  if (ec)
    return elog(ec, "Unable to list {}", root_);

  std::ranges::sort(result, {}, [](PendingBackup const& p) { return INT(p.node); });
  return result;
}

Result<vector<BackupInfo>> DirBackupStore::list() const {
  vector<BackupInfo> result;
  std::error_code    ec;
  for (auto const& entry : fs::directory_iterator(root_, ec)) {
    auto name = entry.path().filename().string();
    if (name.starts_with('.') || !entry.is_directory(ec))
      continue;
    if (auto info = parse_backup_name(name))
      result.push_back(*info);
    else
      log(WARN, "Ignoring {} in the backup store: not a backup name", entry.path());
  }
  if (ec)
    return elog(ec, "Unable to list {}", root_);

  std::ranges::sort(result);
  return result;
}

Result<void> DirBackupStore::get(BackupInfo const& backup, fs::path const& dest) const {
  auto src = root_ / backup.name();

  std::error_code ec;
  if (!fs::is_directory(src, ec))
    return elog(system_error_code(ENOENT), "No backup {}", src);

  if (fs::exists(dest, ec)) {
    if (!fs::is_empty(dest, ec))
      return elog(system_error_code(ENOTEMPTY), "{} is not empty", dest);
    fs::remove(dest, ec);
  }
  fs::create_directories(dest.parent_path(), ec);

  return copy_tree(src, dest);
}

Result<void> DirBackupStore::remove(BackupInfo const& backup) {
  auto path = root_ / backup.name();

  std::error_code ec;
  if (!fs::is_directory(path, ec))
    return elog(system_error_code(ENOENT), "No backup {}", path);
  fs::remove_all(path, ec);
  if (ec)
    return elog(ec, "Unable to remove {}", path);
  return ok();
}
