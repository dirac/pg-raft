#include "src/wal_fuse/pg_initializer.h"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>

#include "src/process/process.h"

PgInitializer::PgInitializer(std::string pg_bin_dir, std::string data_dir,
                              std::vector<std::string> initdb_args)
    : pg_bin_dir_(std::move(pg_bin_dir)),
      data_dir_(std::move(data_dir)),
      initdb_args_(std::move(initdb_args)) {}

bool PgInitializer::run() {
  // ── Step 1: initdb ──────────────────────────────────────────────────────────
  std::vector<std::string> args = {"-D", data_dir_};
  args.insert(args.end(), initdb_args_.begin(), initdb_args_.end());

  Process initdb(pg_bin_dir_ + "/initdb", std::move(args));
  if (!initdb.start()) return false;
  if (initdb.wait() != 0) return false;

  // ── Step 2: create pg_raft_wal ─────────────────────────────────────────────
  const std::string pg_raft_wal = data_dir_ + "/pg_raft_wal";
  if (mkdir(pg_raft_wal.c_str(), 0700) != 0 && errno != EEXIST)
    return false;

  // ── Step 3: move pg_wal contents into pg_raft_wal ─────────────────────────
  // pg_wal is left in place as an empty directory for the FUSE mount point.
  const std::string pg_wal = data_dir_ + "/pg_wal";

  DIR* dp = opendir(pg_wal.c_str());
  if (!dp) return false;

  bool ok = true;
  struct dirent* de;
  while ((de = readdir(dp)) != nullptr) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
      continue;
    const std::string src = pg_wal + "/" + de->d_name;
    const std::string dst = pg_raft_wal + "/" + de->d_name;
    if (rename(src.c_str(), dst.c_str()) != 0) {
      ok = false;
      break;
    }
  }
  closedir(dp);
  return ok;
}
