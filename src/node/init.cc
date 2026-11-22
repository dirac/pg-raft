#include "src/node/init.h"

#include <unistd.h>

#include <fstream>

#include "src/base/logger.h"
#include "src/process/pg_instance.h"

namespace fs = std::filesystem;

fs::path pg_bin(Config const& config) {
  return fs::path{config.install_dir} / std::to_string(config.pg_major_version) / "bin";
}

Result<LogSeqNumber> backup_end_lsn(fs::path const& backup_dir) {
  auto          path = backup_dir / "backup_manifest";
  std::ifstream in(path);
  if (!in)
    return elog(system_error_code(ENOENT), "No backup_manifest in {}", backup_dir);

  try {
    auto manifest = boost::json::parse(string{std::istreambuf_iterator<char>(in), {}});
    auto ranges   = manifest.as_object().at("WAL-Ranges").as_array();
    if (ranges.empty())
      return elog(system_error_code(EBADMSG), "No WAL-Ranges in {}", path);

    // The range that ends last: PostgreSQL lists them newest timeline first, so a backup that
    // spans timelines (a standby's) has its oldest one last.
    std::optional<LogSeqNumber> best;
    for (auto const& r : ranges) {
      auto const& range    = r.as_object();
      auto        timeline = range.at("Timeline").to_number<uint32_t>();
      auto        end      = string{range.at("End-LSN").as_string()};
      unsigned    hi = 0, lo = 0;
      if (sscanf(end.c_str(), "%X/%X", &hi, &lo) != 2)
        return elog(system_error_code(EBADMSG), "Bad End-LSN {} in {}", end, path);
      auto const lsn = LogSeqNumber{.timeline = timeline, .lsn = (uint64_t{hi} << 32U) | lo};
      if (!best || best->lsn < lsn.lsn || (best->lsn == lsn.lsn && best->timeline < lsn.timeline))
        best = lsn;
    }
    return *best;
  } catch (std::exception const& e) {
    return elog(system_error_code(EBADMSG), "Unable to parse {}: {}", path, e.what());
  }
}

Result<BackupInfo> init_cluster(Config const& config, BackupStore& store, uint16_t port) {
  auto existing = store.list();
  if (!existing)
    return existing.error();
  if (!existing->empty())
    return elog(system_error_code(EEXIST),
                "The backup store already holds {} backup(s), the latest {}: not creating a "
                "second cluster in it",
                existing->size(),
                existing->back());

  auto scratch = fs::path{config.wdir} / std::format("pg_raft_initdb.{}", getpid());
  auto data    = scratch / "data";
  auto backup  = scratch / "backup";

  std::error_code ec;
  fs::remove_all(scratch, ec);
  fs::create_directories(scratch, ec);
  if (ec)
    return elog(ec, "Unable to create {}", scratch);

  auto pg = PgInstance{pg_bin(config), data, port, PgInstance::Role::kPrimary};

  // Stop PostgreSQL and remove the scratch directory however this ends.
  auto cleanup =
      std::unique_ptr<void, std::function<void(void*)>>{reinterpret_cast<void*>(1), [&](void*) {
                                                          if (pg.is_running())
                                                            pg.stop(PgInstance::StopMode::Fast);
                                                          std::error_code e;
                                                          fs::remove_all(scratch, e);
                                                        }};

  if (!pg.init())
    return elog(system_error_code(EIO), "initdb failed in {}", data);
  if (!pg.write_config(config.pg_config) || !pg.write_hba(config.pg_hba))
    return elog(system_error_code(EIO), "Unable to configure {}", data);
  if (!pg.start() || !pg.wait_ready(port))
    return elog(system_error_code(EIO), "PostgreSQL did not start on port {}", port);

  auto lk = pg.backup(backup);
  if (!lk)
    return elog(system_error_code(EIO), "pg_basebackup failed");
  lk = pg.verify_backup(backup, std::move(lk));
  if (!lk)
    return elog(system_error_code(EIO), "The backup in {} did not verify", backup);

  auto lsn = backup_end_lsn(backup);
  if (!lsn)
    return lsn.error();

  auto pending = store.begin(RaftNodeId(0), std::time(nullptr));
  if (!pending)
    return pending.error();
  auto info = store.put(*pending, backup, *lsn);
  if (!info) {
    (void)store.abort(RaftNodeId(0));
    return info.error();
  }

  log(INFO, "Created the cluster: backup {}", *info);
  return info;
}
