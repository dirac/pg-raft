#include "src/wal_fuse/pg_initializer.h"

#include "src/process/pg_instance.h"

PgInitializer::PgInitializer(std::string pg_bin_dir, std::string data_dir,
                              std::vector<std::string> initdb_args)
    : pg_bin_dir_(std::move(pg_bin_dir)),
      data_dir_(std::move(data_dir)),
      initdb_args_(std::move(initdb_args)) {}

bool PgInitializer::run() {
  PgInstance pg(pg_bin_dir_, data_dir_);
  return pg.init(initdb_args_) && pg.create_raft_wal();
}
