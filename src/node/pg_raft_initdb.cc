// pg_raft_initdb --config config.toml [--port N]
//
// Creates a cluster's first backup in the backup store (docs/node.md §4.1). Run it once, before
// any node starts. PostgreSQL runs on --port (default: servers[0].pg_port) while the backup is
// taken.

#include "src/backup/dir_backup_store.h"
#include "src/base/io_threads.h"
#include "src/base/logger.h"
#include "src/node/init.h"

int main(int argc, char** argv) {
  string   config_file;
  uint16_t port = 0;
  for (int i = 1; i < argc; ++i) {
    auto arg = std::string_view{argv[i]};
    if (arg == "--config" && i + 1 < argc)
      config_file = argv[++i];
    else if (arg == "--port" && i + 1 < argc)
      port = static_cast<uint16_t>(std::stoul(argv[++i]));
    else {
      std::cerr << "usage: pg_raft_initdb --config config.toml [--port N]\n";
      return 2;
    }
  }
  if (config_file.empty()) {
    std::cerr << "usage: pg_raft_initdb --config config.toml [--port N]\n";
    return 2;
  }

  auto cfg = config(config_file);
  if (cfg.backup_store.empty()) {
    log(ERROR, "{}: no backup_store", config_file);
    return 1;
  }
  if (port == 0) {
    if (cfg.servers.empty()) {
      log(ERROR, "{}: no servers, and no --port", config_file);
      return 1;
    }
    port = cfg.servers.at(0).pg_port;
  }

  IoThreads io{2};
  auto      store = DirBackupStore{cfg.backup_store};
  auto      r     = init_cluster(cfg, store, port);
  return r ? 0 : 1;
}
