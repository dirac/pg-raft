// pg_raft_node --config config.toml --id N
//
// One pg-raft node (docs/node.md): restores the latest backup if it has no data directory, then
// runs PostgreSQL, raft, the FUSE mount over pg_wal and the WAL sender until SIGTERM or SIGINT.

#include <csignal>

#include "src/base/io_threads.h"
#include "src/base/logger.h"
#include "src/node/node.h"

int main(int argc, char** argv) {
  string                  config_file;
  std::optional<unsigned> id;
  for (int i = 1; i < argc; ++i) {
    auto arg = std::string_view{argv[i]};
    if (arg == "--config" && i + 1 < argc)
      config_file = argv[++i];
    else if (arg == "--id" && i + 1 < argc)
      id = static_cast<unsigned>(std::stoul(argv[++i]));
    else
      id.reset(), config_file.clear(), i = argc;
  }
  if (config_file.empty() || !id) {
    std::cerr << "usage: pg_raft_node --config config.toml --id N\n";
    return 2;
  }

  // SIGTERM and SIGINT are waited for, not handled.
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGTERM);
  sigaddset(&signals, SIGINT);
  pthread_sigmask(SIG_BLOCK, &signals, nullptr);

  IoThreads io{8};
  auto      node = Node::create(config(config_file), RaftNodeId(static_cast<uint16_t>(*id)));
  if (auto r = node->start(); !r) {
    node->shutdown();
    return 1;
  }

  int sig = 0;
  sigwait(&signals, &sig);
  log(INFO, "Signal {}: shutting down", sig);
  node->shutdown();
  return 0;
}
