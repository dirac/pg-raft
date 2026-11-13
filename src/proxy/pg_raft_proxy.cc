// pg_raft_proxy --config config.toml
//
// The client entry point (docs/proxy.md): proxy_pg_port forwards to the PostgreSQL of the raft
// leader, proxy_pg_port + 1 + i to servers[i]. Runs until SIGTERM or SIGINT.

#include <csignal>

#include "src/base/io_threads.h"
#include "src/base/logger.h"
#include "src/proxy/proxy.h"

int main(int argc, char** argv) {
  string config_file;
  for (int i = 1; i < argc; ++i) {
    if (std::string_view{argv[i]} == "--config" && i + 1 < argc)
      config_file = argv[++i];
    else
      config_file.clear(), i = argc;
  }
  if (config_file.empty()) {
    std::cerr << "usage: pg_raft_proxy --config config.toml\n";
    return 2;
  }

  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGTERM);
  sigaddset(&signals, SIGINT);
  pthread_sigmask(SIG_BLOCK, &signals, nullptr);

  IoThreads io{4};
  auto      proxy = Proxy::create(io_context(), config(config_file));
  if (auto r = proxy->start(); !r)
    return 1;

  int sig = 0;
  sigwait(&signals, &sig);
  log(INFO, "Signal {}: shutting down", sig);
  proxy->stop();
  return 0;
}
