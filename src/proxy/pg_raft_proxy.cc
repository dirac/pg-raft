// pg_raft_proxy --config config.toml [--engine asio|epoll|uring] [--threads N] [--spin-us N]
//
// The client entry point (docs/proxy.md): proxy_pg_port forwards to the PostgreSQL of the raft
// leader, proxy_pg_port + 1 + i to servers[i]. Runs until SIGTERM or SIGINT.

#include <csignal>

#include "src/base/io_threads.h"
#include "src/base/logger.h"
#include "src/proxy/proxy.h"

int main(int argc, char** argv) {
  string config_file;
  auto   options = ProxyOptions{};
  bool   bad     = false;
  for (int i = 1; i < argc && !bad; ++i) {
    auto const arg = std::string_view{argv[i]};
    if (arg == "--config" && i + 1 < argc) {
      config_file = argv[++i];
    } else if (arg == "--engine" && i + 1 < argc) {
      auto const e = std::string_view{argv[++i]};
      if (e == "asio")
        options.engine = ProxyOptions::Engine::kAsio;
      else if (e == "epoll")
        options.engine = ProxyOptions::Engine::kEpoll;
      else if (e == "uring")
        options.engine = ProxyOptions::Engine::kUring;
      else
        bad = true;
    } else if (arg == "--threads" && i + 1 < argc) {
      options.threads = static_cast<unsigned>(std::stoul(argv[++i]));
    } else if (arg == "--spin-us" && i + 1 < argc) {
      options.spin_us = static_cast<unsigned>(std::stoul(argv[++i]));
    } else {
      bad = true;
    }
  }
  if (config_file.empty() || bad) {
    std::cerr << "usage: pg_raft_proxy --config config.toml [--engine asio|epoll|uring] [--threads "
                 "N] [--spin-us N]\n"
                 "  --engine   the data path: epoll (Linux, the default there: one thread per\n"
                 "             core) or asio\n"
                 "  --threads  epoll threads (default: one per core)\n";
    return 2;
  }

  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGTERM);
  sigaddset(&signals, SIGINT);
  pthread_sigmask(SIG_BLOCK, &signals, nullptr);

  IoThreads io{4};
  auto      proxy = Proxy::create(io_context(), config(config_file), options);
  if (auto r = proxy->start(); !r)
    return 1;

  int sig = 0;
  sigwait(&signals, &sig);
  log(INFO, "Signal {}: shutting down", sig);
  proxy->stop();
  return 0;
}
