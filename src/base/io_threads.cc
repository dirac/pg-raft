#include "src/base/io_threads.h"

#include "src/base/logger.h"

asio::io_context& io_context() {
  static asio::io_context ctx;
  return ctx;
}

IoThreads::IoThreads(unsigned threads) {
  info("Starting {} threads", threads);
  while (threads_.size() < threads)
    threads_.emplace_back([]() { io_context().run(); });
}

void IoThreads::stop() {
  if (!io_context().stopped())
    io_context().stop();

  std::for_each(std::begin(threads_), std::end(threads_), [](auto& th) { th.join(); });
  threads_.clear();
}
