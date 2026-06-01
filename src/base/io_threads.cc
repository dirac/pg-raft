#include "src/base/io_threads.h"

#include "src/base/logger.h"

asio::io_context& io_context() {
  static asio::io_context ctx;
  return ctx;
}

IoThreads::IoThreads(asio::io_context& ctx, unsigned threads)
    : ctx_(ctx), work_(ctx_.get_executor()) {
  LOG_INFO("Starting {} threads", threads);
  while (threads_.size() < threads)
    threads_.emplace_back([this]() { ctx_.run(); });
}

void IoThreads::stop() {
  work_.reset();
  if (!ctx_.stopped())
    ctx_.stop();

  std::for_each(std::begin(threads_), std::end(threads_), [](auto& th) { th.join(); });
  threads_.clear();
}
