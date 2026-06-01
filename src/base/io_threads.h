#pragma once

#include "src/base/base.h"

asio::io_context& io_context();

template <typename Func>
void post(Func func) {
  asio::post(io_context(), std::move(func));
}

class IoThreads {
 public:
  IoThreads(unsigned threads) : IoThreads(io_context(), threads) {}

  IoThreads(asio::io_context& ctx, unsigned threads);

  ~IoThreads() { stop(); }

  void stop();

 private:
  asio::io_context&                                          ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> work_;
  list<std::thread>                                          threads_;
};
