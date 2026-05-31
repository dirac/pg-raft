#pragma once

#include "src/base/base.h"

asio::io_context& io_context();

template <typename Func>
void post(Func func) {
  asio::post(io_context(), std::move(func));
}

class IoThreads {
 public:
  IoThreads(unsigned threads);

  ~IoThreads() { stop(); }

  void stop();

 private:
  list<std::thread> threads_;
};
