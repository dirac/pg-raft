#ifndef LIB_SERVERS_ZOOKEEPER_ZK_TEST_SERVICE_HPP_
#define LIB_SERVERS_ZOOKEEPER_ZK_TEST_SERVICE_HPP_

#include "lib/servers/zookeeper/raft_server.hpp"

namespace banyandb::servers::zookeeper::test {

using namespace banyandb::base;

struct ZkTestService {
  ZkTestService(Certificates const& certs,
                Config&             config,
                unsigned            nodes   = 3,
                unsigned            threads = 1,
                Options             op      = Options{});

  // Start the service and populate the config information.
  void start_all();

  void shutdown();

  /// The leader.
  std::optional<unsigned> leader() const;

  void start(unsigned idx);
  void stop(unsigned idx, bool graceful);

  /// Step down as a leader.
  bool stepdown();

  void breakpoint();

 private:
  struct IoThreads {
    // Spawn threads and adds work to the io_context.
    IoThreads(::boost::asio::io_context& io_context, unsigned threads = 1)
        : io_context_(io_context), num_threads_(threads) {}

    // Terminates the threads.
    ~IoThreads();

    void start();

    // Stop the io_context.
    void stop();

   private:
    boost::asio::io_context& io_context_;
    unsigned const           num_threads_;

    shared_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> work_{
        nullptr};
    banyandb::base::list<std::thread> threads_;
  };

  unsigned const nodes_;
  Options const  options_;

  template <typename T>
  T& elem(unsigned idx, std::list<T>& c) {
    unsigned n = 0;

    for (auto& v : c)
      if (n == idx)
        return v;
      else
        ++n;
    throw;
  }

  boost::asio::io_context& io_context(unsigned idx) {
    return elem<boost::asio::io_context>(idx, io_contexts_);
  }

  IoThreads& io_threads(unsigned idx) { return elem<IoThreads>(idx, io_threads_); }

  std::list<boost::asio::io_context> io_contexts_;
  std::list<IoThreads>               io_threads_;

  Config&             config_;
  Certificates const& certs_;

  std::vector<shared_ptr<RaftServer>>                        servers_;
  std::vector<boost::asio::ip::tcp::endpoint>                endpoints_;
  std::vector<std::optional<boost::asio::ip::tcp::acceptor>> acceptors_;
};

}  // namespace banyandb::servers::zookeeper::test

#endif
