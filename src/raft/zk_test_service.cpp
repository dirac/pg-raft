#include "lib/servers/zookeeper/zk_test_service.hpp"

namespace banyandb::servers::zookeeper::test {

void ZkTestService::IoThreads::stop() {
  if (io_context_.stopped())
    return;

  work_.reset();
  io_context_.stop();

  std::for_each(threads_.begin(), threads_.end(), [](auto& th) { th.join(); });
  threads_.clear();
}

void ZkTestService::IoThreads::start() {
  if (!work_)
    work_ =
        fn::make_shared<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
            io_context_.get_executor());

  while (threads_.size() < num_threads_)
    threads_.emplace_back([this]() {
      try {
        io_context_.run();
      } catch (...) {
        LOG_ERROR << "exception";
      }
    });
}

ZkTestService::IoThreads::~IoThreads() {
  stop();
}

ZkTestService::ZkTestService(Certificates const& certs,
                             Config&             config,
                             unsigned            nodes,
                             unsigned            threads,
                             Options             op)
    : nodes_(nodes), options_(op), config_(config), certs_(certs) {
  for (unsigned idx = 0; idx < nodes_; ++idx)
    io_contexts_.emplace_back();

  config.zk_servers.clear();

  for (auto& io : io_contexts_) {
    io_threads_.emplace_back(io, threads);
    acceptors_.emplace_back(boost::asio::ip::tcp::acceptor{
        io, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0)});

    endpoints_.emplace_back(boost::asio::ip::tcp::endpoint(
        boost::asio::ip::tcp::v4(), acceptors_.back()->local_endpoint().port()));

    config.zk_servers.emplace_back(Config::Zookeeper{
        .hostname = "127.0.0.1", .port = acceptors_.back()->local_endpoint().port()});
  }
  servers_.resize(nodes_);
}

void ZkTestService::shutdown() {
  std::for_each(std::begin(servers_), std::end(servers_), [](auto& s) {
    if (s) {
      s->shutdown();
      s.reset();
    }
  });
  std::for_each(std::begin(io_threads_), std::end(io_threads_), [](auto& th) { th.stop(); });
}

std::optional<unsigned> ZkTestService::leader() const {
  for (unsigned idx = 0; idx < nodes_; ++idx) {
    if (servers_.at(idx) && servers_.at(idx)->is_leader())
      return idx;
  }
  return std::nullopt;
}

bool ZkTestService::stepdown() {
  for (auto& s : servers_) {
    if (s && s->is_leader()) {
      s->stepdown();
      return true;
    }
  }
  return false;
}

void ZkTestService::start(unsigned idx) {
  io_threads(idx).start();

  if (acceptors_.at(idx)) {
    auto a             = std::move(acceptors_.at(idx));
    acceptors_.at(idx) = std::nullopt;

    servers_.at(idx) = Server::create<RaftServer>(
        RaftNodeId{u2(idx)}, std::move(a.value()), certs_, config_, false, options_);
  } else {
    auto a = boost::asio::ip::tcp::acceptor{io_context(idx), endpoints_.at(idx)};

    servers_.at(idx) = Server::create<RaftServer>(
        RaftNodeId{u2(idx)}, std::move(a), certs_, config_, false, options_);
  }
}

void ZkTestService::stop(unsigned idx, bool graceful) {
  if (graceful)
    servers_.at(idx)->shutdown();
  else
    servers_.at(idx)->close();

  servers_.at(idx).reset();
}

void ZkTestService::start_all() {
  for (auto idx = 0U; idx < nodes_; ++idx)
    start(idx);
}

void ZkTestService::breakpoint() {
  LOG_INFO << __func__;
}

}  // namespace banyandb::servers::zookeeper::test
