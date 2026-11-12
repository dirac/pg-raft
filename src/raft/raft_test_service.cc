#include "src/raft/raft_test_service.h"

#include "src/base/logger.h"
#include "src/base/xxhash.h"

using namespace std::chrono_literals;

void TestRaftServer::new_connection(string peer, socket_type socket) {
  auto conn = create_connection(std::move(peer), std::move(socket));

  auto lk = std::scoped_lock<std::mutex>{accepted_mtx_};
  std::erase_if(accepted_, [](auto const& c) { return c.expired(); });
  accepted_.emplace_back(conn);
}

void TestRaftServer::on_shutdown() {
  if (!crash_.test())
    return RaftServer::on_shutdown();

  log(ERROR, "Crashing {}", node_id());
  close();

  auto accepted = std::vector<weak_ptr<Connection>>{};
  {
    auto lk = std::scoped_lock<std::mutex>{accepted_mtx_};
    accepted.swap(accepted_);
  }
  for (auto const& c : accepted) {
    if (auto conn = c.lock(); conn)
      conn->shutdown();
  }
}

RaftTestService::RaftTestService(boost::asio::io_context& io_context,
                                 std::filesystem::path    dir,
                                 Config&                  config,
                                 Options                  options,
                                 unsigned                 nodes)
    : io_context_(io_context),
      dir_(std::move(dir)),
      nodes_(nodes),
      config_(config),
      options_(options) {
  config.servers.clear();

  for (auto i = 0U; i < nodes; ++i) {
    acceptors_.emplace_back(boost::asio::ip::tcp::acceptor{
        io_context, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0)});

    auto& acceptor = acceptors_.back();
    assert(acceptor.has_value());
    auto const port = acceptor->local_endpoint().port();

    endpoints_.emplace_back(boost::asio::ip::tcp::v4(), port);

    config.servers.emplace_back(
        Config::Server{.hostname = "127.0.0.1", .pg_port = 0, .ha_port = port});
  }
  servers_.resize(nodes_);
}

RaftTestService::~RaftTestService() {
  shutdown();
}

void RaftTestService::shutdown() {
  log(INFO, "Shutting down");

  auto servers = vector<shared_ptr<TestRaftServer>>{};
  {
    auto lk = std::unique_lock{servers_mtx_};
    servers.swap(servers_);
    servers_.resize(servers.size());
  }

  bool wait = false;
  std::ranges::for_each(servers, [&wait](auto& s) {
    if (s) {
      wait = true;
      s->shutdown();
      s.reset();
    }
  });
  if (wait)
    std::this_thread::sleep_for(2s);
}

std::optional<unsigned> RaftTestService::leader() const {
  auto servers = this->servers();
  for (unsigned idx = 0; idx < nodes_; ++idx) {
    if (servers.at(idx) && servers.at(idx)->is_leader())
      return idx;
  }
  return std::nullopt;
}

bool RaftTestService::stepdown() {
  // A loaded machine can delay heartbeats into an election: wait for its leader.
  for (auto end = std::chrono::steady_clock::now() + 30s; std::chrono::steady_clock::now() < end;
       std::this_thread::sleep_for(100ms)) {
    for (auto& s : servers()) {
      if (s && s->is_leader()) {
        log(INFO, "Stepping down {}", s->node_id());

        s->stepdown();
        return true;
      }
    }
  }
  return false;
}

RaftIndex RaftTestService::commit_idx() const {
  auto idx = RaftIndex{0};
  for (auto const& s : servers())
    if (s)
      idx = std::max(idx, s->inspect_for_test([](RaftState const&, RaftLog const&, RaftIndex c) {
        return c;
      }));
  return idx;
}

namespace {

/// Records the role changes of one server in the service.
struct TestObserver : public RaftObserver {
  TestObserver(RaftTestService& service, unsigned node) : service_(service), node_(node) {}

  void became_leader(RaftTerm term) override {
    service_.record({.node = node_, .term = term, .became_leader = true});
  }
  void stepped_down(RaftTerm term) override {
    service_.record({.node = node_, .term = term, .became_leader = false});
  }

  RaftTestService& service_;
  unsigned         node_;
};

}  // namespace

void RaftTestService::start(unsigned idx) {
  auto id = RaftNodeId{u2(idx)};

  auto a             = std::move(acceptors_.at(idx));
  acceptors_.at(idx) = std::nullopt;
  if (!a)
    a.emplace(io_context(), endpoints_.at(idx));

  auto server = Server::create<TestRaftServer>(id,
                                               std::move(a.value()),
                                               config_,
                                               raft_dir(idx),
                                               wal_dir(idx),
                                               options_,
                                               make_shared<TestObserver>(*this, idx));

  auto lk          = std::unique_lock{servers_mtx_};
  servers_.at(idx) = std::move(server);
}

void RaftTestService::wait_for_leader() const {
  std::this_thread::sleep_for(std::chrono::seconds(1));
  while (!leader().has_value())
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

void RaftTestService::stop(unsigned idx, bool delete_files, bool crash) {
  auto server = shared_ptr<TestRaftServer>{};
  {
    auto lk = std::unique_lock{servers_mtx_};
    server.swap(servers_.at(idx));
  }
  log(INFO, "Shutting down {}{}", server->node_id(), (crash ? " (crash)" : ""));

  if (crash)
    server->set_crash_on_shutdown();
  server->shutdown();
  server.reset();

  if (delete_files)
    log(ERROR, "Deleted {} files", std::filesystem::remove_all(raft_dir(idx)));
}

namespace {

/// What a server holds: its log and its state machine.
struct ServerView {
  unsigned  node{0};
  RaftIndex first{0};
  RaftIndex last{0};
  RaftState state;

  vector<Entry> entries;  // From first to last.

  Entry const* entry(RaftIndex idx) const {
    if (idx < first || idx > last)
      return nullptr;
    return &entries.at(idx - first);
  }
};

std::tuple<RaftIndex, RaftIndex, RaftIndex> positions(RaftServer const& s) {
  return s.inspect_for_test([](RaftState const& state, RaftLog const& log, RaftIndex) {
    return std::make_tuple(std::get<RaftIndex>(log.first_entry().value()),
                           std::get<RaftIndex>(log.last_entry().value()),
                           state.log_idx);
  });
}

ServerView view(unsigned node, RaftServer const& s) {
  return s.inspect_for_test([node](RaftState const& state, RaftLog const& log, RaftIndex) {
    auto v = ServerView{
        .node  = node,
        .first = std::get<RaftIndex>(log.first_entry().value()),
        .last  = std::get<RaftIndex>(log.last_entry().value()),
        .state = state,
    };
    v.entries = log.entries_from(v.first, (v.last - v.first) + 1U).value().value();
    assert(v.entries.size() == (v.last - v.first) + 1U);
    return v;
  });
}

/// The last entry that a majority of the nodes holds.
RaftIndex accepted_idx(vector<ServerView> const& views,
                       RaftIndex                 lowest,
                       RaftIndex                 highest,
                       unsigned                  majority) {
  RaftIndex accepted{0};
  for (auto idx = lowest; idx <= highest; ++idx) {
    for (auto const& v : views) {
      auto const* e = v.entry(idx);
      if (e == nullptr)
        continue;
      auto same = std::ranges::count_if(views, [idx, e](ServerView const& w) {
        auto const* f = w.entry(idx);
        return f != nullptr && *f == *e;
      });
      if (static_cast<unsigned>(same) >= majority) {
        accepted = idx;
        break;
      }
    }
  }
  return accepted;
}

}  // namespace

RaftTestService::Consistency RaftTestService::check_consistency(
    vector<Write> const& writes,
    std::chrono::seconds timeout) const {
  auto result = Consistency{};

  // Wait until every running server has the same log and has applied all of it.
  auto converged = false;
  auto end       = std::chrono::steady_clock::now() + timeout;
  while (!converged && std::chrono::steady_clock::now() < end) {
    auto pos = vector<std::tuple<RaftIndex, RaftIndex, RaftIndex>>{};
    for (auto const& s : servers())
      if (s)
        pos.emplace_back(positions(*s));

    converged = !pos.empty() && std::ranges::all_of(pos, [&pos](auto const& p) {
      return std::get<1>(p) == std::get<1>(pos.front()) && std::get<2>(p) == std::get<1>(p);
    });
    if (!converged)
      std::this_thread::sleep_for(50ms);
  }

  auto const servers = this->servers();

  auto views = vector<ServerView>{};
  for (unsigned idx = 0; idx < nodes_; ++idx)
    if (servers.at(idx))
      views.emplace_back(view(idx, *servers.at(idx)));

  if (views.empty()) {
    result.violations.emplace_back("No server is running");
    return result;
  }
  if (!converged)
    result.violations.emplace_back(
        std::format("The servers did not apply the same log in {0} seconds", timeout.count()));

  // An entry that a majority of the nodes holds is accepted, and so is every entry before it in any
  // log.
  auto const majority = (nodes_ / 2U) + 1U;

  auto lowest   = std::ranges::min(views, {}, &ServerView::first).first;
  auto highest  = std::ranges::max(views, {}, &ServerView::last).last;
  auto accepted = accepted_idx(views, lowest, highest, majority);

  // The accepted entries are the same in every log that holds them.
  for (auto idx = lowest; idx <= accepted; ++idx) {
    Entry const* e = nullptr;
    for (auto const& v : views) {
      auto const* f = v.entry(idx);
      if (f == nullptr)
        continue;
      if (e == nullptr) {
        e = f;
        result.log.emplace(idx, *f);
      } else if (!(*f == *e)) {
        result.violations.emplace_back(
            std::format("Accepted entry {0} differs: {1} on node {2}", idx, *f, v.node));
      }
    }
  }

  // The states that applied the same entries are the same (but for needs_backup, which is the
  // node's own).
  for (auto const& v : views) {
    auto const& w = views.front();
    if (v.state.log_idx != w.state.log_idx)
      continue;
    if (v.state.timestamp != w.state.timestamp || !(v.state.flushed_lsn == w.state.flushed_lsn) ||
        !(v.state.wal_end == w.state.wal_end))
      result.violations.emplace_back(
          std::format("The state of node {0} differs from node {1}: {2} vs {3}",
                      v.node,
                      w.node,
                      v.state,
                      w.state));
  }

  // The acknowledged writes are accepted, and every server has their bytes if it has their entries.
  for (auto const& w : writes) {
    if (w.idx > accepted) {
      result.violations.emplace_back(
          std::format("The acknowledged write {0} is not accepted", w.idx));
      continue;
    }
    if (auto pos = result.log.find(w.idx); pos != result.log.end()) {
      auto const& cmd = pos->second.cmd;
      if (!(cmd.segment == std::optional<LogSeqNumber>{w.segment}) ||
          !(cmd.command == decltype(cmd.command){w.op}))
        result.violations.emplace_back(
            std::format("The accepted entry {0} is not the acknowledged write: {1}", w.idx, cmd));
    }

    for (auto const& v : views) {
      if (w.idx < v.first || w.idx > v.state.log_idx)
        continue;

      auto seg = servers.at(v.node)->segment(w.segment);
      if (!seg) {
        result.violations.emplace_back(
            std::format("Node {0} has no segment {1}", v.node, w.segment));
        continue;
      }
      auto chk = xxhash32(seg.value()->const_buffer(w.op.offset, w.op.size));
      if (chk != w.checksum)
        result.violations.emplace_back(std::format(
            "Node {0} has other bytes for the write {1} at {2}", v.node, w.idx, w.op.offset));
    }
  }

  // At most one leader per term, and only a leader steps down, in the term it led.
  {
    map<RaftTerm, unsigned> leaders;
    for (auto const& e : role_events()) {
      if (e.became_leader) {
        if (auto [pos, added] = leaders.emplace(e.term, e.node); !added && pos->second != e.node)
          result.violations.emplace_back(std::format(
              "Two leaders in term {0}: nodes {1} and {2}", e.term, pos->second, e.node));
      } else if (auto pos = leaders.find(e.term); pos == leaders.end() || pos->second != e.node) {
        result.violations.emplace_back(
            std::format("Node {0} stepped down in term {1} without leading it", e.node, e.term));
      }
    }
  }

  // Every server has the history file of each timeline it applied.
  for (auto const& v : views) {
    for (auto idx = v.first; idx <= v.state.log_idx; ++idx) {
      auto const* e = v.entry(idx);
      auto const* t = e ? std::get_if<TimelineOp>(&e->cmd.command) : nullptr;
      if (!t)
        continue;
      auto content = read_history_file(wal_dir(v.node), u4(t->timeline));
      if (!content || *content != t->content)
        result.violations.emplace_back(std::format(
            "Node {0} applied the timeline {1} (entry {2}) but has no such history file",
            v.node,
            t->timeline,
            idx));
    }
  }

  log(INFO,
      "Consistency: {} servers, accepted up to {} (logs from {} to {}), {} writes, {} violations",
      views.size(),
      accepted,
      lowest,
      highest,
      writes.size(),
      result.violations.size());
  return result;
}

void RaftTestService::start_all() {
  for (auto idx = 0U; idx < nodes_; ++idx)
    start(idx);
}
