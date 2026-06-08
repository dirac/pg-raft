#include "lib/servers/zookeeper/raft_server.hpp"

#include "lib/net/async_connect.hpp"

namespace banyandb::servers::zookeeper {

namespace {

std::optional<string> service(RaftNodeId id, Config const& cfg) {
  auto n = INT(id);

  for (auto const& zk : cfg.zk_servers) {
    if (n == 0)
      return zk.hostname;
    --n;
  }
  ABORT(fn::format(
      "Invalid node id {0}, only {1} servers are configured", INT(id), cfg.zk_servers.size()));
  return std::nullopt;
}
}  // namespace

RaftServer::RaftServer(RaftNodeId                     id,
                       boost::asio::ip::tcp::acceptor acceptor,
                       Certificates const&            certs,
                       Config const&                  config,
                       bool                           enable_ssl,
                       Options                        options)
    : banyandb::net::Server(std::move(acceptor),
                            certs,
                            wdir(config, Role::kZookeeper, id) / "tmp",
                            enable_ssl,
                            service(id, config)),
      config_(config),
      self_id_(id),
      log_prefix_(fn::format("({0}) ", INT(self_id_))),
      dir_(wdir(config, Role::kZookeeper, self_id_)),
      log_(dir_ / "log"),
      timer_(io_context()),
      match_indexes_(self_id_, config.zk_servers.size()),
      votes_(config.zk_servers.size()),
      random_election_timeout_(
          static_cast<int>(options.election_timeout + 20U * INT(self_id_)),
          static_cast<int>(2U * (options.election_timeout + 20U * INT(self_id_)))),
      options_(options) {
  /// Can only vote after getting the term numbers from majority
  can_vote_.clear();
  stepdown_.clear();

  connections_ = fn::make_shared<PeerConnectionMgr>(
      io_context(), self_id_, config, Certificates{config}, dir_, options_);
}

RaftServer::~RaftServer() {
  LOG_INFO << log_prefix_ << "destroyed";
}

void RaftServer::start() {
  if (auto r = log_.open_term_db(); !r)
    system_error(r.error());
  else
    current_term_ = r.value();

  if (auto r = log_.open_logs_db(); !r)
    system_error(r.error());

  state_          = load_snapshot();
  commit_idx_     = state_.log_idx();
  compaction_idx_ = commit_idx_;

  if (current_term_.load() > RaftTerm{0})
    can_vote_.test_and_set();

  gather_terms();

  do_accept();
}

void RaftServer::gather_terms() {
  if (auto lk = usage_lock(); lk) {
    if (can_vote()) {
      update_last_ping(true);
      return periodic();
    }

    connections_->reconnect(
        self<RaftServer>(), config(), [this, s = self()](shared_ptr<RaftPeerConnection> conn) {
          on_connect(std::move(conn));
        });

    if (auto lk = timer_lock(); lk) {
      timer_.expires_after(std::chrono::milliseconds(10));
      timer_.async_wait([this, s = self()](boost::system::error_code const&) { gather_terms(); });
    }
  }
}

void RaftServer::periodic() {
  if (auto lk = usage_lock(); lk) {
    auto t = std::chrono::milliseconds(options_.election_timeout >> 4);

    if ((is_leader() && !resigning()) || (is_candidate() && has_votes())) {
      if (!tick())
        send_append_entries(/*check_sync_flag*/ false);

      if (auto cmds = state_.expire_sessions(); !cmds.empty())
        exec(
            ZkRequest{
                .session_id = 0,
                .request_no = -1,
                .commands   = std::move(cmds),
            },
            [](ZkResponse) {});

    } else if (is_follower()) {
      auto idx = apply_entries();
      if (compaction_idx() + options_.compaction_lag < idx) {
        auto lk = try_unique_lock();
        if (lk && (compaction_idx() + options_.compaction_lag < idx) && is_follower()) {
          auto last_entry = value(log().last_entry());
          LOG_INFO << log_prefix_ << "Compacting ... commit_idx = " << idx
                   << " last_entry = " << std::get<RaftIndex>(last_entry);

          if (auto r = log_.save_snapshot(state_.copy(), options_.keep_old_logs); !r)
            system_error(r.error());

          assert(last_entry <= value(log().last_entry()));
        }
        compaction_idx_.store(idx);
      }

      auto c = epoch_msec();
      auto p = last_ping_epoch();
      auto r = options_.election_timeout - 10U;

      if (c > p + r) {
        r = random_election_timeout();
        if (c > p + r) {
          t = std::chrono::milliseconds(options_.election_timeout >> 4);

          LOG_INFO << log_prefix_ << "@ " << c << " the last ping was @ " << p
                   << " election timeout = " << r;

          make_candidate();
        }
      }
      t = std::min(t,
                   std::chrono::milliseconds(std::max(c - (p + options_.election_timeout), 10U)));
    }

    connections_->reconnect(
        self<RaftServer>(), config(), [this, s = self()](shared_ptr<RaftPeerConnection> conn) {
          on_connect(std::move(conn));
        });

    if (auto lk = timer_lock(); lk) {
      timer_.expires_after(t);
      timer_.async_wait([this, s = self()](boost::system::error_code const& ec) {
        if (!ec)
          periodic();
      });
    }
  }
}

void RaftServer::new_connection(char const*           role,
                                string                peer,
                                std::filesystem::path tmpdir,
                                socket_type           socket) {
  if (strcmp(role, to_string(Role::kZookeeper)) == 0) {
    connections_->reset_last_connect_attempts();
    Connection<>::create<RaftFollowerConnection>(
        self_id_, std::move(peer), std::move(socket), self<RaftServer>(), tmpdir);
  } else
    Connection<>::create<RaftClientConnection>(
        self<RaftServer>(), role, std::move(peer), std::move(socket));
}

void RaftServer::on_connect(shared_ptr<RaftPeerConnection> conn) {
  if (is_leader()) {
    LOG_INFO << log_prefix_ << "Connected to node=" << conn->peer_id() << ", send_append_entries";
    conn->send_append_entries(log(), leadership_term(), commit_idx(), snapshot());
  } else if (is_candidate()) {
    if (has_votes()) {
      LOG_INFO << log_prefix_ << "Connected to node=" << conn->peer_id()
               << ", send_append_entries (have enough votes as a candidate)";
      conn->send_append_entries(log(), leadership_term(), commit_idx(), snapshot());
    } else {
      auto term = votes_.term();
      LOG_INFO << log_prefix_ << "Connected to node=" << conn->peer_id()
               << ", send_request_vote for term=" << term;
      conn->send_request_vote(log(), term);
    }
  } else if (!can_vote()) {
    LOG_INFO << log_prefix_ << "Connected to node=" << conn->peer_id() << ", send_ping";
    conn->send_ping(log());
  }
}

void RaftServer::exec(ZkRequest request, std::function<void(ZkResponse)> handler) {
  auto usage_lk = usage_lock();

  if (!is_leader() || !usage_lk || shutdown_signal() || resigning())
    return handler(ZkResponse{
        .status = ResultStatus::kInvalidLeader,
    });

  if (!ZkState::needs_quorum(request)) {
    return handler(state_.exec(std::move(request)));
  }

  if (auto lk = unique_lock(); lk) {
    if (!is_leader() || shutdown_signal() || resigning()) [[unlikely]] {
      return handler(ZkResponse{
          .status = ResultStatus::kInvalidLeader,
      });
    }

    last_entry_time_ = std::time(nullptr);

    // Get the last entry.
    auto [prev_log_term, prev_log_idx] = value(log().last_entry());

    auto entries = vector{Entry{
        .time = last_entry_time_,
        .term = current_term(),
        .cmd  = std::move(request),
    }};

    if (append_entries(prev_log_idx, prev_log_term, entries, false) == false)
      system_error(system_error_code(EINVAL));

    handlers_.emplace_back(prev_log_idx + 1, std::move(handler), epoch_msec());
    queue_size_.store(i8(handlers_.size()), std::memory_order_relaxed);
  }

  send_append_entries(/*check_sync_flag=*/true);
}

bool RaftServer::tick(bool force) {
  if (!is_leader())
    return false;

  if (!force && std::time(nullptr) <
                    (last_entry_time_.load(std::memory_order_relaxed) + options_.tick_seconds))
    return false;

  LOG_INFO << "Counter " << RaftClientConnection::counter.load();

  last_entry_time_.store(std::time(nullptr), std::memory_order_relaxed);

  // Get the last entry.
  auto [prev_log_term, prev_log_idx] = value(log().last_entry());

  // Create a tick entry.
  auto entries = vector{Entry{
      .time = last_entry_time_,
      .term = current_term(),
  }};

  if (force) {
    LOG_INFO << log_prefix_ << "Adding an entry after " << prev_log_idx;
  }

  // Add to the local log. If it fails, the reason being another entry got added.
  if (append_entries(prev_log_idx, prev_log_term, entries, false)) {
    LOG_INFO << log_prefix_ << "Added a tick entry idx= " << (prev_log_idx + 1U);
    send_append_entries(/*check_sync_flag=*/false);
    return true;
  }
  return false;
}

unsigned RaftServer::send_append_entries(bool check_sync_flag) {
  auto s = snapshot();
  return connections_->post([this, s, check_sync_flag](shared_ptr<RaftPeerConnection> conn) {
    if (check_sync_flag && !conn->in_sync())
      return;
    conn->send_append_entries(log(), leadership_term(), commit_idx(), s);
  });
}

unsigned RaftServer::send_request_votes(RaftTerm term) {
  return connections_->post([this, s = self(), term](shared_ptr<RaftPeerConnection> conn) {
    conn->send_request_vote(log(), term);
  });
}

void RaftServer::make_follower(std::source_location loc) {
  if (is_follower())
    return;

  make_follower(unique_lock(), std::move(loc));
}

void RaftServer::make_follower(RaftServer::UniqueLock lk, std::source_location loc) {
  (void)lk;

  if (is_follower())
    return;

  LOG_INFO << log_prefix_ << "Becoming a follower @ " << loc.function_name() << "("
           << loc.file_name() << ':' << loc.line() << ")";
  update_last_ping(false);

  if (is_leader()) {
    leadership_term_.store(RaftTerm{0}, std::memory_order_relaxed);
    LOG_INFO << log_prefix_ << "Sending invalid leader to " << handlers_.size() << " requests";
    for (auto& [_, handler, time] : handlers_) {
      handler(ZkResponse{
          .status = ResultStatus::kInvalidLeader,
      });
      LOG_INFO << log_prefix_ << "Waiting in the queue for " << (epoch_msec() - time)
               << " milliseconds";
    }
    handlers_.clear();
    queue_size_.store(i8(handlers_.size()), std::memory_order_relaxed);
  }
  type_.store(NodeType::kFollower);

  stepdown_.clear();
}

void RaftServer::make_candidate(unsigned inc) {
  if (!is_follower())
    return;

  make_candidate(inc, unique_lock());
}

void RaftServer::make_candidate(unsigned inc, RaftServer::UniqueLock lk) {
  if (!is_follower())
    return;

  RaftTerm new_term = current_term() + inc;

  LOG_INFO << log_prefix_ << "New candidate with term=" << new_term;

  if (new_term == std::get<RaftTerm>(set_current_term(new_term)) && vote(new_term, self_id_)) {
    type_.store(NodeType::kCandidate);
    votes_.clear(self_id_, new_term);
    leadership_term_.store(RaftTerm{0}, std::memory_order_relaxed);

    if (send_request_votes(new_term) != (nodes() - 1U)) {
      connections_->reconnect(
          self<RaftServer>(), config(), [this, s = self()](shared_ptr<RaftPeerConnection> conn) {
            on_connect(std::move(conn));
          });
    }
  } else
    make_follower(std::move(lk));
}

void RaftServer::make_leader(RaftServer::UniqueLock lk) {
  (void)lk;

  if (!is_candidate())
    return;

  if (leadership_term() != RaftTerm{0})
    return;

  stepdown_.clear();
  election_request_.clear();

  auto last_entry = value(log().last_entry());
  LOG_INFO << log_prefix_ << "Becoming a leader term=" << current_term() << " with last_entry = ("
           << std::get<RaftTerm>(last_entry) << ", " << std::get<RaftIndex>(last_entry) << ")";

  match_indexes_.reset();
  leadership_term_.store(votes_.term());

  if (connections_->has_unconnected_peers())
    connections_->reconnect(
        self<RaftServer>(), config(), [this, s = self()](shared_ptr<RaftPeerConnection> conn) {
          on_connect(conn);
        });

  send_append_entries(/*check_sync_flag=*/false);
}

std::tuple<RaftTerm, bool> RaftServer::set_current_term(RaftTerm term) {
  RaftTerm curr_term = current_term();

  if (curr_term >= term)
    return {curr_term, true};

  // Update the stable state.
  RaftTerm new_term = value(log_.set_current_term(term));
  ASSERT(new_term >= curr_term);

  while (new_term > curr_term && !current_term_.compare_exchange_strong(/* &expected = */ curr_term,
                                                                        /*desired=*/new_term))
    ;

  return {current_term(), false};
}

RaftTerm RaftServer::update_current_term(RaftTerm term) {
  auto [curr_term, unchanged] = set_current_term(term);

  if (unchanged) [[likely]]
    return curr_term;

  LOG_INFO << log_prefix_ << "Term = " << curr_term;

  if (is_leader() || is_candidate())
    make_follower();

  return current_term();
}

RaftResponse RaftServer::handle_peer_request(RaftNodeId /*node_id*/,
                                             RaftTerm term,
                                             std::monostate) {
  // If we don't have a connection open one to node_id, maybe connect.
  return RaftResponse{
      .term = update_current_term(term),
  };
}

void RaftServer::handle_response(shared_ptr<RaftPeerConnection> conn,
                                 RaftTerm,
                                 RaftTerm peer_term,
                                 std::monostate) {
  RaftTerm const current_term = update_current_term(peer_term);

  if (can_vote())
    return;

  auto lk = unique_lock();

  votes_.set_vote(conn->peer_id(), true);
  if (has_votes()) {
    if (!can_vote_.test_and_set()) {
      LOG_INFO << log_prefix_ << "Now can vote for term greater than " << current_term;
      vote(current_term, self_id_);
    }
  }
}

RaftResponse RaftServer::handle_peer_request(RaftNodeId  candidate,
                                             RaftTerm    term,
                                             RequestVote request) {
  RaftTerm const current_term = update_current_term(term);
  if (term != current_term || !can_vote()) {
    LOG_INFO << log_prefix_ << "Rejected terms: " << term << " vs " << current_term
             << " from node=" << candidate;
    return RaftResponse{
        .term     = current_term,
        .response = RequestVoteResponse{.vote_granted = false},
    };
  }

  auto last_entry = value(log().last_entry());
  if (last_entry > request.last_entry) {
    LOG_INFO << log_prefix_ << "Rejected because log is not upto date ("
             << std::get<RaftTerm>(last_entry) << "," << std::get<RaftIndex>(last_entry) << ") vs "
             << "(" << std::get<RaftTerm>(request.last_entry) << ","
             << std::get<RaftIndex>(request.last_entry) << ")"
             << " from node=" << candidate;

    return RaftResponse{
        .term     = current_term,
        .response = RequestVoteResponse{.vote_granted = false},
    };
  }

  bool v = vote(current_term, candidate);
  if (v) {
    update_last_ping(true);
    LOG_INFO << log_prefix_ << "Voted for term=" << current_term << " from node=" << candidate;
  } else {
    LOG_INFO << log_prefix_ << "Rejected because already voted for term=" << current_term
             << " from node=" << candidate;
  }

  return RaftResponse{.term = current_term, .response = RequestVoteResponse{.vote_granted = v}};
}

void RaftServer::handle_response(shared_ptr<RaftPeerConnection> conn,
                                 RaftTerm                       req_term,
                                 RaftTerm                       peer_term,
                                 RequestVoteResponse            response) {
  RaftTerm const term = update_current_term(peer_term);

  // If the term has changed ignore the vote response, a stale vote.
  if (term != req_term)
    return;

  auto lk = unique_lock();

  if (!is_candidate())
    return;

  votes_.set_vote(conn->peer_id(), response.vote_granted);
  if (has_votes())
    return make_leader(std::move(lk));
  else if (has_lost_election())
    return make_follower(std::move(lk));
}

RaftResponse RaftServer::handle_peer_request(RaftNodeId /* leader */,
                                             RaftTerm term,
                                             InstallSnapshot /*request*/,
                                             std::filesystem::path snapshot_file) {
  auto current_term = update_current_term(term);

  auto lk = unique_lock();

  if (is_candidate() || is_leader())
    return RaftResponse{
        .term = current_term,
        .response =
            InstallSnapshotResponse{
                .success = false,
            },
    };

  update_last_ping(true);

  apply_db_snapshot(std::move(snapshot_file));
  state_          = load_snapshot();
  commit_idx_     = state_.log_idx();
  compaction_idx_ = commit_idx_;

  update_last_ping(true);

  return RaftResponse{
      .term = current_term,
      .response =
          InstallSnapshotResponse{
              .success = true,
              .idx     = std::get<RaftIndex>(value(log().last_entry())),
          },
  };
}

void RaftServer::handle_response(shared_ptr<RaftPeerConnection> conn,
                                 RaftTerm                       req_term,
                                 RaftTerm                       peer_term,
                                 InstallSnapshotResponse        response) {
  // If the term has changed ignore the response.
  if (req_term != update_current_term(peer_term))
    return;

  if (response.success) {
    update_match_index(conn->peer_id(), response.idx);
    if (auto lk = conn->should_send_append_entries(); lk) {
      conn->set_next_idx(response.idx + 1U, lk);
      conn->send_append_entries(std::move(lk), log(), leadership_term(), commit_idx(), snapshot());
    }
  }
}

RaftResponse RaftServer::handle_peer_request(RaftNodeId /*leader*/,
                                             RaftTerm      term,
                                             AppendEntries request) {
  auto current_term = update_current_term(term);

  // The peer is no longer a leader.
  if (current_term > term)
    return RaftResponse{
        .term = current_term,
        .response =
            AppendEntriesResponse{
                .success = false,
            },
    };

  // convert to a follower if not already one.
  make_follower();

  auto lk = unique_lock();

  bool success = request.prev_log_idx >= commit_idx() && append_entries(request.prev_log_idx,
                                                                        request.prev_log_term,
                                                                        request.entries,
                                                                        /*delete_entries=*/true);
  if (success) [[likely]] {
    update_last_ping(false);
    set_commit_idx(std::min(request.commit_idx, request.prev_log_idx + request.entries.size()));

    return RaftResponse{
        .term = current_term,
        .response =
            AppendEntriesResponse{
                .success = true,
                .idx     = request.prev_log_idx + request.entries.size(),
            },
    };
  }
  update_last_ping(true);

  // Logs don't match.
  auto [_, last_idx] = value(log().last_entry());

  auto log_idx = (last_idx < request.prev_log_idx) ? last_idx : request.prev_log_idx;
  auto log_term =
      log_idx >= commit_idx() ? value(log().get_term(log_idx)) : std::optional<RaftTerm>{};

  auto n = count(log_idx, log_term.value_or(RaftTerm{0}));

  return RaftResponse{.term     = current_term,
                      .response = AppendEntriesResponse{
                          .success = false,
                          .idx     = log_idx,
                          .term    = log_term,  // Can be std::nullopt.
                          .n       = n,
                      }};
}

void RaftServer::handle_response(shared_ptr<RaftPeerConnection> conn,
                                 RaftTerm                       req_term,
                                 RaftTerm                       peer_term,
                                 AppendEntriesResponse          response) {
  // If the term has changed ignore the response.
  if (req_term != update_current_term(peer_term))
    return;

  if (response.success) {
    conn->set_in_sync();
    update_match_index(conn->peer_id(), response.idx);

    if (is_candidate()) {
      auto last_entry = value(log().last_entry());

      if (commit_idx() == std::get<RaftIndex>(last_entry)) {
        LOG_INFO << log_prefix_ << "Full leadership term=" << req_term << " with last_entry = ("
                 << std::get<RaftTerm>(last_entry) << ", " << std::get<RaftIndex>(last_entry)
                 << ")";

        type_.store(NodeType::kLeader);
      } else
        LOG_INFO << log_prefix_ << "Waiting to become a leader .." << commit_idx() << " vs "
                 << std::get<RaftIndex>(last_entry);
    }

    if (stepping_down(/*acquire_lock=*/true) &&
        response.idx == std::get<RaftIndex>(value(log().last_entry()))) {
      // The node is a leader and the logs are synced, so send the election request to the first
      // candidate.
      if (is_leader() && should_send_election_request()) {
        conn->send_request_election(log(), leadership_term());
      }
    }
  } else if (auto lk = conn->should_send_append_entries(); lk) {
    conn->set_not_in_sync();
    if (!response.term)
      /// The node did not have an entry at the prev_log_idx, send a snapshot
      conn->send_install_snapshot(std::move(lk), log(), leadership_term(), snapshot());
    else {
      uint64_t  nth = std::min<uint64_t>(response.n.value(), 64);
      RaftIndex idx = response.idx;

      while (true) {
        std::optional<RaftTerm> log_term = value(log().get_term(idx));

        if (!log_term || response.term.value() == log_term.value() || nth == 0) {
          conn->set_next_idx(idx + 1, lk);
          conn->send_append_entries(
              std::move(lk), log(), leadership_term(), commit_idx(), snapshot());

          return;
        }
        --idx;
        --nth;
      }
    }
  } else
    conn->set_not_in_sync();
}

RaftResponse RaftServer::handle_peer_request(RaftNodeId /*leader*/,
                                             RaftTerm        term,
                                             RequestElection request) {
  RaftTerm const curr_term = update_current_term(term);

  if (curr_term == term && value(log().last_entry()) == request.last_entry) {
    make_candidate();

    return RaftResponse{
        .term = current_term(),
        .response =
            RequestElectionResponse{
                .success = true,
            },
    };
  }

  return RaftResponse{
      .term = curr_term,
      .response =
          RequestElectionResponse{
              .success = false,
          },
  };
}

void RaftServer::handle_response(shared_ptr<RaftPeerConnection>,
                                 RaftTerm,
                                 RaftTerm,
                                 RequestElectionResponse response) {
  if (!response.success)
    stepdown();
}

void RaftServer::update_match_index(RaftNodeId node_id, RaftIndex idx) {
  match_indexes_.match_index(node_id, idx);
  if (auto n_commit_idx = match_indexes_.commit_index(commit_idx()); n_commit_idx > commit_idx()) {
    set_commit_idx(n_commit_idx);
    apply_entries();
  }
}

RaftIndex RaftServer::apply_entries() {
  auto lk  = unique_lock();
  auto idx = state_.log_idx();
  while (commit_idx() > idx) {
    ++idx;

    Entry       e = entry(idx);
    std::time_t t = e.time;

    if (std::holds_alternative<std::monostate>(e.cmd))
      state_.set_timestamp(t, idx);
    else {
      auto response = state_.exec(t, idx, std::get<ZkRequest>(e.cmd));
      try {
        to_encoded_bytes(response);
      } catch (...) {
        std::cerr << "commands " << std::get<ZkRequest>(e.cmd).commands.size() << std::endl;
        for (auto const& cmds : response.results) {
          std::cerr << "index " << cmds.result.index() << std::endl;
        }
        throw;
      }

      while (!handlers_.empty() && std::get<RaftIndex>(handlers_.front()) < idx) {
        std::get<1>(handlers_.front())(ZkResponse{.status = ResultStatus::kInvalidLeader});
        handlers_.pop_front();
      }
      if (!handlers_.empty() && std::get<RaftIndex>(handlers_.front()) == idx) {
        auto d = (epoch_msec() - std::get<unsigned>(handlers_.front()));
        if (d > 20)
          LOG_INFO << log_prefix_ << "Time in the queue " << d << " milliseconds";
        std::get<1>(std::move(handlers_.front()))(std::move(response));
        handlers_.pop_front();
      }
      queue_size_.store(i8(handlers_.size()), std::memory_order_relaxed);
    }
  }
  return idx;
}

bool RaftServer::stepping_down(bool acquire_lock) const {
  if (!stepdown_.test()) [[likely]]
    return false;

  // Lock so that the thread setting the stepdown_ flag is able to populate the database with a
  // tick before this function returns.
  if (acquire_lock)
    auto lk = unique_lock();

  return true;
}

bool RaftServer::should_send_election_request() {
  return !election_request_.test_and_set();
}

void RaftServer::stepdown() {
  auto lk = unique_lock();

  if (!is_leader())
    return;

  LOG_INFO << log_prefix_ << "Stepping down";

  // Starting a step down.
  stepdown_.test_and_set();
  election_request_.clear();

  /// Send a last tick.
  tick(/*force=*/true);
}

void RaftServer::on_shutdown() {
  LOG_ERROR << log_prefix_ << "on_shutdown";
  if (auto lk = timer_lock(); lk)
    timer_.cancel();

  if (is_leader()) {
    stepdown();

    auto idx = 0;
    while (is_leader() && ++idx < 5 && (connections_->connected_peers() > (nodes() >> 2))) {
      LOG_INFO << log_prefix_ << "Waiting to complete leadership transfer ...";
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  close();
}

unsigned RaftServer::count(RaftIndex idx, RaftTerm term) const {
  unsigned result = 0;
  while (idx > commit_idx()) {
    if (auto t = value(log().get_term(idx)); t && *t == term) {
      ++result;
      --idx;
    } else
      break;
  }
  return result;
}

void RaftServer::system_error(std::error_code ec) const {
  LOG_ERROR << log_prefix_ << "Exiting ... " << ec.message();
  LOG_ERROR << boost::stacktrace::stacktrace{1, 5};
  exit(-1);
}

void RaftServer::close() {
  if (closed_.test_and_set())
    return;

  LOG_ERROR << log_prefix_ << "close";

  acceptor_cancel();

  if (auto lk = timer_lock(); lk)
    timer_.cancel();

  if (connections_)
    connections_->close();

  if (auto lk = unique_lock(); lk) {
    handlers_.clear();
    queue_size_.store(i8(handlers_.size()), std::memory_order_relaxed);
  }

  auto lk = std::unique_lock<std::shared_mutex>{usage_lock_};
  snapshot_.reset();
  log_.close();
}

}  // namespace banyandb::servers::zookeeper
