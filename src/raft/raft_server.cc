#include "src/raft/raft_server.h"

#include "src/base/error_code.h"
#include "src/base/logger.h"
#include "src/net/async_connect.h"

RaftServer::RaftServer(RaftNodeId                     id,
                       boost::asio::ip::tcp::acceptor acceptor,
                       Config const&                  config,
                       std::filesystem::path          dir,
                       std::filesystem::path          wal_dir,
                       Options                        options,
                       shared_ptr<RaftObserver>       observer)
    : Server(std::move(acceptor)),
      config_(config),
      self_id_(id),
      log_prefix_(std::format("({0}) ", INT(self_id_))),
      dir_(std::move(dir)),
      wal_dir_(std::move(wal_dir)),
      cache_(make_shared<WalSegmentCache>()),
      log_(dir_ / "log", options.sync_commits, options.no_sync),
      observer_(observer ? std::move(observer) : make_shared<RaftObserver>()),
      timer_(io_context()),
      match_indexes_(self_id_, config.servers.size()),
      votes_(config.servers.size()),
      random_election_timeout_(
          static_cast<int>(options.election_timeout_msecs + (20U * INT(self_id_))),
          static_cast<int>(2U * (options.election_timeout_msecs + (20U * INT(self_id_))))),
      options_(options) {
  assert(INT(self_id_) < config.servers.size());

  std::filesystem::create_directories(wal_dir_);

  /// Can only vote after getting the term numbers from majority
  can_vote_.clear();
  stepdown_.clear();

  connections_ = make_shared<PeerConnectionMgr>(io_context(), self_id_, config, options_);
}

RaftServer::~RaftServer() {
  log(INFO, "{}destroyed", log_prefix_);
}

void RaftServer::start() {
  auto term = log_.open_term_db();
  if (!term)
    system_error(term.error());
  current_term_ = term.value();

  if (auto r = log_.open_logs_db(1U); !r)
    system_error(r.error());

  state_ = load_snapshot();
  publish_state();
  {
    auto lk             = std::unique_lock{published_mtx_};
    published_boundary_ = value(log_.load_boundary());
  }
  gc_thread_ = std::thread([this] { wal_gc(); });
  {
    auto lk  = std::unique_lock{gc_mtx_};
    gc_wake_ = true;
  }
  gc_cv_.notify_all();
  applied_idx_    = state_.log_idx;
  commit_idx_     = state_.log_idx;
  compaction_idx_ = commit_idx_.load();

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
          on_connect(conn);
        });

    if (auto lk = timer_lock(); lk) {
      timer_.expires_after(std::chrono::milliseconds(10));
      timer_.async_wait([this, s = self()](boost::system::error_code const&) { gather_terms(); });
    }
  }
}

void RaftServer::periodic() {
  if (auto lk = usage_lock(); lk) {
    auto t = std::chrono::milliseconds(options_.election_timeout_msecs >> 4U);

    check_stepdown_timeout();

    if ((is_leader() && !resigning()) || (is_candidate() && has_votes())) {
      bool ticked;
      {
        // tick() mutates the raft log; every other mutating path (stepdown(), exec()) holds
        // mtx_ while doing so.
        auto lk = unique_lock();
        ticked  = tick();
      }
      if (!ticked)
        send_append_entries(/*check_sync_flag*/ false);

      // A leader compacts its own log too: entries are small (the WAL is in the segments), and
      // stepping down to compact would be a PostgreSQL failover.
      if (is_leader() && compaction_idx() + options_.compaction_lag < applied_idx())
        compact(applied_idx(), try_unique_lock());

    } else if (is_follower()) {
      auto idx = apply_entries(false);
      if (compaction_idx() + options_.compaction_lag < idx)
        compact(idx, try_unique_lock());

      auto c = epoch_msec();
      auto p = last_ping_epoch();

      // Draw the timeout once per ping; drawing on every check would make every node time out
      // shortly after the minimum, causing split votes.
      if (p != election_timeout_epoch_) {
        election_timeout_epoch_ = p;
        election_timeout_       = random_election_timeout();
      }
      auto r = election_timeout_;

      if (c > p + r) {
        log(INFO,
            "{}@ {} ms the last ping was @ {} ms election timeout = {} ms",
            log_prefix_,
            c,
            p,
            r);
        log_untriggered_election(c);

        make_candidate();
      } else {
        // Wake up at the election deadline.
        t = std::min(t, std::chrono::milliseconds(std::max(p + r - c, 1U)));
      }
    }

    connections_->reconnect(
        self<RaftServer>(), config(), [this, s = self()](shared_ptr<RaftPeerConnection> conn) {
          on_connect(conn);
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

void RaftServer::new_connection(string peer, socket_type socket) {
  create_connection(std::move(peer), std::move(socket));
}

shared_ptr<Connection> RaftServer::create_connection(string peer, socket_type socket) {
  if (peer.starts_with("Raft:")) {
    connections_->reset_last_connect_attempts();
    return Connection::create<RaftFollowerConnection>(
        self_id_, std::move(peer), std::move(socket), self<RaftServer>());
  }
  return Connection::create<RaftClientConnection>(
      self<RaftServer>(), std::move(peer), std::move(socket));
}

void RaftServer::on_connect(shared_ptr<RaftPeerConnection> const& conn) {
  if (is_leader()) {
    log(INFO, "{}Connected to node={}, send_append_entries", log_prefix_, conn->peer_id());
    conn->send_append_entries(raft_log(), leadership_term(), commit_idx());
  } else if (is_candidate()) {
    if (has_votes()) {
      log(INFO,
          "{}Connected to node={}, send_append_entries (have enough votes as a candidate)",
          log_prefix_,
          conn->peer_id());
      conn->send_append_entries(raft_log(), leadership_term(), commit_idx());
    } else {
      auto term = votes_.term();
      log(INFO,
          "{}Connected to node={}, send_request_vote for term={}",
          log_prefix_,
          conn->peer_id(),
          term);
      conn->send_request_vote(raft_log(), term);
    }
  } else if (!can_vote()) {
    log(INFO, "{}Connected to node={}, send_ping", log_prefix_, conn->peer_id());
    conn->send_ping(raft_log());
  }
}

void RaftServer::backup(SnapshotRequest request, BackupHandler handler) {
  std::function<void(SnapshotRequest, BackupHandler)> provider;
  {
    auto lk  = std::unique_lock{backup_mtx_};
    provider = backup_provider_;
  }
  if (!provider)
    return handler(SnapshotResponse{.success = false, .files = {}, .links = {}}, {});
  provider(std::move(request), std::move(handler));
}

void RaftServer::exec(WalCommand cmd, ExecHandler handler) {
  assert(handler);

  auto not_leader = [&handler] { handler(make_error_code(ErrorCode::kNotLeader)); };

  auto usage_lk = usage_lock();
  if (!usage_lk)
    return not_leader();

  if (!is_leader() || shutdown_signal() || resigning()) {
    usage_lk.unlock();
    return not_leader();
  }

  if (auto lk = unique_lock(); lk) {
    if (!is_leader() || shutdown_signal() || resigning()) [[unlikely]] {
      lk.unlock();
      return not_leader();
    }

    last_entry_time_ = std::time(nullptr);

    auto entries = vector{
        Entry{
            .time = last_entry_time_,
            .term = current_term(),
            .cmd  = std::move(cmd),
        },
    };

    // Get the last entry.
    auto [prev_log_term, prev_log_idx] = value(raft_log().last_entry());

    while (!append_entries(prev_log_idx, prev_log_term, entries, false))
      std::tie(prev_log_term, prev_log_idx) = value(raft_log().last_entry());

    handlers_.emplace_back(prev_log_idx + 1U, std::move(handler), epoch_msec());
  }

  send_append_entries(/*check_sync_flag=*/true);
}

void RaftServer::set_needs_backup(bool needs_backup) {
  auto lk = unique_lock();
  if (state_.needs_backup == needs_backup)
    return;

  log(INFO, "{}needs_backup = {}", log_prefix_, needs_backup);
  state_.needs_backup = needs_backup;
  // Saving the state with a log size this large deletes no entry.
  value(log_.save_snapshot(state_, std::numeric_limits<unsigned>::max()));
  publish_state();
}

bool RaftServer::restored_backup(LogSeqNumber end) {
  auto lk = unique_lock();
  if (state_.first_segment.lsn > end.lsn - end.lsn % kWalSegmentSize) {
    log(INFO,
        "{}The restored backup ends at {}, before the first segment {}: needs_backup stays set",
        log_prefix_,
        end,
        state_.first_segment);
    return false;
  }
  if (state_.needs_backup) {
    log(INFO, "{}needs_backup = false (restored a backup ending at {})", log_prefix_, end);
    state_.needs_backup = false;
    value(log_.save_snapshot(state_, std::numeric_limits<unsigned>::max()));
    publish_state();
  }
  return true;
}

void RaftServer::publish_state() {
  auto lk    = std::unique_lock{published_mtx_};
  published_ = state_;
}

RaftState RaftServer::applied_state() const {
  auto lk = std::unique_lock{published_mtx_};
  return published_;
}

RaftState RaftServer::boundary_state() const {
  auto lk = std::unique_lock{published_mtx_};
  return published_boundary_;
}

std::optional<TimelineOp> RaftServer::latest_timeline() const {
  auto timeline = latest_history_timeline(wal_dir_);
  if (!timeline)
    return std::nullopt;
  auto content = read_history_file(wal_dir_, *timeline);
  if (!content) {
    elog(content.error(),
         "{}Unable to read the history file of timeline {}",
         log_prefix_,
         *timeline);
    return std::nullopt;
  }
  return TimelineOp{.timeline = *timeline, .content = std::move(*content)};
}

Result<shared_ptr<WalSegment>> RaftServer::segment(LogSeqNumber const& lsn) const {
  auto name = filename(lsn);
  auto file = wal_dir_ / name;

  if (!std::filesystem::exists(file) && !std::filesystem::exists(wal_dir_ / ("." + name))) {
    // A segment that the leader has started: create it zero filled.
    int fd = ::open(file.c_str(), O_RDWR | O_CREAT | O_EXCL, file_mode());
    if (fd == -1 && errno != EEXIST)
      return elog(system_error_code(errno), "{}Unable to create {}", log_prefix_, file);
    if (fd != -1) {
      int  rc = ::ftruncate(fd, kWalSegmentSize);
      auto e  = errno;
      ::close(fd);
      if (rc != 0)
        return elog(system_error_code(e), "{}Unable to size {}", log_prefix_, file);
    }
  }

  auto seg = make_shared<WalSegment>(cache_, wal_dir_, lsn);
  if (auto r = seg->open(); !r)
    return r.error();
  return seg;
}

void RaftServer::write_blocks(inplace_vector<MemBlock, 2> const& blocks, Bytes const& bytes) {
  uint8_t const* data = bytes.data();
  for (auto const& b : blocks) {
    auto seg = value(segment(b.lsn));
    seg->write(data, b.offset, b.size);
    if (options_.follower_sync)
      value(seg->fsync(b.offset, b.size));
    data += b.size;
  }
}

bool RaftServer::tick(bool force) {
  if (!is_leader())
    return false;

  if (!force && std::time(nullptr) < (last_entry_time_.load(std::memory_order_relaxed) +
                                      static_cast<std::time_t>(options_.tick_seconds)))
    return false;

  last_entry_time_.store(std::time(nullptr), std::memory_order_relaxed);

  // Get the last entry.
  auto [prev_log_term, prev_log_idx] = value(raft_log().last_entry());

  // Create a tick entry.
  auto entries = vector{
      Entry{
          .time = last_entry_time_,
          .term = current_term(),
          .cmd  = {},
      },
  };

  if (force)
    log(INFO, "{}Adding an entry after {}", log_prefix_, prev_log_idx);

  // Add to the local log. If it fails, the reason being another entry got added.
  if (append_entries(prev_log_idx, prev_log_term, entries, false)) {
    log(DEBUG, "{}Added a tick entry idx= {}", log_prefix_, (prev_log_idx + 1U));
    send_append_entries(/*check_sync_flag=*/false);
    return true;
  }
  return false;
}

unsigned RaftServer::send_append_entries(bool check_sync_flag) {
  return connections_->post(
      [this, self = self(), check_sync_flag](shared_ptr<RaftPeerConnection> conn) {
        if (check_sync_flag && !conn->in_sync())
          return;
        conn->send_append_entries(raft_log(), leadership_term(), commit_idx());
      });
}

unsigned RaftServer::send_request_votes(RaftTerm term) {
  return connections_->post([this, s = self(), term](shared_ptr<RaftPeerConnection> conn) {
    conn->send_request_vote(raft_log(), term);
  });
}

void RaftServer::make_follower(std::source_location loc) {
  if (is_follower())
    return;

  make_follower(unique_lock(), loc);
}

void RaftServer::make_follower(RaftServer::UniqueLock lk, std::source_location loc) {
  (void)lk;

  if (is_follower())
    return;

  log(INFO,
      "{}Becoming a follower @ {}({}:{})",
      log_prefix_,
      loc.function_name(),
      loc.file_name(),
      loc.line());
  update_last_ping(false);

  bool const was_leader = is_leader();
  auto const term       = leadership_term();
  if (was_leader) {
    leadership_term_.store(RaftTerm{0}, std::memory_order_relaxed);
    log(INFO, "{}Sending invalid leader to {} requests", log_prefix_, handlers_.size());
    for (auto& [_, handler, time] : handlers_) {
      handler(make_error_code(ErrorCode::kNotLeader));
      log(INFO, "{}Waiting in the queue for {} milliseconds", log_prefix_, epoch_msec() - time);
    }
    handlers_.clear();
  }
  type_.store(NodeType::kFollower);
  if (was_leader)
    observer_->stepped_down(term);

  stepdown_.clear();
}

void RaftServer::log_untriggered_election(unsigned now) const {
  // Having heard from the leader of the current term, this node decides on its own that the leader
  // is gone (a stepdown starts an election through RequestElection instead).
  if (auto term = leader_msg_term_.load(std::memory_order_relaxed);
      term != RaftTerm{0} && term == current_term())
    log(ERROR,
        "{}Untriggered election: no message from the leader of term={} for {} ms",
        log_prefix_,
        current_term(),
        now - leader_msg_epoch_.load(std::memory_order_relaxed));
}

void RaftServer::make_candidate(unsigned inc) {
  if (!is_follower())
    return;

  make_candidate(inc, unique_lock());
}

void RaftServer::make_candidate(unsigned inc, RaftServer::UniqueLock lk) {
  if (!is_follower())
    return;

  if (state_.needs_backup) {
    // Nothing to promote: wait for a leader instead (update_last_ping restarts the timeout).
    log(INFO, "{}Not starting an election: the node needs a backup", log_prefix_);
    update_last_ping(false);
    return;
  }

  RaftTerm new_term = current_term() + inc;

  log(INFO, "{}New candidate with term={}", log_prefix_, new_term);

  if (new_term == std::get<RaftTerm>(set_current_term(new_term)) && vote(new_term, self_id_)) {
    type_.store(NodeType::kCandidate);
    votes_.clear(self_id_, new_term);
    leadership_term_.store(RaftTerm{0}, std::memory_order_relaxed);

    if (send_request_votes(new_term) != (nodes() - 1U)) {
      connections_->reconnect(
          self<RaftServer>(), config(), [this, s = self()](shared_ptr<RaftPeerConnection> conn) {
            on_connect(conn);
          });
    }
  } else {
    make_follower(std::move(lk));
  }
}

void RaftServer::make_leader(RaftServer::UniqueLock lk) {
  (void)lk;

  // The caller has found that it is a candidate, with the lock that it holds.
  assert(is_candidate());

  if (leadership_term() != RaftTerm{0})
    return;

  stepdown_.clear();
  election_request_.clear();

  auto last_entry = value(raft_log().last_entry());
  log(INFO,
      "{}Becoming a leader term={} with last_entry = ({}, {})",
      log_prefix_,
      current_term(),
      std::get<RaftTerm>(last_entry),
      std::get<RaftIndex>(last_entry));

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
  assert(new_term >= curr_term);

  while (new_term > curr_term && !current_term_.compare_exchange_strong(/* &expected = */ curr_term,
                                                                        /*desired=*/new_term))
    ;

  return {current_term(), false};
}

RaftTerm RaftServer::update_current_term(RaftTerm term) {
  auto [curr_term, unchanged] = set_current_term(term);

  if (unchanged) [[likely]]
    return curr_term;

  log(INFO, "{}Term = {}", log_prefix_, curr_term);

  // A leader that has not been asked to step down learns of a higher term only when another node
  // started an election on its own (it missed our pings).
  if (is_leader() && !stepdown_.test())
    log(ERROR,
        "{}Untriggered election: leader of term={} got a message with term={}",
        log_prefix_,
        leadership_term(),
        curr_term);

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
  if (has_votes() && (!can_vote_.test_and_set())) {
    log(INFO, "{}Now can vote for term greater than {}", log_prefix_, current_term);
    vote(current_term, self_id_);
  }
}

RaftResponse RaftServer::handle_peer_request(RaftNodeId  candidate,
                                             RaftTerm    term,
                                             RequestVote request) {
  RaftTerm const current_term = update_current_term(term);
  if (term != current_term || !can_vote()) {
    log(INFO,
        "{}Rejected terms: {} vs {} from node={}",
        log_prefix_,
        term,
        current_term,
        candidate);
    return RaftResponse{
        .term     = current_term,
        .response = RequestVoteResponse{.vote_granted = false},
    };
  }

  auto last_entry = value(raft_log().last_entry());
  if (last_entry > request.last_entry) {
    log(INFO,
        "{}Rejected because log is not upto date ({},{}) vs ({},{}) from node={}",
        log_prefix_,
        std::get<RaftTerm>(last_entry),
        std::get<RaftIndex>(last_entry),
        std::get<RaftTerm>(request.last_entry),
        std::get<RaftIndex>(request.last_entry),
        candidate);

    return RaftResponse{
        .term     = current_term,
        .response = RequestVoteResponse{.vote_granted = false},
    };
  }

  bool v = vote(current_term, candidate);
  if (v) {
    update_last_ping(true);
    log(INFO, "{}Voted for term={} from node={}", log_prefix_, current_term, candidate);
  } else {
    log(INFO,
        "{}Rejected because already voted for term={} from node={}",
        log_prefix_,
        current_term,
        candidate);
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
  if (has_lost_election())
    return make_follower(std::move(lk));
}

RaftResponse RaftServer::handle_peer_request(RaftNodeId /* leader */,
                                             RaftTerm                           term,
                                             InstallSnapshot                    request,
                                             inplace_vector<MemBlock, 2> const& blocks,
                                             Bytes const&                       bytes) {
  auto current_term = update_current_term(term);

  auto lk = unique_lock();

  if (current_term > term || is_candidate() || is_leader() || request.entries.empty())
    return RaftResponse{
        .term = current_term,
        .response =
            InstallSnapshotResponse{
                .idx     = RaftIndex{0},
                .success = false,
            },
    };

  update_last_ping(true);

  // The segments of the latest entries, the latest timeline history file, then the entries: the
  // log starts after request.log_idx and the WAL before the segments is missing until a
  // PostgreSQL backup restores it.
  write_blocks(blocks, bytes);
  if (request.timeline)
    value(write_history_file(wal_dir_, u4(request.timeline->timeline), request.timeline->content));

  // The leader's state at request.log_idx: the follower's state before it applies the entries,
  // and its boundary state.
  auto boundary      = request.state;
  boundary.log_idx   = request.log_idx;
  auto state         = boundary;
  state.needs_backup = true;
  state.first_idx    = request.log_idx + 1U;
  if (auto pos = std::ranges::find_if(request.entries,
                                      [](Entry const& e) { return e.cmd.segment.has_value(); });
      pos != request.entries.end())
    state.first_segment = *pos->cmd.segment;
  value(log_.install_snapshot(request.log_idx, request.entries, state, boundary));

  state_ = state;
  publish_state();
  {
    auto lk             = std::unique_lock{published_mtx_};
    published_boundary_ = boundary;
  }
  {
    auto lk  = std::unique_lock{gc_mtx_};
    gc_wake_ = true;
  }
  gc_cv_.notify_all();
  applied_idx_.store(state_.log_idx);
  commit_idx_.store(state_.log_idx);
  compaction_idx_.store(state_.log_idx);
  observer_->installed(state_);

  update_last_ping(true);

  log(INFO, "{}Installed snapshot {}", log_prefix_, state_);

  return RaftResponse{
      .term = current_term,
      .response =
          InstallSnapshotResponse{
              .idx     = std::get<RaftIndex>(value(raft_log().last_entry())),
              .success = true,
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
      conn->send_append_entries(std::move(lk), raft_log(), leadership_term(), commit_idx());
    }
  }
}

RaftResponse RaftServer::handle_peer_request(RaftNodeId /*leader*/,
                                             RaftTerm                           term,
                                             AppendEntries                      request,
                                             inplace_vector<MemBlock, 2> const& blocks,
                                             Bytes const&                       bytes) {
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

  leader_msg_epoch_.store(epoch_msec(), std::memory_order_relaxed);
  leader_msg_term_.store(current_term, std::memory_order_relaxed);

  auto lk = unique_lock();

  bool success = request.prev_log_idx >= commit_idx() && append_entries(request.prev_log_idx,
                                                                        request.prev_log_term,
                                                                        request.entries,
                                                                        /*delete_entries=*/true);
  if (success) [[likely]] {
    // The WAL bytes of the entries, once they are accepted: a request of a leader that has been
    // deposed does not get here.
    write_blocks(blocks, bytes);

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
  auto [_, last_idx] = value(raft_log().last_entry());

  auto log_idx = std::min(last_idx, request.prev_log_idx);
  auto log_term =
      log_idx > commit_idx() ? value(raft_log().get_term(log_idx)) : std::optional<RaftTerm>{};

  auto n = count(log_idx, log_term.value_or(RaftTerm{0}));
  assert(log_term.has_value() == (n > 0));

  log(INFO,
      "{}prev_log_idx = {} prev_log_term = {} log_idx = {} log_term = {} n = {}",
      log_prefix_,
      request.prev_log_idx,
      request.prev_log_term,
      log_idx,
      log_term,
      n);

  return RaftResponse{
      .term = current_term,
      .response =
          AppendEntriesResponse{
              .success = false,
              .idx     = log_idx,
              .term    = log_term,  // Can be std::nullopt.
              .n       = n,
          },
  };
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
      // The type changes under the lock: a vote that comes now finds a candidate and makes it a
      // leader with the lock held, and the state machine is changed only with it.
      auto lk         = unique_lock();
      auto last_entry = value(raft_log().last_entry());

      if (!is_candidate() || leadership_term() != req_term) {
        // A follower now, or a candidate of a newer term.
      } else if (commit_idx() == std::get<RaftIndex>(last_entry)) {
        log(INFO,
            "{}Full leadership term={} with last_entry = ({}, {})",
            log_prefix_,
            req_term,
            std::get<RaftTerm>(last_entry),
            std::get<RaftIndex>(last_entry));

        type_.store(NodeType::kLeader);
        observer_->became_leader(req_term);
      } else {
        log(INFO,
            "{}Waiting to become a leader .. {} vs {}",
            log_prefix_,
            commit_idx(),
            std::get<RaftIndex>(last_entry));
      }
    }

    if (stepping_down(/*acquire_lock=*/true) &&
        response.idx == std::get<RaftIndex>(value(raft_log().last_entry()))) {
      // The node is a leader and the logs are synced, so send the election request to the first
      // candidate.
      if (is_leader() && should_send_election_request())
        conn->send_request_election(raft_log(), leadership_term());
    }

  } else if (auto lk = conn->should_send_append_entries(); lk) {
    conn->set_not_in_sync();
    if (!response.term) {
      // The follower's log ends at response.idx, within what it has committed: those entries match
      // ours (log matching), so it continues right after them, if we still have that entry. Only
      // when we compacted it away does it need a snapshot.
      if (value(raft_log().get_term(response.idx))) {
        conn->set_next_idx(response.idx + 1U, lk);
        return conn->send_append_entries(
            std::move(lk), raft_log(), leadership_term(), commit_idx());
      }
      return conn->send_install_snapshot(std::move(lk), raft_log(), leadership_term());
    }

    if (!response.n)
      return;

    uint64_t  nth = std::min<uint64_t>(response.n.value(), 64);
    RaftIndex idx = response.idx;

    while (true) {
      std::optional<RaftTerm> log_term = value(raft_log().get_term(idx));

      // We got a log match.
      if (log_term && response.term.value() == log_term.value()) {
        conn->set_next_idx(idx + 1U, lk);
        return conn->send_append_entries(
            std::move(lk), raft_log(), leadership_term(), commit_idx());
      }

      // Entry doesn't exist in the raft_log, so send a snapshot.
      if (!log_term)
        return conn->send_install_snapshot(std::move(lk), raft_log(), leadership_term());

      if (nth == 0 || idx == RaftIndex{1}) {
        // Probe from idx next. Not below 1: the (0, 0) sentinel at index 0 is in every log, so
        // prev_log_idx = 0 always matches (next_idx = 0 would make prev_log_idx underflow).
        conn->set_next_idx(std::max(idx, RaftIndex{1}), lk);
        return conn->send_append_entries(
            std::move(lk), raft_log(), leadership_term(), commit_idx());
      }

      --idx;
      --nth;
    }
  } else {
    conn->set_not_in_sync();
  }
}

RaftResponse RaftServer::handle_peer_request(RaftNodeId /*leader*/,
                                             RaftTerm        term,
                                             RequestElection request) {
  RaftTerm const curr_term = update_current_term(term);

  if (curr_term == term && value(raft_log().last_entry()) == request.last_entry &&
      !applied_state().needs_backup) {
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
    apply_entries(true);
  }
}

RaftIndex RaftServer::apply_entries(bool check_delay) {
  auto lk   = unique_lock();
  auto idx  = state_.log_idx;
  auto cidx = commit_idx();
  while (cidx > idx) {
    ++idx;

    Entry e = entry(idx);
    state_.apply(idx, e);
    publish_state();

    // A new timeline (a promotion on the primary): its history file goes next to the segments,
    // where the WalSender serves it (TIMELINE_HISTORY) and learns the timeline's switch point.
    if (auto const* t = std::get_if<TimelineOp>(&e.cmd.command))
      value(write_history_file(wal_dir_, u4(t->timeline), t->content));

    applied_idx_.store(idx, std::memory_order_relaxed);
    observer_->applied(idx, e, state_);

    // The leader has a handler for every entry that exec() adds, added with the entry under the
    // lock that this function has; ticks and the entries of earlier leaders have none.
    while (!handlers_.empty() && std::get<RaftIndex>(handlers_.front()) <= idx) {
      auto& [h_idx, handler, time] = handlers_.front();
      assert(h_idx == idx);

      if (check_delay && epoch_msec() - time > 50U)
        log(INFO,
            "{}Time in the queue {} milliseconds, commit_idx={}",
            log_prefix_,
            epoch_msec() - time,
            cidx);

      handler(h_idx);
      handlers_.pop_front();
    }
    cidx = commit_idx();
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

  log(INFO, "{}Stepping down", log_prefix_);

  // Starting a step down.
  if (!stepdown_.test_and_set())
    stepdown_epoch_.store(epoch_msec(), std::memory_order_relaxed);
  election_request_.clear();

  /// Send a last tick.
  tick(/*force=*/true);
}

void RaftServer::compact(RaftIndex idx, UniqueLock lk) {
  // Another thread may hold the lock, or compact, before it is held. A compaction that is not made
  // is not recorded: the next periodic() makes it.
  if (!lk || compaction_idx() + options_.compaction_lag >= idx || state_.log_idx < idx)
    return;

  compact_log();
  compaction_idx_.store(idx);
}

void RaftServer::set_wal_keep_from(LogSeqNumber keep_from) {
  auto lk = unique_lock();
  if (keep_from_ && !(*keep_from_ < keep_from))
    return;
  log(INFO, "{}Keeping the entries from segment {} on", log_prefix_, keep_from);
  keep_from_ = keep_from;
  compact_log();
}

void RaftServer::compact_log() {
  RaftIndex const first        = std::get<RaftIndex>(value(raft_log().first_entry()));
  RaftIndex const last_applied = state_.log_idx;
  if (last_applied == RaftIndex{0})
    return;

  // The new first entry. With a keep_from (the latest backup's segment): the first entry whose
  // segment is at or after it; otherwise keep_old_logs entries. Never past the last applied entry,
  // which stays (AppendEntries needs its term).
  RaftIndex target = last_applied;
  if (keep_from_) {
    for (RaftIndex idx = std::max(first, RaftIndex{1}); idx < last_applied;) {
      auto batch = value(raft_log().entries_from(idx, 4096));
      if (!batch || batch->empty())
        break;
      bool found = false;
      for (auto const& e : *batch) {
        if (idx >= last_applied)
          break;
        if (e.cmd.segment && !(*e.cmd.segment < *keep_from_)) {
          target = idx;
          found  = true;
          break;
        }
        ++idx;
      }
      if (found)
        break;
    }
  } else {
    if (INT(last_applied) <= options_.keep_old_logs)
      return;
    target = last_applied - options_.keep_old_logs;
  }
  if (target <= first)
    return;

  // The state at target - 1: the previous boundary, with the entries being deleted applied.
  auto            boundary = value(log_.load_boundary());
  RaftIndex const from     = boundary.log_idx + 1U;
  if (from < std::max(first, RaftIndex{1})) {
    log(ERROR,
        "{}No boundary state to compact from: {} vs first entry {}",
        log_prefix_,
        boundary,
        first);
    return;
  }
  for (RaftIndex idx = from; idx < target;) {
    auto batch =
        value(raft_log().entries_from(idx, std::min<size_t>(4096, INT(target) - INT(idx))));
    if (!batch || batch->empty())
      break;
    for (auto const& e : *batch)
      if (idx < target)
        boundary.apply(idx++, e);
  }

  // The segment of the first entry, from target on, that has one.
  auto first_segment = state_.first_segment;
  for (RaftIndex idx = target;;) {
    auto batch = value(raft_log().entries_from(idx, 4096));
    if (!batch || batch->empty())
      break;
    auto pos =
        std::ranges::find_if(*batch, [](Entry const& e) { return e.cmd.segment.has_value(); });
    if (pos != batch->end()) {
      first_segment = *pos->cmd.segment;
      break;
    }
    idx = idx + batch->size();
  }

  log(INFO,
      "{}Compacting: entries before {} (first segment {}), applied = {}",
      log_prefix_,
      target,
      first_segment,
      last_applied);
  state_.first_idx     = target;
  state_.first_segment = first_segment;
  value(log_.compact(target, state_, boundary));
  publish_state();
  {
    auto lk             = std::unique_lock{published_mtx_};
    published_boundary_ = boundary;
  }
  {
    auto lk  = std::unique_lock{gc_mtx_};
    gc_wake_ = true;
  }
  gc_cv_.notify_all();
}

void RaftServer::wal_gc() {
  while (true) {
    {
      auto lk = std::unique_lock{gc_mtx_};
      gc_cv_.wait(lk, [this] { return gc_wake_ || gc_stop_; });
      if (gc_stop_)
        return;
      gc_wake_ = false;
    }

    auto const first = applied_state().first_segment;
    if (first.lsn == 0)
      continue;

    std::vector<std::filesystem::path> old;
    std::error_code                    ec;
    for (auto const& entry : std::filesystem::directory_iterator(wal_dir_, ec)) {
      auto seg = log_seq_number(entry.path().filename().string().c_str());
      if (seg && seg->lsn + kWalSegmentSize <= first.lsn)
        old.push_back(entry.path());
    }
    for (auto const& p : old) {
      std::filesystem::remove(p, ec);
      log(INFO,
          "{}Deleted the WAL segment {}: before the first entry's {}",
          log_prefix_,
          p.filename(),
          first);
    }
  }
}

void RaftServer::check_stepdown_timeout() {
  // On shutdown the server is closed after the leadership is handed over or a wait.
  if (!stepping_down(/*acquire_lock=*/false) || !is_leader() || shutdown_signal())
    return;

  auto start = stepdown_epoch_.load(std::memory_order_relaxed);
  if (epoch_msec() <= start + (kStepdownTimeouts * options_.election_timeout_msecs))
    return;

  auto lk = unique_lock();
  if (!is_leader() || !stepdown_.test())
    return;

  // No peer took over: rather than refusing the clients, the server remains the leader.
  log(ERROR,
      "{}Stepdown started @ {} did not hand over the leadership, calling it off",
      log_prefix_,
      start);
  stepdown_.clear();
  election_request_.clear();
}

void RaftServer::on_shutdown() {
  log(ERROR, "{}on_shutdown", log_prefix_);
  if (auto lk = timer_lock(); lk)
    timer_.cancel();

  if (closed_.test())
    return;

  // A server that has been closed has no log to hand over.
  if (is_leader()) {
    stepdown();

    auto idx = 0;
    while (is_leader() && ++idx < 5 && (connections_->connected_peers() > (nodes() >> 2U))) {
      log(INFO, "{}Waiting to complete leadership transfer ...", log_prefix_);
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  close();
}

unsigned RaftServer::count(RaftIndex idx, RaftTerm term) const {
  unsigned result = 0;
  while (idx > commit_idx()) {
    if (auto t = value(raft_log().get_term(idx)); t && *t == term) {
      ++result;
      --idx;
    } else {
      break;
    }
  }
  return result;
}

void RaftServer::system_error(std::error_code ec) const {
  log(ERROR, "{}Exiting ... {}", log_prefix_, ec.message());
  std::abort();
}

void RaftServer::close() {
  if (closed_.test_and_set())
    return;

  log(ERROR, "{}close", log_prefix_);

  acceptor_cancel();

  if (auto lk = timer_lock(); lk)
    timer_.cancel();

  if (connections_)
    connections_->close();

  if (auto lk = unique_lock(); lk) {
    for (auto& [_, handler, time] : handlers_)
      handler(make_error_code(ErrorCode::kNotLeader));
    handlers_.clear();
  }

  {
    auto lk  = std::unique_lock{gc_mtx_};
    gc_stop_ = true;
  }
  gc_cv_.notify_all();
  if (gc_thread_.joinable() && gc_thread_.get_id() != std::this_thread::get_id())
    gc_thread_.join();

  auto lk = std::unique_lock<std::shared_mutex>{usage_lock_};
  log_.close();
}
