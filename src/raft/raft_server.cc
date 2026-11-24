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
      term_file_(dir_ / "term", options.no_sync),
      ledger_(dir_ / "ledger",
              LedgerOptions{
                  .file_size   = options.ledger_file_size,
                  .sparse_step = LedgerOptions{}.sparse_step,
                  .sync        = options.leader_sync,
                  .no_sync     = options.no_sync,
                  .spare       = true,
              }),
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
  wal_gc_floor_.store(options_.wal_gc_floor);  // before the WAL deletion thread starts
  std::filesystem::create_directories(dir_);
  current_term_ = value(term_file_.open()).term;

  auto meta = value(read_meta_file(meta_path()));
  ledger_.set_durable_callback([this](RaftIndex idx) { on_durable(idx); });
  value(ledger_.open(meta.state.first_idx, meta.installing));

  state_            = meta.state;
  checkpoint_state_ = meta.state;
  if (meta.installing)
    save_meta();  // the install is finished
  write_history(state_);
  publish_state();
  gc_thread_         = std::thread([this] { wal_gc(); });
  checkpoint_thread_ = std::thread([this] { checkpoint_loop(); });
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
  // The wait of a lagging node for a leader (start_pre_vote()) counts from now.
  leader_msg_epoch_.store(epoch_msec(), std::memory_order_relaxed);

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

        start_pre_vote();
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
        self_id_, std::move(peer), std::move(socket), self<RaftServer>(), options_.reader_threads);
  }
  return Connection::create<RaftClientConnection>(
      self<RaftServer>(), std::move(peer), std::move(socket));
}

void RaftServer::on_connect(shared_ptr<RaftPeerConnection> const& conn) {
  if (is_leader()) {
    log(INFO, "{}Connected to node={}, send_append_entries", log_prefix_, conn->peer_id());
    conn->send_append_entries(ledger_, leadership_term(), commit_idx());
  } else if (is_candidate()) {
    if (has_votes()) {
      log(INFO,
          "{}Connected to node={}, send_append_entries (have enough votes as a candidate)",
          log_prefix_,
          conn->peer_id());
      conn->send_append_entries(ledger_, leadership_term(), commit_idx());
    } else {
      auto term = votes_.term();
      log(INFO,
          "{}Connected to node={}, send_request_vote for term={}",
          log_prefix_,
          conn->peer_id(),
          term);
      conn->send_request_vote(ledger_, term);
    }
  } else if (!can_vote()) {
    log(INFO, "{}Connected to node={}, send_ping", log_prefix_, conn->peer_id());
    conn->send_ping(current_term());
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

void RaftServer::exec(WalCommand cmd, std::span<uint8_t const> wal, ExecHandler handler) {
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

    // The leadership's term, not current_term(): a leader that has just heard of a newer term
    // (update_current_term() sets it before make_follower() gets this lock) still appends as the
    // leader of its own term. An entry labelled with the newer term would share (index, term) with
    // a different entry of that term's leader, breaking log matching.
    auto const idx = append(
        Entry{
            .time = last_entry_time_,
            .term = leadership_term(),
            .cmd  = std::move(cmd),
        },
        wal);
    handlers_.emplace_back(idx, std::move(handler), epoch_msec());
  }

  send_append_entries(/*check_sync_flag=*/true, /*here=*/true);
}

RaftIndex RaftServer::append(Entry const& e, std::span<uint8_t const> wal) {
  // The record gets its own copy of the bytes (its crc is of that copy): PostgreSQL writing on
  // meanwhile doesn't matter.
  return value(ledger_.append(e, wal));
}

void RaftServer::on_durable(RaftIndex idx) {
  // Not usage_lock(): close() holds it exclusively while it stops the ledger's sync thread.
  auto lk = std::shared_lock<std::shared_mutex>{usage_lock_, std::try_to_lock};
  if (!lk || closed_.test())
    return;
  if (is_leader() || is_candidate())
    update_match_index(self_id_, idx);
}

void RaftServer::set_needs_backup(bool needs_backup) {
  auto lk = unique_lock();
  if (state_.needs_backup == needs_backup)
    return;

  log(INFO, "{}needs_backup = {}", log_prefix_, needs_backup);
  state_.needs_backup = needs_backup;
  save_meta();
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
    save_meta();
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

Result<shared_ptr<WalSegment>> RaftServer::segment(LogSeqNumber const& lsn) const {
  {
    auto lk = std::unique_lock{open_segments_mtx_};
    if (auto pos = open_segments_.find(lsn); pos != open_segments_.end())
      return pos->second;
  }

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

  // Keep it open; the oldest goes when there are too many (the WAL moves forward).
  auto lk = std::unique_lock{open_segments_mtx_};
  open_segments_.try_emplace(lsn, seg);
  while (open_segments_.size() > kOpenSegments)
    open_segments_.erase(open_segments_.begin());
  return seg;
}

Result<shared_ptr<RaftServer::WalFile>> RaftServer::wal_file(LogSeqNumber const& lsn) {
  // With wal_files_mtx_ held.
  if (auto pos = wal_files_.find(lsn); pos != wal_files_.end())
    return pos->second;

  // As segment(): X, or .X (renamed by an unlink, WalFuseOps), or a new X, zero filled.
  auto const name = filename(lsn);
  auto       path = wal_dir_ / name;
  if (!std::filesystem::exists(path) && std::filesystem::exists(wal_dir_ / ("." + name)))
    path = wal_dir_ / ("." + name);
  auto f = make_shared<WalFile>();
  f->fd  = ::open(path.c_str(), O_RDWR | O_CREAT, file_mode());
  if (f->fd == -1)
    return elog(system_error_code(errno), "{}Unable to open {}", log_prefix_, path);
  struct stat st {};
  if (::fstat(f->fd, &st) != 0)
    return elog(system_error_code(errno), "{}Unable to stat {}", log_prefix_, path);
  if (st.st_size < kWalSegmentSize && ::ftruncate(f->fd, kWalSegmentSize) != 0)
    return elog(system_error_code(errno), "{}Unable to size {}", log_prefix_, path);

  // Keep the last few open (the WAL moves forward); the dirty ones stay open in dirty_.
  wal_files_.emplace(lsn, f);
  while (wal_files_.size() > kOpenSegments)
    wal_files_.erase(wal_files_.begin());
  return f;
}

void RaftServer::write_wal(Entry const& e, std::span<uint8_t const> wal) {
  auto const bytes = wal_bytes(e.cmd);
  if (!bytes)
    return;
  auto const lsn = *e.cmd.segment;

  auto lk = std::unique_lock{wal_files_mtx_};
  auto f  = value(wal_file(lsn));
  for (size_t done = 0; done < bytes->second;) {
    auto n = ::pwrite(f->fd,
                      wal.data() + done,
                      bytes->second - done,
                      bytes->first + static_cast<off_t>(done));
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      system_error(elog(system_error_code(n < 0 ? errno : EIO),
                        "{}Unable to write the WAL of {}",
                        log_prefix_,
                        lsn));
    done += static_cast<size_t>(n);
  }
  dirty_.try_emplace(lsn, f);
}

void RaftServer::checkpoint() {
  // The state applied, and the segments written before it.
  RaftState                              state;
  uint64_t                               gen = 0;
  map<LogSeqNumber, shared_ptr<WalFile>> dirty;
  {
    auto lk = unique_lock();
    state   = state_;
    gen     = install_gen_;
    auto wl = std::unique_lock{wal_files_mtx_};
    dirty.swap(dirty_);
  }

  // Synced without the server lock: applying goes on meanwhile (into the next checkpoint).
  if (!options_.no_sync)
    for (auto const& [lsn, f] : dirty) {
#ifdef __APPLE__
      int rc = ::fsync(f->fd);
#else
      int rc = ::fdatasync(f->fd);
#endif
      if (rc != 0)
        system_error(elog(system_error_code(errno), "{}Unable to sync the segment {}", log_prefix_, lsn));
    }

  auto lk = unique_lock();
  if (gen != install_gen_ || state.log_idx <= checkpoint_state_.log_idx)
    return;  // an install replaced the log meanwhile, or nothing new
  checkpoint_state_ = state;
  save_meta();
  // Compaction (to the latest backup's segment) never goes past the last checkpoint: it may go
  // further now. Without a keep_from (the raft tests) it waits for compaction_lag, as before.
  if (keep_from_)
    compact_log();
}

void RaftServer::checkpoint_loop() {
  for (;;) {
    {
      auto lk = std::unique_lock{checkpoint_mtx_};
      checkpoint_cv_.wait_for(lk, std::chrono::seconds(options_.checkpoint_secs), [this] {
        return checkpoint_stop_;
      });
      if (checkpoint_stop_)
        return;
    }
    checkpoint();
  }
}

bool RaftServer::tick(bool force) {
  if (!is_leader())
    return false;

  if (!force && std::time(nullptr) < (last_entry_time_.load(std::memory_order_relaxed) +
                                      static_cast<std::time_t>(options_.tick_seconds)))
    return false;

  last_entry_time_.store(std::time(nullptr), std::memory_order_relaxed);

  // A tick entry (the callers hold the lock), of the leadership's term (see exec()).
  auto const idx = append(
      Entry{
          .time = last_entry_time_,
          .term = leadership_term(),
          .cmd  = {},
      },
      {});
  if (force)
    log(INFO, "{}Added the entry {}", log_prefix_, idx);
  else
    log(DEBUG, "{}Added a tick entry idx= {}", log_prefix_, idx);
  send_append_entries(/*check_sync_flag=*/false);
  return true;
}

unsigned RaftServer::send_append_entries(bool check_sync_flag, bool here) {
  auto send = [this, self = self(), check_sync_flag](shared_ptr<RaftPeerConnection> conn) {
    if (check_sync_flag && !conn->in_sync())
      return;
    conn->send_append_entries(ledger_, leadership_term(), commit_idx());
  };
  return here ? connections_->run(send) : connections_->post(send);
}

unsigned RaftServer::send_request_votes(RaftTerm term) {
  return connections_->post([this, s = self(), term](shared_ptr<RaftPeerConnection> conn) {
    conn->send_request_vote(ledger_, term);
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
  if (was_leader) {
    // A deposed leader's wait for the next leader (start_pre_vote()) counts from now.
    leader_msg_epoch_.store(epoch_msec(), std::memory_order_relaxed);
    observer_->stepped_down(term);
  }

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

  auto last_entry = ledger_.last_entry();
  log(INFO,
      "{}Becoming a leader term={} with last_entry = ({}, {})",
      log_prefix_,
      current_term(),
      std::get<RaftTerm>(last_entry),
      std::get<RaftIndex>(last_entry));

  leadership_term_.store(votes_.term());

  // An entry of this term at once (Raft §5.4.2): the leader commits only entries of its own term
  // by counting replicas (update_match_index), and the entries of earlier terms with them. Without
  // it, an earlier term's entry counted onto a majority could commit (be applied, its WAL streamed
  // to the standbys) and still be overwritten by a candidate whose last entry has a newer term.
  last_entry_time_ = std::time(nullptr);
  auto const idx   = append(
      Entry{
          .time = last_entry_time_,
          .term = leadership_term(),
          .cmd  = {},
      },
      {});
  log(INFO, "{}Added the entry {} of term {}", log_prefix_, idx, leadership_term());

  match_indexes_.reset();
  match_indexes_.match_index(self_id_, ledger_.durable_idx());

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
  RaftTerm new_term = value(term_file_.set_current_term(term));
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

  auto last_entry = ledger_.last_entry();
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
                                             RaftTerm        term,
                                             InstallSnapshot request,
                                             Bytes const&    bytes) {
  auto current_term = update_current_term(term);

  auto lk = unique_lock();

  auto const failed = RaftResponse{
      .term = current_term,
      .response =
          InstallSnapshotResponse{
              .idx     = RaftIndex{0},
              .success = false,
          },
  };
  if (current_term > term || is_candidate() || is_leader())
    return failed;

  update_last_ping(true);
  leader_msg_epoch_.store(epoch_msec(), std::memory_order_relaxed);
  leader_msg_term_.store(current_term, std::memory_order_relaxed);

  // A chunk of the leader's first ledger file, into the install file.
  if (auto r = ledger_.install_chunk(request.offset, bytes); !r)
    return failed;
  if (request.offset + bytes.size() < request.size)
    return RaftResponse{
        .term     = current_term,
        .response = InstallSnapshotResponse{.idx = RaftIndex{0}, .success = true, .done = false},
    };

  // The last one: the file is checked whole, then replaces the ledger.
  auto info = ledger_.prepare_install();
  if (!info || info->header.first != request.first)
    return failed;
  install(*info);

  return RaftResponse{
      .term = current_term,
      .response =
          InstallSnapshotResponse{
              .idx     = std::get<RaftIndex>(ledger_.last_entry()),
              .success = true,
              .done    = true,
          },
  };
}

void RaftServer::install(InstallInfo const& info) {
  auto const first = info.header.first;

  // The state at the entry before the file's first record (its header); the WAL before its
  // records is missing until a PostgreSQL backup restores it.
  auto state          = info.header.state;
  state.needs_backup  = true;
  state.first_idx     = first;
  state.first_segment = info.first_segment.value_or(state.first_segment);

  // The meta file names the install, the install file replaces the ledger (a crash in between:
  // Ledger::open finishes it), and the meta file no longer names it. The records are applied as
  // they commit, which writes their WAL into wal_dir; nothing of them is there before.
  state_            = state;
  checkpoint_state_ = state;
  ++install_gen_;
  {
    auto wl = std::unique_lock{wal_files_mtx_};
    dirty_.clear();
  }
  save_meta(first);
  {
    // finish_install deletes every ledger file: a compaction's unlinks must not come after it.
    auto lk = std::unique_lock{unlink_mtx_};
    drop_epoch_.fetch_add(1);
    value(ledger_.finish_install(first));
  }
  save_meta();
  write_history(state_);  // the timeline the file's records continue on, from its header

  publish_state();
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
  log(INFO, "{}Installed {} … {}: {}", log_prefix_, first, info.last, state_);
}

void RaftServer::handle_response(shared_ptr<RaftPeerConnection> conn,
                                 RaftTerm                       req_term,
                                 RaftTerm                       peer_term,
                                 InstallSnapshotResponse        response) {
  // If the term has changed ignore the response.
  if (req_term != update_current_term(peer_term))
    return;

  auto lk = conn->should_send_append_entries();
  if (!lk)
    return;
  if (!response.success)
    return conn->end_snapshot(lk);  // the next one starts over
  if (!response.done)
    return conn->send_snapshot_chunk(std::move(lk), ledger_, leadership_term());

  conn->end_snapshot(lk);
  conn->set_next_idx(response.idx + 1U, lk);
  lk.unlock();
  update_match_index(conn->peer_id(), response.idx);
  if (auto lk2 = conn->should_send_append_entries(); lk2)
    conn->send_append_entries(std::move(lk2), ledger_, leadership_term(), commit_idx());
}

RaftResponse RaftServer::handle_peer_request(RaftNodeId /*leader*/,
                                             RaftTerm      term,
                                             AppendEntries request,
                                             Bytes const&  bytes) {
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

  // The leader's records, checked (crc, indexes, terms, chain) before anything is written.
  auto decoded = decode_records(bytes);
  if (!decoded || decoded->size() != request.n)
    log(ERROR,
        "{}Invalid records after {}: {} bytes, {} records expected",
        log_prefix_,
        request.prev_log_idx,
        bytes.size(),
        request.n);

  std::optional<Appended> appended;
  if (decoded && decoded->size() == request.n && request.prev_log_idx >= commit_idx())
    appended = value(ledger_.append_records(
        request.prev_log_idx, request.prev_log_term, *decoded, options_.follower_sync));

  if (appended) [[likely]] {
    // Their WAL goes into wal_dir when they are applied (committed). With follower_sync the
    // records were written with RWF_DSYNC; a full sync only when that didn't cover everything up
    // to the answer (records this ledger already had, e.g. from its time as the leader).
    if (options_.follower_sync && ledger_.synced_idx() < appended->last)
      value(ledger_.sync());

    update_last_ping(false);
    set_commit_idx(std::min(request.commit_idx, appended->last));

    return RaftResponse{
        .term = current_term,
        .response =
            AppendEntriesResponse{
                .success = true,
                .idx     = appended->last,
            },
    };
  }
  update_last_ping(true);

  // Logs don't match.
  auto [_, last_idx] = ledger_.last_entry();

  auto log_idx = std::min(last_idx, request.prev_log_idx);
  auto log_term = log_idx > commit_idx() ? ledger_.term(log_idx) : std::optional<RaftTerm>{};

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
      auto last_entry = ledger_.last_entry();

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
        response.idx == std::get<RaftIndex>(ledger_.last_entry())) {
      // The node is a leader and the logs are synced, so send the election request to the first
      // candidate.
      if (is_leader() && should_send_election_request())
        conn->send_request_election(ledger_, leadership_term());
    }

  } else if (auto lk = conn->should_send_append_entries(); lk) {
    conn->set_not_in_sync();
    if (!response.term) {
      // The follower's log ends at response.idx, within what it has committed: those entries match
      // ours (log matching), so it continues right after them, if we still have that entry. Only
      // when we compacted it away does it need a snapshot.
      if (ledger_.term(response.idx)) {
        conn->set_next_idx(response.idx + 1U, lk);
        return conn->send_append_entries(
            std::move(lk), ledger_, leadership_term(), commit_idx());
      }
      return conn->send_install_snapshot(std::move(lk), ledger_, leadership_term());
    }

    if (!response.n)
      return;

    uint64_t  nth = std::min<uint64_t>(response.n.value(), 64);
    RaftIndex idx = response.idx;

    while (true) {
      std::optional<RaftTerm> log_term = ledger_.term(idx);

      // We got a log match.
      if (log_term && response.term.value() == log_term.value()) {
        conn->set_next_idx(idx + 1U, lk);
        return conn->send_append_entries(
            std::move(lk), ledger_, leadership_term(), commit_idx());
      }

      // The entry was compacted away: send a snapshot.
      if (!log_term)
        return conn->send_install_snapshot(std::move(lk), ledger_, leadership_term());

      if (nth == 0 || idx == RaftIndex{1}) {
        // Probe from idx next. Not below 1: the (0, 0) sentinel at index 0 is in every log, so
        // prev_log_idx = 0 always matches (next_idx = 0 would make prev_log_idx underflow).
        conn->set_next_idx(std::max(idx, RaftIndex{1}), lk);
        return conn->send_append_entries(
            std::move(lk), ledger_, leadership_term(), commit_idx());
      }

      --idx;
      --nth;
    }
  } else {
    conn->set_not_in_sync();
  }
}

bool RaftServer::hears_leader() const {
  auto const term = leader_msg_term_.load(std::memory_order_relaxed);
  return term != RaftTerm{0} && term == current_term() &&
         epoch_msec() - leader_msg_epoch_.load(std::memory_order_relaxed) <
             options_.election_timeout_msecs;
}

void RaftServer::start_pre_vote() {
  // A node that needs a backup starts no election (make_candidate() says so and waits).
  if (applied_state().needs_backup || nodes() == 1U)
    return make_candidate();

  // A standby far behind (or not running) would win and then not promote in time: leave the
  // election to a node that can serve, unless there has been no leader for a while.
  if (options_.max_standby_lag != 0) {
    auto const replayed = standby_applied_lsn();
    auto const wal_end  = applied_state().wal_end.lsn;
    auto const leaderless =
        epoch_msec() - leader_msg_epoch_.load(std::memory_order_relaxed);
    if ((!replayed || *replayed + options_.max_standby_lag < wal_end) &&
        leaderless < options_.lagging_election_wait_msecs) {
      log(INFO,
          "{}Not starting an election: the standby is at {} of {} (no leader for {} ms)",
          log_prefix_,
          replayed ? std::format("{:X}", *replayed) : string{"(not running)"},
          std::format("{:X}", wal_end),
          leaderless);
      update_last_ping(false);
      return;
    }
  }

  auto const proposed = current_term() + 1U;
  {
    auto lk        = std::unique_lock{pre_vote_mtx_};
    pre_vote_term_ = proposed;
    pre_votes_     = {self_id_};
  }
  // The next round after another election timeout, if this one gets no majority.
  update_last_ping(false);
  log(INFO, "{}PreVote for term={}", log_prefix_, proposed);
  if (connections_->post([this, s = self(), proposed](shared_ptr<RaftPeerConnection> conn) {
        conn->send_request_pre_vote(ledger_, proposed);
      }) != (nodes() - 1U))
    connections_->reconnect(
        self<RaftServer>(), config(), [this, s = self()](shared_ptr<RaftPeerConnection> conn) {
          on_connect(conn);
        });
}

RaftResponse RaftServer::handle_peer_request(RaftNodeId     candidate,
                                             RaftTerm       term,
                                             RequestPreVote request) {
  // Nothing changes here, the term included.
  auto const current = current_term();
  auto const granted = term > current && can_vote() && !is_leader() && !is_candidate() &&
                       !hears_leader() && !(ledger_.last_entry() > request.last_entry);
  log(INFO,
      "{}PreVote for term={} from node={}: {}",
      log_prefix_,
      term,
      candidate,
      granted ? "granted" : "refused");
  return RaftResponse{.term = current, .response = RequestPreVoteResponse{.granted = granted}};
}

void RaftServer::handle_response(shared_ptr<RaftPeerConnection> conn,
                                 RaftTerm                       req_term,
                                 RaftTerm                       peer_term,
                                 RequestPreVoteResponse         response) {
  // A newer term elsewhere: take it (this node is a follower; nothing to depose).
  if (peer_term >= req_term)
    update_current_term(peer_term);
  if (!response.granted || !is_follower() || current_term() + 1U != req_term)
    return;

  {
    auto lk = std::unique_lock{pre_vote_mtx_};
    if (pre_vote_term_ != req_term)
      return;
    pre_votes_.insert(conn->peer_id());
    if (pre_votes_.size() < nodes() / 2U + 1U)
      return;
    pre_vote_term_ = RaftTerm{0};
  }
  log(INFO, "{}PreVote for term={} won", log_prefix_, req_term);
  make_candidate();
}

RaftResponse RaftServer::handle_peer_request(RaftNodeId /*leader*/,
                                             RaftTerm        term,
                                             RequestElection request) {
  RaftTerm const curr_term = update_current_term(term);

  if (curr_term == term && ledger_.last_entry() == request.last_entry &&
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
  // The leader counts itself once its ledger has the entries on disk (at once without syncs).
  match_indexes_.match_index(self_id_, ledger_.durable_idx());
  if (auto n_commit_idx = match_indexes_.commit_index(commit_idx()); n_commit_idx > commit_idx()) {
    // Only an entry of the leader's own term commits by counting (Raft §5.4.2); the earlier
    // entries commit with it. The terms only grow along the log: when the entry at n_commit_idx is
    // of an earlier term, so is every entry before it, and nothing commits yet (make_leader()
    // appended one of this term). Not a leader (leadership term 0): nothing either.
    auto const term = leadership_term();
    if (term == RaftTerm{0} || ledger_.term(n_commit_idx) != term)
      return;
    set_commit_idx(n_commit_idx);
    apply_entries(true);
  }
}

RaftIndex RaftServer::apply_entries(bool check_delay) {
  auto lk   = unique_lock();
  auto idx  = state_.log_idx;
  auto cidx = commit_idx();
  vector<LedgerEntry> batch;
  size_t              next = 0;
  while (cidx > idx) {
    ++idx;

    if (next == batch.size()) {
      auto r = value(ledger_.entries(idx, std::min<size_t>(4096, INT(cidx) - INT(idx) + 1U)));
      if (!r || r->empty() || r->front().idx != idx)
        system_error(system_error_code(EINVAL));  // a committed entry is not in the ledger
      batch = std::move(*r);
      next  = 0;
    }
    Entry const& e = batch.at(next++).entry;
    // The WAL bytes into wal_dir first: the observer advances the WAL sender over them.
    write_wal(e, *batch.at(next - 1).wal);
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
  ledger_.release(idx);  // the entries up to idx no longer need to be kept in memory
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

void RaftServer::write_history(RaftState const& s) {
  for (auto const& [tli, content] : history_files(s)) {
    std::error_code ec;
    if (std::filesystem::exists(wal_dir_ / history_filename(tli), ec))
      continue;
    log(INFO, "{}Writing the history file of timeline {} from the state", log_prefix_, tli);
    value(write_history_file(wal_dir_, tli, content));
  }
}

void RaftServer::compact_log() {
  RaftIndex const first = ledger_.first_idx();
  // Never past the last checkpoint: the entries after it are applied again after a crash, so they
  // stay (docs/ledger.md §9.2).
  RaftIndex const last_applied = std::min(state_.log_idx, checkpoint_state_.log_idx + 1U);
  if (last_applied == RaftIndex{0})
    return;

  // The new first entry. With a keep_from (the latest backup's segment): the start of the last
  // file before which all the WAL ends at or before it (so every entry whose segment is at or
  // after it stays); otherwise keep_old_logs entries. Never past the last applied entry, which
  // stays (AppendEntries needs its term).
  RaftIndex target = last_applied;
  if (keep_from_) {
    // The file headers' wal_end, not a scan of the records: this runs with the server lock held,
    // and reading every record up to keep_from took it for 20 s after a backup moved keep_from on
    // (no heartbeats meanwhile: an election).
    target = ledger_.compaction_point_before_wal(keep_from_->lsn, last_applied);
  } else {
    if (!options_.compact_without_keep_from)
      return;  // until the node sets keep_from
    if (INT(last_applied) <= options_.keep_old_logs)
      return;
    target = last_applied - options_.keep_old_logs;
  }
  // Whole files: the first index of the file that holds the target (its header holds the state
  // before it, the boundary state).
  target = ledger_.compaction_point(target);
  if (target <= first)
    return;

  // The segment of the first entry, from target on, that has one: usually the first. Small
  // batches of records, decoded in place (entries() copied up to 8 MiB of WAL for this).
  auto first_segment = state_.first_segment;
  for (RaftIndex idx = target;;) {
    auto batch = value(ledger_.records(idx, 16, 256U << 10U));
    if (!batch || batch->count() == 0)
      break;
    std::optional<LogSeqNumber> found;
    for (auto const& buf : batch->buffers) {
      for (std::span<uint8_t const> rest{buf}; !found && !rest.empty();) {
        auto rec = value(decode_record(rest));
        found    = rec.entry.cmd.segment;
        rest     = rest.subspan(rec.bytes.size());
      }
      if (found)
        break;
    }
    if (found) {
      first_segment = *found;
      break;
    }
    idx = batch->last + 1U;
  }

  log(INFO,
      "{}Compacting: entries before {} (first segment {}), applied = {}",
      log_prefix_,
      target,
      first_segment,
      last_applied);
  // The ledger drops the files in memory; the WAL deletion thread saves the meta file, then
  // unlinks them (after a crash between the two, Ledger::open drops the files before first_idx).
  state_.first_idx     = target;
  state_.first_segment = first_segment;
  auto files           = value(ledger_.detach_before(target));
  auto meta            = meta_snapshot();
  auto seq             = ++meta_seq_;
  {
    auto       lk    = std::unique_lock{gc_mtx_};
    auto const epoch = drop_epoch_.load();
    if (!drop_)
      drop_.emplace();
    if (drop_->epoch != epoch)
      drop_->files.clear();
    drop_->epoch = epoch;
    drop_->seq   = seq;
    drop_->meta  = std::move(meta);
    drop_->files.insert(drop_->files.end(), files.begin(), files.end());
    gc_wake_ = true;
  }
  // After the job: the WAL deletion thread takes first_segment from a job, else from the
  // published state, which must then be in the meta file.
  publish_state();
  gc_cv_.notify_all();
}

Result<void> RaftServer::write_meta(uint64_t seq, LedgerMeta const& meta) {
  auto lk = std::unique_lock{meta_mtx_};
  if (seq <= meta_written_)
    return ok();
  if (auto r = write_meta_file(meta_path(), meta, options_.no_sync); !r)
    return r;
  meta_written_ = seq;
  return ok();
}

void RaftServer::wal_gc() {
  while (true) {
    std::optional<DropJob> job;
    {
      auto lk = std::unique_lock{gc_mtx_};
      gc_cv_.wait(lk, [this] { return gc_wake_ || gc_stop_; });
      if (gc_stop_)
        return;  // a compaction not finished: the next start's (or the last save's) meta file
      gc_wake_ = false;
      job      = std::exchange(drop_, std::nullopt);
    }

    // The compactions: the meta file, then their ledger files.
    if (job) {
      if (auto r = write_meta(job->seq, job->meta); !r) {
        log(ERROR, "{}Unable to save the meta file of a compaction: retried later", log_prefix_);
        auto lk = std::unique_lock{gc_mtx_};
        if (!drop_)
          drop_ = std::move(job);
        else if (drop_->epoch == job->epoch)  // a later compaction: its meta file, every file
          drop_->files.insert(drop_->files.begin(), job->files.begin(), job->files.end());
        continue;
      }
      auto lk = std::unique_lock{unlink_mtx_};
      if (job->epoch == drop_epoch_.load())
        if (auto r = ledger_.unlink_files(job->files); !r)
          log(ERROR,
              "{}Unable to delete the compacted ledger files (the next start does)",
              log_prefix_);
    }

    // The meta file has the first segment (a compaction's, saved above; otherwise saved with the
    // server lock held before it was published, as an install's, which voids an earlier job's).
    auto first = job && job->epoch == drop_epoch_.load() ? job->meta.state.first_segment
                                                         : applied_state().first_segment;
    if (first.lsn == 0)
      continue;
    // Not past the local PostgreSQL's redo point (set_wal_gc_floor()).
    if (auto const floor = wal_gc_floor_.load(); floor != 0 && floor - floor % kWalSegmentSize < first.lsn)
      first.lsn = floor - floor % kWalSegmentSize;

    std::vector<std::filesystem::path> old;
    std::error_code                    ec;
    for (auto const& entry : std::filesystem::directory_iterator(wal_dir_, ec)) {
      auto seg = log_seq_number(entry.path().filename().string().c_str());
      if (seg && seg->lsn + kWalSegmentSize <= first.lsn)
        old.push_back(entry.path());
    }
    {
      auto lk = std::unique_lock{open_segments_mtx_};
      std::erase_if(open_segments_, [&first](auto const& kv) {
        return kv.first.lsn + kWalSegmentSize <= first.lsn;
      });
    }
    {
      auto lk  = std::unique_lock{wal_files_mtx_};
      auto before = [&first](auto const& kv) {
        return kv.first.lsn + kWalSegmentSize <= first.lsn;
      };
      std::erase_if(wal_files_, before);
      std::erase_if(dirty_, before);
    }
    for (auto const& p : old) {
      std::filesystem::remove(p, ec);
      log(INFO,
          "{}Deleted the WAL segment {}: before {} (the first entry's, or the redo point's)",
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
    if (auto t = ledger_.term(idx); t && *t == term) {
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

  {
    auto lk          = std::unique_lock{checkpoint_mtx_};
    checkpoint_stop_ = true;
  }
  checkpoint_cv_.notify_all();
  if (checkpoint_thread_.joinable() && checkpoint_thread_.get_id() != std::this_thread::get_id())
    checkpoint_thread_.join();
  // A last checkpoint, so that a restart applies little again.
  checkpoint();

  auto lk = std::unique_lock<std::shared_mutex>{usage_lock_};
  ledger_.close();
}
