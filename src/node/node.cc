#include "src/node/node.h"

#include <fstream>
#include <future>

#include "src/backup/dir_backup_store.h"
#include "src/base/io_threads.h"
#include "src/base/logger.h"
#include "src/node/init.h"
#include "src/node/wal_rebuild.h"
#include "src/status/pg_client.h"
#include "src/wal_fuse/wal_fuse_ops.h"

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

/// pg_controldata's "key: value" lines.
std::map<string, string> controldata(fs::path const& bin, fs::path const& dir) {
  struct Capture : public Process {
    using Process::Process;
    std::map<string, string> values;
    std::mutex               mtx;

    void on_stdout_line(std::string_view line) override {
      auto pos = line.find(':');
      if (pos == std::string_view::npos)
        return;
      auto val = line.substr(pos + 1);
      while (!val.empty() && std::isspace(static_cast<unsigned char>(val.front())))
        val.remove_prefix(1);
      auto lk = std::unique_lock{mtx};
      values.emplace(string{line.substr(0, pos)}, string{val});
    }
  };
  Capture p((bin / "pg_controldata").string(), {"-D", dir.string()});
  if (!p.start() || p.wait() != 0)
    return {};
  return std::move(p.values);
}

/// Creates |path| (empty) and syncs it and its directory: it must be on disk before PostgreSQL
/// writes anything.
Result<void> write_marker(fs::path const& path) {
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, file_mode());
  if (fd == -1)
    return elog(system_error_code(errno), "Unable to create {}", path);
  ::fsync(fd);
  ::close(fd);
  if (int d = ::open(path.parent_path().c_str(), O_RDONLY); d != -1) {
    ::fsync(d);
    ::close(d);
  }
  return ok();
}

/// One query on a fresh connection; the first column of the first row.
std::optional<string> query(uint16_t                  port,
                            char const*               sql,
                            std::chrono::milliseconds timeout = 5s) {
  auto conn = PgClient::connect(port);
  if (!conn)
    return std::nullopt;
  std::optional<string> out;
  if (auto stmt = (*conn)->prepare(sql); stmt) {
    auto p = make_shared<std::promise<Result<Rows>>>();
    auto f = p->get_future();
    (*conn)->async_exec(*stmt, [p](Result<Rows> r) { p->set_value(std::move(r)); });
    if (f.wait_for(timeout) == std::future_status::ready)
      if (auto rows = f.get(); rows && !rows->empty() && !rows->at(0).empty())
        out = rows->at(0).at(0);
  }
  (*conn)->close();
  return out;
}

/// A statement on a fresh connection: whether it succeeded (CHECKPOINT returns no rows).
bool execute(uint16_t port, char const* sql, std::chrono::milliseconds timeout) {
  auto conn = PgClient::connect(port);
  if (!conn)
    return false;
  bool done = false;
  if (auto stmt = (*conn)->prepare(sql); stmt) {
    auto p = make_shared<std::promise<Result<Rows>>>();
    auto f = p->get_future();
    (*conn)->async_exec(*stmt, [p](Result<Rows> r) { p->set_value(std::move(r)); });
    done = f.wait_for(timeout) == std::future_status::ready && f.get().has_value();
  }
  (*conn)->close();
  return done;
}

std::optional<uint64_t> parse_lsn(std::optional<string> const& s) {
  unsigned hi = 0, lo = 0;
  if (!s || sscanf(s->c_str(), "%X/%X", &hi, &lo) != 2)
    return std::nullopt;
  return (uint64_t{hi} << 32U) | lo;
}

/// Wait until |pred| holds or |timeout| passes (or |abort| holds).
bool wait_until(
    std::chrono::milliseconds    timeout,
    std::function<bool()> const& pred,
    std::function<bool()> const& abort = [] { return false; }) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (abort())
      return false;
    if (pred())
      return true;
    std::this_thread::sleep_for(200ms);
  }
  return pred();
}

constexpr char const* kPollSql =
    "SELECT pg_is_in_recovery()::text, "
    "CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_flush_lsn()::text END, "
    "CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_walfile_name(pg_current_wal_flush_lsn()) END, "
    "CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()::text END, "
    "(SELECT redo_lsn FROM pg_control_checkpoint())::text";

/// The control file's redo location: where PostgreSQL's replay starts after a crash.
std::optional<uint64_t> redo_location(std::map<string, string> const& control) {
  if (auto pos = control.find("Latest checkpoint's REDO location"); pos != control.end())
    return parse_lsn(pos->second);
  return std::nullopt;
}

/// Whether a WAL segment holding |lsn| (on any timeline) is in |dir|.
bool has_segment(fs::path const& dir, uint64_t lsn) {
  std::error_code ec;
  for (auto const& entry : fs::directory_iterator(dir, ec))
    if (auto seg = log_seq_number(entry.path().filename().string().c_str());
        seg && seg->lsn == lsn - lsn % kWalSegmentSize)
      return true;
  return false;
}

}  // namespace

// ── raft observer ─────────────────────────────────────────────────────────────

struct Node::Observer : public RaftObserver {
  explicit Observer(weak_ptr<Node> node) : node_(std::move(node)) {}

  void applied(RaftIndex, Entry const&, RaftState const& state) override {
    if (auto n = node_.lock())
      n->on_applied(state);
  }
  void became_leader(RaftTerm term) override {
    if (auto n = node_.lock())
      n->on_became_leader(term);
  }
  void stepped_down(RaftTerm term) override {
    if (auto n = node_.lock())
      n->on_stepped_down(term);
  }
  void installed(RaftState const&) override {
    if (auto n = node_.lock())
      n->on_installed();
  }

  weak_ptr<Node> node_;
};

// ── construction and startup ──────────────────────────────────────────────────

shared_ptr<Node> Node::create(Config config, RaftNodeId id, NodeOptions options) {
  return shared_ptr<Node>(new Node(std::move(config), id, std::move(options)));
}

Node::Node(Config config, RaftNodeId id, NodeOptions options)
    : config_(std::move(config)),
      id_(id),
      options_(std::move(options)),
      base_dir_(fs::path{config_.wdir} / std::format("node{}", id_)),
      data_dir_(base_dir_ / "data"),
      pg_wal_dir_(base_dir_ / "pg" / "pg_wal"),
      raft_dir_(base_dir_ / "raft"),
      raft_wal_dir_(raft_dir_ / "wal") {}

Node::~Node() {
  shutdown();
}

Result<void> Node::start() {
  if (config_.backup_store.empty())
    return elog(system_error_code(EINVAL), "No backup_store in the configuration");
  if (INT(id_) >= config_.servers.size())
    return elog(system_error_code(EINVAL), "No server {} in the configuration", id_);

  std::error_code ec;
  for (auto const& d : {base_dir_, pg_wal_dir_, raft_dir_, raft_wal_dir_})
    fs::create_directories(d, ec);
  if (ec)
    return elog(ec, "Unable to create the directories under {}", base_dir_);

  store_ = make_shared<DirBackupStore>(config_.backup_store);
  (void)store_->abort(id_);  // a lease (and a partial copy) this node left before a restart
  pg_ = make_unique<PgInstance>(pg_bin(config_), data_dir_, pg_port());

  // The network frames' checksums (process-wide; TCP checksums the segments already).
  Connection::set_frame_checksums(config_.frame_checksums);
  // The version the WAL sender reports to the standby (process-wide).
  WalSender::set_pg_major_version(config_.pg_major_version);

  // Raft first: it knows whether the node needs a backup (an InstallSnapshot replaced its log).
  auto endpoint =
      asio::ip::tcp::endpoint(asio::ip::tcp::v4(), config_.servers.at(INT(id_)).ha_port);
  // The acceptor binds here (it throws on an error, e.g. the port in use): an error, not an abort.
  std::optional<asio::ip::tcp::acceptor> acceptor;
  try {
    acceptor.emplace(io_context(), endpoint);
  } catch (boost::system::system_error const& e) {
    return elog(system_error_code(e.code().value()), "Unable to listen on raft port {}", endpoint.port());
  }
  // The disk syncs on the commit path (config.h): off unless the config (or the caller) asks.
  auto raft_options          = options_.raft;
  raft_options.leader_sync   = raft_options.leader_sync || config_.leader_sync;
  raft_options.follower_sync = raft_options.follower_sync || config_.follower_sync;
  // Compaction waits for keep_from (the latest backup's segment, set below and by retention()).
  raft_options.compact_without_keep_from = false;
  // A node whose standby is more than 4 segments behind leaves the elections to the others.
  if (raft_options.max_standby_lag == 0)
    raft_options.max_standby_lag = 4 * kWalSegmentSize;
  // raft/wal's WAL from the local PostgreSQL's redo point on is kept from the first moment (the
  // deletion thread runs as soon as raft starts; a restarting standby replays from there).
  if (fs::exists(data_dir_ / "PG_VERSION"))
    if (auto redo = redo_location(controldata(pg_bin(config_), data_dir_)); redo && *redo != 0)
      raft_options.wal_gc_floor = *redo;
  raft_options.ledger_file_size = options_.ledger_file_size_for_test != 0
                                      ? options_.ledger_file_size_for_test
                                      : config_.ledger_file_size;
  raft_                      = Server::create<RaftServer>(id_,
                                     std::move(*acceptor),
                                     config_,
                                     raft_dir_,
                                     raft_wal_dir_,
                                     raft_options,
                                     make_shared<Observer>(weak_from_this()));
  // The latest backup's WAL is kept from the start (compaction waits for it).
  if (auto latest = store_->latest(); latest && *latest)
    raft_->set_wal_keep_from(keep_from(**latest));
  sync_rep_ = config_.commit_wait == "sync_rep";
  tracker_  = make_shared<NodeWalTracker>(raft_, config_.write_op_bytes, !sync_rep_);

  // A previous run killed with its FUSE mount in place leaves a dead mount on data/pg_wal: a
  // restore or rebuild below would fail on it. (After the raft port's bind: a second instance of
  // this node fails there, before it could unmount the running one's.)
  Fuse::remove_stale_mount(data_dir_ / "pg_wal");

  bool restored = false;
  if (!fs::exists(data_dir_ / "PG_VERSION") || raft_->applied_state().needs_backup) {
    raft_->set_needs_backup(true);  // no elections until there is something to promote
    // Behind the log (EAGAIN): start anyway, needs_backup stays set and the backup thread restores
    // a newer backup once there is one.
    if (auto r = restore_latest_backup(); !r && r.error() != system_error_code(EAGAIN))
      return r;
    restored = true;
  }

  // A standby whose redo point (its last restartpoint) is before raft's first segment can't
  // recover: that WAL was compacted away. The latest backup is past it (compaction keeps its WAL).
  if (!restored && fs::exists(data_dir_ / "PG_VERSION")) {
    auto const control = controldata(pg_bin(config_), data_dir_);
    auto const redo    = redo_location(control);
    auto const first   = raft_->applied_state().first_segment;
    if (redo && *redo != 0)
      raft_->set_wal_gc_floor(*redo);
    // (A restored backup's redo point is usually before it: its WAL came with the backup, into
    // pg/pg_wal. Behind means that WAL is nowhere.)
    if (redo && first.lsn != 0 && *redo < first.lsn && !has_segment(pg_wal_dir_, *redo) &&
        !has_segment(raft_wal_dir_, *redo)) {
      log(WARN,
          "Node {}: PostgreSQL's redo point {:X} is before the first segment {}, and its WAL is "
          "gone: restoring the latest backup",
          id_,
          *redo,
          first);
      raft_->set_needs_backup(true);
      if (auto r = restore_latest_backup(); !r && r.error() != system_error_code(EAGAIN))
        return r;
      restored = true;
    }
  }

  // After an unclean end of the previous run, pg/pg_wal can hold WAL that never committed (a
  // primary's write whose fsync failed or never completed): rebuild it from raft/wal, which holds
  // committed WAL only, before PostgreSQL starts on it. (A restore has just replaced it.)
  if (fs::exists(running_marker()) && !restored && fs::exists(data_dir_ / "PG_VERSION")) {
    log(WARN, "Node {}: the previous run did not end cleanly: rebuilding pg/pg_wal", id_);
    if (auto r = rebuild(); !r)
      return r;
  }
  if (auto r = write_marker(running_marker()); !r)
    return r;

  // FUSE over data/pg_wal, backed by pg/pg_wal.
  if (hook_mode()) {
    hook_server_ = make_shared<WalHookServer>(
        fs::path{config_.socket_dir} / std::format(".s.pg_raft.{}", pg_port()), pg_wal_dir_, *tracker_);
    if (auto r = hook_server_->start(); !r)
      return r;
  }
  if (auto r = attach_wal(); !r)
    return r;

  // The WAL sender, which the standby streams from.
  auto cd        = controldata(pg_bin(config_), data_dir_);
  auto system_id = cd.contains("Database system identifier")
                       ? std::stoull(cd.at("Database system identifier"))
                       : 0ULL;
  auto timeline  = cd.contains("Latest checkpoint's TimeLineID")
                       ? static_cast<uint32_t>(std::stoul(cd.at("Latest checkpoint's TimeLineID")))
                       : 1U;
  if (system_id == 0)
    return elog(system_error_code(EIO), "pg_controldata failed on {}", data_dir_);

  auto weak = weak_from_this();
  {
    auto lk    = std::unique_lock{senders_mtx_};
    committed_ = raft_->applied_state().flushed_lsn;
  }
  wal_sender_server_ = WalSenderServer::create(
      config_.socket_dir,
      static_cast<uint16_t>(pg_port() + 20000U),
      raft_wal_dir_,
      system_id,
      timeline,
      committed_.lsn,
      [weak](shared_ptr<WalSender> s) {
        if (auto n = weak.lock()) {
          auto lk = std::unique_lock{n->senders_mtx_};
          n->senders_.push_back(s);
          if (n->committed_.lsn > 0)
            s->advance(n->committed_);
        }
      },
      [weak](StandbyStatus const& st) {
        if (auto n = weak.lock()) {
          {
            auto lk             = std::unique_lock{n->standby_mtx_};
            n->standby_flushed_ = std::max(n->standby_flushed_, st.flushed_lsn);
          }
          n->standby_cv_.notify_all();
        }
      });
  if (!wal_sender_server_->start())
    return elog(system_error_code(EIO), "Unable to start the WAL sender in {}", config_.socket_dir);

  // PostgreSQL as a standby of the local WAL sender.
  if (auto r = write_standby_config(wal_sender_server_->bound_port()); !r)
    return r;
  tracker_->set_mode(NodeWalTracker::Mode::kFollower);
  // A standby after a crash replays from its last restartpoint before it accepts connections:
  // wait as long as PostgreSQL runs (and recovers), not a fixed time.
  // (At most 20 minutes: one that waits for WAL nobody has any more never gets there.)
  auto started = pg_->start() && pg_->wait_ready(pg_port(), 60);
  for (int i = 0; i < 19 && !started && pg_->is_running() && !pg_->exited(); ++i) {
    log(INFO, "Node {}: PostgreSQL is still starting (recovery)", id_);
    started = pg_->wait_ready(pg_port(), 60);
  }
  if (!started) {
    // A standby that can't recover from its data directory and pg_wal (e.g. WAL it can't replay):
    // the node would fail every start the same way. Restore the latest backup once instead; the
    // standby then streams the rest from raft/wal.
    if (restored)
      return elog(system_error_code(EIO), "PostgreSQL did not start on port {}", pg_port());
    log(ERROR, "Node {}: PostgreSQL did not start on port {}: restoring the latest backup", id_, pg_port());
    raft_->set_needs_backup(true);
    if (auto r = restore_and_restart(); !r)
      return r;
  }

  started_.store(true);
  transition_thread_ = std::thread([this] { transitions(); });
  poll_thread_       = std::thread([this] { poll(); });
  backup_thread_     = std::thread([this] { backups(); });
  log(INFO, "Node {} started: PostgreSQL on {} as a standby", id_, pg_port());
  return ok();
}

Result<void> Node::restore_latest_backup() {
  auto latest = store_->latest();
  if (!latest)
    return latest.error();
  if (!*latest)
    return elog(system_error_code(ENOENT),
                "The backup store {} is empty: run pg_raft_initdb first",
                config_.backup_store);
  log(INFO, "Node {}: restoring the backup {}", id_, **latest);

  std::error_code ec;
  auto            scratch = base_dir_ / "restore";
  fs::remove_all(scratch, ec);
  if (auto r = store_->get(**latest, scratch); !r)
    return r;

  // Into the same data directory (PostgreSQL's System V IPC is keyed by its inode).
  if (!pg_->restore(scratch))
    return elog(system_error_code(EIO), "Unable to restore {} into {}", scratch, data_dir_);
  fs::remove_all(scratch, ec);

  // The backup's WAL goes to pg/pg_wal (the FUSE backing directory), and to raft/wal where raft
  // has no newer copy: the standbys stream the backup's last segment from there (every node
  // restores the same backup, so the copies agree).
  for (auto const& entry : fs::directory_iterator(pg_wal_dir_, ec))
    fs::remove_all(entry.path(), ec);
  for (auto const& entry : fs::directory_iterator(data_dir_ / "pg_wal", ec)) {
    auto name = entry.path().filename().string();
    if (entry.is_regular_file() && (log_seq_number(name.c_str()) || history_timeline(name)) &&
        !fs::exists(raft_wal_dir_ / name))
      fs::copy_file(entry.path(), raft_wal_dir_ / name, ec);
    fs::rename(entry.path(), pg_wal_dir_ / name, ec);
    if (ec)
      return elog(ec, "Unable to move {} into {}", entry.path(), pg_wal_dir_);
  }

  // Its replay starts at the backup's redo point: keep raft/wal's WAL from there on.
  if (auto redo = redo_location(controldata(pg_bin(config_), data_dir_)); redo && *redo != 0)
    raft_->set_wal_gc_floor(*redo);

  if (!raft_->restored_backup((*latest)->lsn))
    return elog(system_error_code(EAGAIN), "Node {}: the restored backup is behind the log", id_);
  return ok();
}

Result<void> Node::write_standby_config(uint16_t wal_sender_port) {
  std::ofstream(data_dir_ / "standby.signal").close();

  // The node's settings live in pg_raft.conf, rewritten on every start, included once.
  {
    std::ofstream conf(data_dir_ / "pg_raft.conf", std::ios::trunc);
    conf << std::format("primary_conninfo = 'host={} port={} application_name=node{}'\n",
                        config_.socket_dir,
                        wal_sender_port,
                        id_)
         << "recovery_target_timeline = 'latest'\n";
    // commit_wait = "sync_rep": a commit waits for raft in PostgreSQL's synchronous replication
    // wait. With FUSE, the node is the primary's synchronous standby (SyncAcker); with the hooks,
    // the extension's sync_rep_wait_hook asks the hook server, and the wait is not interrupted.
    if (sync_rep_ && !hook_mode())
      conf << "synchronous_standby_names = 'pg_raft'\n";
    // wal_capture = "hook": the extension reports the WAL to the hook server (it needs pg-raft's
    // PostgreSQL, install_dir).
    if (hook_mode())
      conf << "shared_preload_libraries = 'pg_raft_wal'\n"
           << std::format("pg_raft.socket = '{}'\n", hook_server_->socket_path().string())
           << std::format("pg_raft.sync_rep = {}\n", sync_rep_ ? "on" : "off");
    if (!conf.flush())
      return elog(system_error_code(EIO), "Unable to write {}", data_dir_ / "pg_raft.conf");
  }
  auto          include = string{"include_if_exists = 'pg_raft.conf'"};
  std::ifstream in(data_dir_ / "postgresql.conf");
  auto          content = string{std::istreambuf_iterator<char>(in), {}};
  if (content.find(include) == string::npos)
    std::ofstream(data_dir_ / "postgresql.conf", std::ios::app) << "\n" << include << "\n";
  return ok();
}

// ── raft hooks (the server lock is held) ─────────────────────────────────────

void Node::on_applied(RaftState const& state) {
  if (auto a = acker_.load(); a)
    a->ack(state.wal_end.lsn);
  auto lk    = std::unique_lock{senders_mtx_};
  committed_ = state.flushed_lsn;
  for (auto const& w : senders_)
    if (auto s = w.lock())
      s->advance(committed_);
}

void Node::advance_senders(LogSeqNumber lsn) {
  auto lk = std::unique_lock{senders_mtx_};
  for (auto const& w : senders_)
    if (auto s = w.lock())
      s->advance(lsn);
}

void Node::on_became_leader(RaftTerm term) {
  {
    auto lk       = std::unique_lock{role_mtx_};
    leader_term_  = term;
    role_changed_ = true;
  }
  role_cv_.notify_all();
}

void Node::on_stepped_down(RaftTerm term) {
  // At once, before anything else: nothing PostgreSQL writes from now on may be acknowledged, and
  // no client of the old primary is served.
  log(INFO, "Node {}: lost leadership of term {}: fencing and killing PostgreSQL", id_, term);
  if (tracker_)
    tracker_->set_mode(NodeWalTracker::Mode::kFenced);
  // No more acknowledgements; stopped (joined) off the raft lock.
  if (auto a = acker_.exchange(nullptr); a)
    std::thread([a] { a->stop(); }).detach();
  if (pg_)
    pg_->kill();
  needs_restart_.store(true);
  primary_.store(false);
  {
    auto lk = std::unique_lock{role_mtx_};
    leader_term_.reset();
    role_changed_ = true;
  }
  role_cv_.notify_all();
}

void Node::on_installed() {
  // The log was replaced: the standby is behind the snapshot and is restored from a backup. Its
  // PostgreSQL goes now (lock-free); the transition thread restores.
  log(INFO, "Node {}: the log was replaced by an InstallSnapshot: restoring a backup", id_);
  if (pg_)
    pg_->kill();
  needs_restart_.store(true);
  {
    auto lk            = std::unique_lock{role_mtx_};
    restore_requested_ = true;
  }
  role_cv_.notify_all();
}

Result<void> Node::restore_and_restart() {
  auto const state = raft_->applied_state();
  if (!state.needs_backup)
    return ok();

  // No gap: the standby replays the backup's WAL, then streams from the start of the segment that
  // holds its end, which raft/wal must have.
  auto latest = store_->latest();
  if (!latest)
    return latest.error();
  if (!*latest)
    return elog(system_error_code(ENOENT), "Node {}: no backup to restore", id_);
  auto const backup_segment = (*latest)->lsn.lsn - (*latest)->lsn.lsn % kWalSegmentSize;
  if (state.first_segment.lsn > backup_segment)
    return elog(system_error_code(EAGAIN),
                "Node {}: the latest backup {} is older than the first entry's segment {}",
                id_,
                **latest,
                state.first_segment);

  pg_->kill();
  pg_->stop(PgInstance::StopMode::Immediate, 5000);  // reaps it, and releases its IPC
  detach_wal();

  if (auto r = restore_latest_backup(); !r && r.error() != system_error_code(EAGAIN))
    return r;  // (behind the log: restart anyway, needs_backup stays set and is retried)

  if (auto r = attach_wal(); !r)
    return r;
  if (auto r = write_standby_config(wal_sender_server_->bound_port()); !r)
    return r;
  tracker_->set_mode(NodeWalTracker::Mode::kFollower);
  if (!pg_->start() || !pg_->wait_ready(pg_port(), 120))
    return elog(system_error_code(EIO), "PostgreSQL did not restart on port {}", pg_port());
  needs_restart_.store(false);
  log(INFO, "Node {}: restored and restarted as a standby", id_);
  return ok();
}

void Node::retention() {
  if (auto latest = store_->latest(); latest && *latest)
    raft_->set_wal_keep_from(keep_from(**latest));

  // PostgreSQL's unlinked segments (.X), before the first entry's segment: nothing refers to them.
  auto const first = raft_->applied_state().first_segment;
  if (first.lsn == 0)
    return;
  std::error_code                    ec;
  std::vector<std::filesystem::path> old;
  for (auto const& entry : std::filesystem::directory_iterator(pg_wal_dir_, ec)) {
    auto name = entry.path().filename().string();
    if (!name.starts_with('.'))
      continue;
    if (auto seg = log_seq_number(name.c_str() + 1); seg && seg->lsn + kWalSegmentSize <= first.lsn)
      old.push_back(entry.path());
  }
  for (auto const& p : old)
    std::filesystem::remove(p, ec);
}

// ── transitions ───────────────────────────────────────────────────────────────

void Node::transitions() {
  while (true) {
    auto lk = std::unique_lock{role_mtx_};
    role_cv_.wait(lk, [this] { return role_changed_ || restore_requested_ || stopping_; });
    if (stopping_)
      return;
    auto const restore = std::exchange(restore_requested_, false);
    auto const changed = std::exchange(role_changed_, false);
    auto       term    = leader_term_;
    lk.unlock();

    if (restore)
      if (auto r = restore_and_restart(); !r)
        log(ERROR, "Node {}: the restore failed ({}); retrying later", id_, r.error().message());
    if (!changed)
      continue;
    if (term)
      promote(*term);
    else
      demote();
  }
}

bool Node::still_leader(RaftTerm term) const {
  return raft_->is_leader() && raft_->leadership_term() == term;
}

void Node::promote(RaftTerm term) {
  if (!still_leader(term))
    return;

  // Deposed and elected again before the demotion ran: restart PostgreSQL as a standby first.
  if (needs_restart_.load()) {
    if (auto r = restart_as_standby(); !r) {
      log(ERROR, "Node {}: unable to restart PostgreSQL: stepping down", id_);
      raft_->stepdown();
      return;
    }
  }
  if (!still_leader(term) || !pg_->is_running())
    return;

  auto const timeout =
      std::chrono::duration_cast<std::chrono::milliseconds>(options_.promote_timeout);
  // Give up when the node is no longer the leader of |term|, or is shutting down.
  auto const deposed = [this, term] {
    auto lk = std::unique_lock{role_mtx_};
    return stopping_ || !still_leader(term);
  };
  auto const wal_end = raft_->applied_state().wal_end;
  log(INFO, "Node {}: leader of term {}: promoting, wal_end = {}", id_, term, wal_end);

  // 1. The standby receives everything in raft/wal: every byte a previous primary acknowledged.
  if (wal_end.lsn > 0) {
    advance_senders(wal_end);
    // In slices: a deposition doesn't notify standby_cv_ (only the standby's reports do).
    auto lk       = std::unique_lock{standby_mtx_};
    auto deadline = std::chrono::steady_clock::now() + timeout;
    auto reached  = false;
    while (!(reached = standby_flushed_ >= wal_end.lsn) && !deposed() &&
           std::chrono::steady_clock::now() < deadline)
      standby_cv_.wait_for(lk, 200ms);
    if (!reached || deposed()) {
      lk.unlock();
      log(ERROR, "Node {}: the standby did not reach {}: stepping down", id_, wal_end);
      if (!deposed())
        raft_->stepdown();
      return;
    }
  }

  // 2. PostgreSQL picks its new timeline as one past the newest history file it sees: every
  //    committed history file goes into its pg_wal first (a standby learns of a timeline only by
  //    streaming into it). Otherwise it may pick a timeline another primary already used, and two
  //    primaries' WAL would share a timeline.
  auto const committed_tli = latest_history_timeline(raft_wal_dir_).value_or(1U);
  for (uint32_t t = 2; t <= committed_tli; ++t)
    if (auto content = read_history_file(raft_wal_dir_, t), have = read_history_file(pg_wal_dir_, t);
        content && (!have || *have != *content))
      if (auto r = write_history_file(pg_wal_dir_, t, *content); !r)
        log(ERROR, "Node {}: unable to install the history of timeline {}", id_, t);

  // 3. Leader mode before PostgreSQL writes anything, then promote.
  tracker_->set_mode(NodeWalTracker::Mode::kLeader);
  pg_->promote();
  if (!wait_until(
          timeout,
          [this] { return query(pg_port(), "SELECT pg_is_in_recovery()::text") == "false"; },
          deposed)) {
    // A leader that doesn't serve leaves the cluster without a primary: hand the leadership over.
    log(ERROR, "Node {}: PostgreSQL did not leave recovery: stepping down", id_);
    if (!deposed())
      raft_->stepdown();
    return;
  }

  // 4. Its new timeline, published by the FUSE hook as a TimelineOp, must be committed (the
  //    history file is written into raft/wal when it is applied) before clients reach it.
  auto file = query(pg_port(), "SELECT pg_walfile_name(pg_current_wal_lsn())");
  auto tli  = file && file->size() >= 8 ? std::stoul(file->substr(0, 8), nullptr, 16) : 0UL;
  if (tli <= committed_tli) {
    // A timeline in use already: it must not serve (its WAL would mix with the other's).
    log(ERROR,
        "Node {}: PostgreSQL picked timeline {}, not past the committed {}: stepping down",
        id_,
        tli,
        committed_tli);
    if (!deposed())
      raft_->stepdown();
    return;
  }
  if (!wait_until(
          timeout,
          [this, tli] { return latest_history_timeline(raft_wal_dir_).value_or(1U) >= tli; },
          deposed)) {
    log(ERROR, "Node {}: timeline {} was not committed: stepping down", id_, tli);
    if (!deposed())
      raft_->stepdown();
    return;
  }

  // 5. A checkpoint before clients come. The control file keeps the standby's minimum recovery
  //    point until the first checkpoint after the promotion (PostgreSQL clears it there: "crash
  //    recovery should always recover to the end of WAL"), and the node restarts a crashed primary
  //    as a standby, which trusts that point: it then declares consistency there, before a later
  //    truncation it would have replayed, and PANICs on "WAL contains references to invalid pages".
  if (!execute(pg_port(), "CHECKPOINT", timeout))
    log(WARN, "Node {}: the checkpoint after the promotion did not complete", id_);

  // 6. commit_wait = "sync_rep" with FUSE: the synchronous standby the commits wait for, before
  //    clients come. (With the hooks, the commits wait in the tracker, in leader mode since 3.)
  if (sync_rep_ && !hook_mode()) {
    auto a = make_shared<SyncAcker>(pg_port());
    if (auto r = a->start(); !r) {
      log(ERROR, "Node {}: the synchronous standby connection failed: stepping down", id_);
      if (!deposed())
        raft_->stepdown();
      return;
    }
    a->ack(raft_->applied_state().wal_end.lsn);
    acker_.store(a);
  }

  if (deposed())
    return;
  raft_->set_serving(true);
  primary_.store(true);
  log(INFO, "Node {}: serving as the primary on timeline {}", id_, tli);
}

void Node::demote() {
  primary_.store(false);
  raft_->set_serving(false);
  if (needs_restart_.load())
    if (auto r = restart_as_standby(); !r) {
      // As at a start: a standby that doesn't come up from its data directory gets the latest
      // backup. (Still failing: the backup thread asks again.)
      log(ERROR,
          "Node {}: unable to restart PostgreSQL as a standby ({}): restoring the latest backup",
          id_,
          r.error().message());
      raft_->set_needs_backup(true);
      if (auto r2 = restore_and_restart(); !r2)
        log(ERROR, "Node {}: the restore failed: {}", id_, r2.error().message());
    }
}

Result<void> Node::rebuild() {
  log(INFO, "Node {}: rebuilding pg/pg_wal from raft/wal", id_);
  return rebuild_pg_wal(pg_wal_dir_, raft_wal_dir_);
}

Result<void> Node::attach_wal() {
  std::error_code ec;
  auto const      pg_wal = data_dir_ / "pg_wal";
  if (!hook_mode()) {
    fs::create_directories(pg_wal, ec);
    fuse_ = make_unique<Fuse>(pg_wal, make_unique<WalFuseOps>(*tracker_, pg_wal_dir_));
    if (!fuse_->start())
      return elog(system_error_code(EIO), "Unable to mount FUSE on {}", pg_wal);
    return ok();
  }

  // A link to pg/pg_wal. A plain directory there (a restored backup's, or a FUSE run's mount
  // point): its files go to pg/pg_wal first.
  if (fs::is_symlink(pg_wal, ec))
    return ok();
  if (fs::is_directory(pg_wal, ec)) {
    for (auto const& entry : fs::directory_iterator(pg_wal, ec)) {
      fs::rename(entry.path(), pg_wal_dir_ / entry.path().filename(), ec);
      if (ec)
        return elog(ec, "Unable to move {} into {}", entry.path(), pg_wal_dir_);
    }
    fs::remove(pg_wal, ec);
    if (ec)
      return elog(ec, "Unable to remove {}", pg_wal);
  }
  fs::create_directory_symlink(fs::path{".."} / "pg" / "pg_wal", pg_wal, ec);
  if (ec)
    return elog(ec, "Unable to link {} to {}", pg_wal, pg_wal_dir_);
  return ok();
}

void Node::detach_wal() {
  if (fuse_) {  // (none after a failed attempt)
    fuse_->stop();
    fuse_.reset();
  }
  if (hook_server_)
    hook_server_->reset_segments();
}

Result<void> Node::restart_as_standby() {
  // The hook killed it; reap it.
  pg_->stop(PgInstance::StopMode::Immediate, 5000);

  // Rebuild pg/pg_wal with FUSE unmounted: WalFuseOps caches mmaps by LSN, and a cached mapping of
  // a replaced file must not be reused.
  detach_wal();

  // The backup's WAL counts up to the end of its last segment, not its End-LSN: pg_backup_stop
  // writes an XLOG_SWITCH record there, which ends the segment, so every restored standby replays
  // to the next segment boundary. Zeroing from the End-LSN would remove that record, and a
  // standby promoted from this pg_wal would fork its timeline before where the others already are.
  // Its redo point before raft's first segment: that WAL is gone, restore the latest backup.
  auto const control = controldata(pg_bin(config_), data_dir_);
  auto const redo    = redo_location(control);
  auto const first   = raft_->applied_state().first_segment;
  if (redo && *redo != 0)
    raft_->set_wal_gc_floor(*redo);
  if (redo && first.lsn != 0 && *redo < first.lsn && !has_segment(pg_wal_dir_, *redo) &&
      !has_segment(raft_wal_dir_, *redo)) {
    log(WARN,
        "Node {}: PostgreSQL's redo point {:X} is before the first segment {}, and its WAL is gone: "
        "restoring the latest backup",
        id_,
        *redo,
        first);
    raft_->set_needs_backup(true);
    if (auto r = restore_latest_backup(); !r && r.error() != system_error_code(EAGAIN))
      return r;
  } else if (auto r = rebuild(); !r) {
    return r;
  }

  if (auto r = attach_wal(); !r)
    return r;

  if (auto r = write_standby_config(wal_sender_server_->bound_port()); !r)
    return r;
  tracker_->set_mode(NodeWalTracker::Mode::kFollower);
  // Cleared only once it runs: a failed restart must not look like a standby to a promotion.
  if (!pg_->start() || !pg_->wait_ready(pg_port(), 60))
    return elog(system_error_code(EIO), "PostgreSQL did not restart on port {}", pg_port());
  needs_restart_.store(false);

  log(INFO, "Node {}: PostgreSQL restarted as a standby", id_);
  return ok();
}

// ── status poller ─────────────────────────────────────────────────────────────

void Node::poll() {
  shared_ptr<PgClient> conn;
  PgStmt               stmt;

  auto stopping = [this] {
    auto lk = std::unique_lock{role_mtx_};
    return stopping_;
  };

  while (!stopping()) {
    std::this_thread::sleep_for(options_.poll_interval);

    // PostgreSQL exited on its own (a standby's PANIC, a crash): nothing else restarts it.
    if (started_.load() && !needs_restart_.load() && pg_->exited()) {
      if (raft_->is_leader() || primary_.load()) {
        log(ERROR, "Node {}: PostgreSQL exited on the primary: stepping down", id_);
        tracker_->set_mode(NodeWalTracker::Mode::kFenced);
        needs_restart_.store(true);
        raft_->stepdown();
      } else {
        log(ERROR, "Node {}: PostgreSQL exited: restarting it as a standby", id_);
        needs_restart_.store(true);
        {
          auto lk       = std::unique_lock{role_mtx_};
          role_changed_ = true;
        }
        role_cv_.notify_all();
      }
      if (conn) {
        conn->close();
        conn.reset();
      }
      continue;
    }

    // PostgreSQL unreachable (killed, restarting, recovering): its last status is no longer true.
    // (A stale standby position would let the lag check of the elections pass.)
    auto const unreachable = [this] { raft_->set_pg_status(std::nullopt, std::nullopt); };

    if (!conn) {
      auto c = PgClient::connect(pg_port());
      if (!c) {
        unreachable();
        continue;
      }
      auto s = (*c)->prepare(kPollSql);
      if (!s) {
        (*c)->close();
        continue;
      }
      conn = *c;
      stmt = *s;
    }

    auto p = make_shared<std::promise<Result<Rows>>>();
    auto f = p->get_future();
    conn->async_exec(stmt, [p](Result<Rows> r) { p->set_value(std::move(r)); });
    if (f.wait_for(2s) != std::future_status::ready) {
      conn->close();
      conn.reset();
      unreachable();
      continue;
    }
    auto rows = f.get();
    if (!rows || rows->empty() || rows->at(0).size() < 5) {
      conn->close();
      conn.reset();
      unreachable();
      continue;
    }

    auto const& row = rows->at(0);
    if (auto redo = parse_lsn(row.at(4)); redo && *redo != 0)
      raft_->set_wal_gc_floor(*redo);
    if (row.at(0) == "false") {
      auto lsn  = parse_lsn(row.at(1));
      auto file = row.at(2);
      if (lsn && file && file->size() >= 8) {
        auto tli = static_cast<uint32_t>(std::stoul(file->substr(0, 8), nullptr, 16));
        tracker_->set_flushed_lsn({.timeline = tli, .lsn = *lsn});
      }
      raft_->set_pg_status(lsn, std::nullopt);
    } else {
      raft_->set_pg_status(std::nullopt, parse_lsn(row.at(3)));
    }
  }
  if (conn)
    conn->close();
}

// ── backups ───────────────────────────────────────────────────────────────────

bool Node::wait_stopping(std::chrono::milliseconds timeout) {
  auto lk = std::unique_lock{role_mtx_};
  return role_cv_.wait_for(lk, timeout, [this] { return stopping_; });
}

void Node::backups() {
  while (!wait_stopping(options_.backup_check_interval)) {
    maybe_backup();
    retention();

    // A restart as a standby that failed (PostgreSQL still down, not the leader): ask again.
    if (needs_restart_.load() && !raft_->is_leader()) {
      {
        auto lk       = std::unique_lock{role_mtx_};
        role_changed_ = true;
      }
      role_cv_.notify_all();
    }

    // A restore that couldn't be done yet (no backup recent enough): ask again.
    if (raft_->applied_state().needs_backup) {
      {
        auto lk            = std::unique_lock{role_mtx_};
        restore_requested_ = true;
      }
      role_cv_.notify_all();
    }
  }
}

void Node::maybe_backup() {
  // A standby with a running PostgreSQL; never the primary's node.
  if (primary_.load() || needs_restart_.load() || raft_->is_leader() || !pg_->is_running())
    return;
  auto const state = raft_->applied_state();
  if (state.needs_backup)
    return;

  auto const policy = BackupPolicy{
      .interval        = std::chrono::seconds{config_.backup_interval_secs},
      .wal_size        = config_.backup_wal_size,
      .pending_timeout = std::chrono::seconds{config_.backup_pending_timeout_secs},
  };
  auto const now    = std::time(nullptr);
  auto const latest = store_->latest();
  if (!latest)
    return;
  if (!backup_due(policy, INT(id_), u4(config_.servers.size()), *latest, now, state.wal_end))
    return;
  auto const pending = store_->pending();
  if (!pending || pending_blocks(policy, *pending, id_, now))
    return;

  auto lease = store_->begin(id_, now);
  if (!lease)
    return;
  log(INFO,
      "Node {}: taking a backup (the latest is {})",
      id_,
      latest->has_value() ? (*latest)->name() : "none");

  // The lease is refreshed while pg_basebackup and the verification run.
  std::atomic_bool done{false};
  std::thread      refresher([&] {
    auto const every =
        std::chrono::duration_cast<std::chrono::milliseconds>(policy.pending_timeout) / 4;
    while (!done.load()) {
      for (auto waited = 0ms; waited < every && !done.load(); waited += 100ms)
        std::this_thread::sleep_for(100ms);
      if (!done.load())
        (void)store_->refresh(*lease, std::time(nullptr));
    }
  });

  auto const scratch = base_dir_ / "backup";
  auto       lk      = pg_->backup(scratch);
  if (lk)
    lk = pg_->verify_backup(scratch, std::move(lk));
  auto lsn = lk ? backup_end_lsn(scratch) : Result<LogSeqNumber>{system_error_code(EIO)};

  done.store(true);
  refresher.join();

  if (lsn) {
    if (auto b = store_->put(*lease, scratch, *lsn); b)
      log(INFO, "Node {}: stored the backup {}", id_, *b);
    else
      (void)store_->abort(id_);
  } else {
    log(ERROR, "Node {}: the backup failed", id_);
    (void)store_->abort(id_);
  }
  std::error_code ec;
  std::filesystem::remove_all(scratch, ec);
}

// ── shutdown ──────────────────────────────────────────────────────────────────

void Node::shutdown() {
  {
    auto lk = std::unique_lock{role_mtx_};
    if (stopping_)
      return;
    stopping_ = true;
  }
  role_cv_.notify_all();
  standby_cv_.notify_all();
  if (transition_thread_.joinable())
    transition_thread_.join();
  if (poll_thread_.joinable())
    poll_thread_.join();
  if (backup_thread_.joinable())
    backup_thread_.join();

  // PostgreSQL first, cleanly: on the primary its shutdown checkpoint still commits through raft
  // (the node still leads), and a clean stop releases its System V shared memory and semaphores,
  // which a kill leaves behind (only a postmaster in the same data directory reclaims them).
  //
  // A leader without a quorum (the last node of a cluster shutting down) never commits that
  // checkpoint: its fsync blocks in FUSE, and on Linux a process waiting on a FUSE request the
  // daemon has read cannot be killed, so the checkpointer holds PostgreSQL's output pipes and
  // stop() never returns. Shutting raft down fails the pending fsyncs (close() completes the
  // handlers with kNotLeader); the checkpointer then exits and the stop finishes.
  // Clean: PostgreSQL ran and stopped (Fast) within the wait, its fsyncs all answered.
  bool clean = false;
  if (pg_ && pg_->is_running()) {
    auto stopped =
        std::async(std::launch::async, [this] { pg_->stop(PgInstance::StopMode::Fast); });
    clean = stopped.wait_for(std::chrono::seconds(12)) == std::future_status::ready;
    if (!clean && raft_) {
      log(WARN, "Node {}: PostgreSQL did not stop; shutting raft down to release its fsyncs", id_);
      raft_->shutdown();
    }
    stopped.get();
  }

  // PostgreSQL is stopped: no commit waits for the synchronous standby any more.
  if (auto a = acker_.exchange(nullptr); a)
    a->stop();

  // Then raft hands the leadership over (stepped_down finds nothing left to kill).
  if (raft_)
    raft_->shutdown();
  if (pg_ && pg_->is_running())
    pg_->stop(PgInstance::StopMode::Immediate);

  if (wal_sender_server_)
    wal_sender_server_->shutdown();
  {
    auto lk = std::unique_lock{senders_mtx_};
    for (auto const& w : senders_)
      if (auto s = w.lock())
        s->shutdown();
    senders_.clear();
  }
  detach_wal();
  if (hook_server_)
    hook_server_->stop();
  // A clean end: the next start needs no rebuild of pg/pg_wal.
  if (clean) {
    std::error_code ec;
    fs::remove(running_marker(), ec);
  }
  log(INFO, "Node {} stopped{}", id_, clean ? "" : " (not cleanly: the next start rebuilds pg/pg_wal)");
}
