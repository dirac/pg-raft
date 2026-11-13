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
    "CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()::text END";

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

  // Raft first: it knows whether the node needs a backup (an InstallSnapshot replaced its log).
  auto endpoint =
      asio::ip::tcp::endpoint(asio::ip::tcp::v4(), config_.servers.at(INT(id_)).ha_port);
  // The disk syncs on the commit path (config.h): off unless the config (or the caller) asks.
  auto raft_options          = options_.raft;
  raft_options.follower_sync = raft_options.follower_sync || config_.follower_sync;
  raft_options.sync_commits  = raft_options.sync_commits || config_.log_sync;
  raft_                      = Server::create<RaftServer>(id_,
                                     asio::ip::tcp::acceptor(io_context(), endpoint),
                                     config_,
                                     raft_dir_,
                                     raft_wal_dir_,
                                     raft_options,
                                     make_shared<Observer>(weak_from_this()));
  tracker_                   = make_shared<NodeWalTracker>(
      raft_, config_.leader_sync, config_.follower_sync, config_.write_op_bytes);

  if (!fs::exists(data_dir_ / "PG_VERSION") || raft_->applied_state().needs_backup) {
    raft_->set_needs_backup(true);  // no elections until there is something to promote
    // Behind the log (EAGAIN): start anyway, needs_backup stays set and the backup thread restores
    // a newer backup once there is one.
    if (auto r = restore_latest_backup(); !r && r.error() != system_error_code(EAGAIN))
      return r;
  }

  // FUSE over data/pg_wal, backed by pg/pg_wal.
  fs::create_directories(data_dir_ / "pg_wal", ec);
  fuse_ = make_unique<Fuse>(data_dir_ / "pg_wal", make_unique<WalFuseOps>(*tracker_, pg_wal_dir_));
  if (!fuse_->start())
    return elog(system_error_code(EIO), "Unable to mount FUSE on {}", data_dir_ / "pg_wal");

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
  if (!pg_->start() || !pg_->wait_ready(pg_port(), 60))
    return elog(system_error_code(EIO), "PostgreSQL did not start on port {}", pg_port());

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

  // Its WAL is committed by definition: the rebuild of a demotion keeps it (backup_end()).
  std::ofstream(base_dir_ / "backup_end")
      << (*latest)->lsn.timeline << ' ' << (*latest)->lsn.lsn << '\n';

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
  fuse_->stop();
  fuse_.reset();

  if (auto r = restore_latest_backup(); !r && r.error() != system_error_code(EAGAIN))
    return r;  // (behind the log: restart anyway, needs_backup stays set and is retried)

  fuse_ = make_unique<Fuse>(data_dir_ / "pg_wal", make_unique<WalFuseOps>(*tracker_, pg_wal_dir_));
  if (!fuse_->start())
    return elog(system_error_code(EIO), "Unable to mount FUSE on {}", data_dir_ / "pg_wal");
  if (auto r = write_standby_config(wal_sender_server_->bound_port()); !r)
    return r;
  tracker_->set_mode(NodeWalTracker::Mode::kFollower);
  needs_restart_.store(false);
  if (!pg_->start() || !pg_->wait_ready(pg_port(), 120))
    return elog(system_error_code(EIO), "PostgreSQL did not restart on port {}", pg_port());
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
    auto lk      = std::unique_lock{standby_mtx_};
    auto reached = standby_cv_.wait_for(
        lk, timeout, [&] { return standby_flushed_ >= wal_end.lsn || deposed(); });
    if (!reached || deposed()) {
      lk.unlock();
      log(ERROR, "Node {}: the standby did not reach {}: stepping down", id_, wal_end);
      if (!deposed())
        raft_->stepdown();
      return;
    }
  }

  // 2. Leader mode before PostgreSQL writes anything, then promote.
  tracker_->set_mode(NodeWalTracker::Mode::kLeader);
  pg_->promote();
  if (!wait_until(
          timeout,
          [this] { return query(pg_port(), "SELECT pg_is_in_recovery()::text") == "false"; },
          deposed)) {
    log(ERROR, "Node {}: PostgreSQL did not leave recovery", id_);
    return;
  }

  // 3. Its new timeline, published by the FUSE hook as a TimelineOp, must be committed (the
  //    history file is written into raft/wal when it is applied) before clients reach it.
  auto file = query(pg_port(), "SELECT pg_walfile_name(pg_current_wal_lsn())");
  auto tli  = file && file->size() >= 8 ? std::stoul(file->substr(0, 8), nullptr, 16) : 0UL;
  if (!wait_until(
          timeout,
          [this, tli] { return latest_history_timeline(raft_wal_dir_).value_or(1U) >= tli; },
          deposed)) {
    log(ERROR, "Node {}: timeline {} was not committed", id_, tli);
    return;
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
    if (auto r = restart_as_standby(); !r)
      log(ERROR,
          "Node {}: unable to restart PostgreSQL as a standby: {}",
          id_,
          r.error().message());
}

LogSeqNumber Node::backup_end() const {
  std::ifstream in(base_dir_ / "backup_end");
  uint32_t      timeline = 0;
  uint64_t      lsn      = 0;
  if (!(in >> timeline >> lsn))
    return {};
  return {.timeline = timeline, .lsn = lsn};
}

Result<void> Node::restart_as_standby() {
  // The hook killed it; reap it.
  pg_->stop(PgInstance::StopMode::Immediate, 5000);

  // Rebuild pg/pg_wal with FUSE unmounted: WalFuseOps caches mmaps by LSN, and a cached mapping of
  // a replaced file must not be reused.
  fuse_->stop();
  fuse_.reset();

  // The backup's WAL counts up to the end of its last segment, not its End-LSN: pg_backup_stop
  // writes an XLOG_SWITCH record there, which ends the segment, so every restored standby replays
  // to the next segment boundary. Zeroing from the End-LSN would remove that record, and a
  // standby promoted from this pg_wal would fork its timeline before where the others already are.
  auto backup = backup_end();
  if (backup.lsn % kWalSegmentSize != 0)
    backup.lsn += kWalSegmentSize - backup.lsn % kWalSegmentSize;
  auto end = std::max(
      raft_->applied_state().wal_end, backup, [](auto const& a, auto const& b) { return a < b; });
  log(INFO, "Node {}: rebuilding pg/pg_wal from raft/wal up to {}", id_, end);
  if (auto r = rebuild_pg_wal(pg_wal_dir_, raft_wal_dir_, end); !r)
    return r;

  fuse_ = make_unique<Fuse>(data_dir_ / "pg_wal", make_unique<WalFuseOps>(*tracker_, pg_wal_dir_));
  if (!fuse_->start())
    return elog(system_error_code(EIO), "Unable to mount FUSE on {}", data_dir_ / "pg_wal");

  if (auto r = write_standby_config(wal_sender_server_->bound_port()); !r)
    return r;
  tracker_->set_mode(NodeWalTracker::Mode::kFollower);
  needs_restart_.store(false);
  if (!pg_->start() || !pg_->wait_ready(pg_port(), 60))
    return elog(system_error_code(EIO), "PostgreSQL did not restart on port {}", pg_port());

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

    if (!conn) {
      auto c = PgClient::connect(pg_port());
      if (!c)
        continue;
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
      continue;
    }
    auto rows = f.get();
    if (!rows || rows->empty() || rows->at(0).size() < 4) {
      conn->close();
      conn.reset();
      continue;
    }

    auto const& row = rows->at(0);
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
  if (pg_ && pg_->is_running()) {
    auto stopped =
        std::async(std::launch::async, [this] { pg_->stop(PgInstance::StopMode::Fast); });
    if (stopped.wait_for(std::chrono::seconds(12)) != std::future_status::ready && raft_) {
      log(WARN, "Node {}: PostgreSQL did not stop; shutting raft down to release its fsyncs", id_);
      raft_->shutdown();
    }
    stopped.get();
  }

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
  if (fuse_)
    fuse_->stop();
  log(INFO, "Node {} stopped", id_);
}
