#include "src/node/node.h"

#include <fstream>

#include "src/backup/dir_backup_store.h"
#include "src/base/io_threads.h"
#include "src/base/logger.h"
#include "src/node/init.h"
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
  pg_    = make_unique<PgInstance>(pg_bin(config_), data_dir_, pg_port());

  // Raft first: it knows whether the node needs a backup (an InstallSnapshot replaced its log).
  auto endpoint =
      asio::ip::tcp::endpoint(asio::ip::tcp::v4(), config_.servers.at(INT(id_)).ha_port);
  raft_    = Server::create<RaftServer>(id_,
                                     asio::ip::tcp::acceptor(io_context(), endpoint),
                                     config_,
                                     raft_dir_,
                                     raft_wal_dir_,
                                     options_.raft,
                                     make_shared<Observer>(weak_from_this()));
  tracker_ = make_shared<NodeWalTracker>(raft_);

  if (!fs::exists(data_dir_ / "PG_VERSION") || raft_->applied_state().needs_backup) {
    raft_->set_needs_backup(true);  // no elections until there is something to promote
    if (auto r = restore_latest_backup(); !r)
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

  raft_->set_needs_backup(false);
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
  primary_.store(false);
  {
    auto lk = std::unique_lock{role_mtx_};
    leader_term_.reset();
    role_changed_ = true;
  }
  role_cv_.notify_all();
}

// ── transitions ───────────────────────────────────────────────────────────────

void Node::transitions() {
  while (true) {
    auto lk = std::unique_lock{role_mtx_};
    role_cv_.wait(lk, [this] { return role_changed_ || stopping_; });
    if (stopping_)
      return;
    role_changed_ = false;
    auto term     = leader_term_;
    lk.unlock();

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
  // The hook killed it; reap it.
  pg_->stop(PgInstance::StopMode::Immediate, 5000);
  // TODO(phase 2): cut pg/pg_wal back to the committed WAL (wal_end) and restart PostgreSQL as a
  // standby (docs/node.md §3.3).
  log(INFO, "Node {}: PostgreSQL stopped after losing leadership", id_);
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

  // Raft hands the leadership over (its stepped_down kills a primary).
  if (raft_)
    raft_->shutdown();
  if (pg_ && pg_->is_running())
    pg_->stop(PgInstance::StopMode::Fast);

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
