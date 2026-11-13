// The io_uring engine of the proxy's data path (relay.h), Linux only.
//
// The same shape as the epoll engine (epoll_relay.cc): one thread per core, each with its own ring
// and its own SO_REUSEPORT listeners, owning its connections from accept to close. The I/O is
// io_uring's: each listener has a multishot accept, and each direction of a pair has one receive in
// flight; its completion queues a send of the bytes, and the send's completion queues the next
// receive (back-pressure, as with one read at a time). Everything queued while handling a batch of
// completions goes to the kernel with the wait for the next batch, in one io_uring_enter: no read,
// write or epoll_wait per message. A ring is set up for a single issuer with deferred task work, so
// completions are reaped only when the thread waits for them.
//
// A pair that closes cancels its requests in flight, and is freed once their completions have all
// arrived (the kernel holds the buffers until then). Other threads reach a loop through its
// eventfd, read by a request of the ring: a primary change, and stop.

#include <liburing.h>
#include <pthread.h>
#include <sys/eventfd.h>

#include "src/base/logger.h"
#include "src/proxy/proxy.h"
#include "src/proxy/relay.h"
#include "src/proxy/relay_util.h"

namespace {

using Clock = std::chrono::steady_clock;
using relay::Upstream;

constexpr size_t   kBufSize     = 16384;
constexpr unsigned kRingEntries = 4096;

struct Pair;
struct Listener;

/// A request in the ring: its completion's user_data points at it.
struct Op {
  enum class Kind { kAccept, kWake, kConnect, kRecv, kSend, kCancel };
  Kind      kind;
  Pair*     pair     = nullptr;
  Listener* listener = nullptr;
  bool      upstream = false;  // kRecv / kSend: received from (sent to) the upstream side
};

struct Listener {
  int    fd   = -1;
  size_t port = 0;  // 0: the primary port, 1 + i: servers[i]
  Op     accept{.kind = Op::Kind::kAccept};
};

/// The bytes received from one side, being sent to the other: [off, len) of buf.
struct Buffer {
  std::array<char, kBufSize> buf;
  size_t                     off = 0;
  size_t                     len = 0;
};

struct Pair {
  enum class State { kHolding, kConnecting, kRelaying, kClosed };

  int               client   = -1;
  int               upstream = -1;
  Buffer            to_upstream;  // client -> upstream
  Buffer            to_client;    // upstream -> client
  State             state    = State::kHolding;
  bool              primary  = false;
  bool              counted  = false;
  unsigned          node     = 0;
  unsigned          inflight = 0;  // requests in the ring
  Clock::time_point deadline;

  Op connect{.kind = Op::Kind::kConnect};
  Op recv_client{.kind = Op::Kind::kRecv};
  Op recv_upstream{.kind = Op::Kind::kRecv, .upstream = true};
  Op send_client{.kind = Op::Kind::kSend};
  Op send_upstream{.kind = Op::Kind::kSend, .upstream = true};
  Op cancel{.kind = Op::Kind::kCancel};
};

class UringRelay;

class Loop {
 public:
  Loop(UringRelay& relay, unsigned index) : relay_(relay), index_(index) {}
  ~Loop();

  Result<void> init(vector<uint16_t> const& ports, string const& address);
  void         run();
  void         wake();

 private:
  io_uring_sqe* sqe();
  void          arm_accept(Listener& l);
  void          arm_wake();
  void          recv(Pair& p, bool from_upstream);
  void          send(Pair& p, bool to_upstream);

  void on_cqe(io_uring_cqe const& cqe);
  void on_accept(Listener& l, int fd, bool more);
  void on_wake();
  void route(Pair& p);
  void connect(Pair& p, unsigned node);
  void close(Pair& p);
  void done(Pair& p);  // a request of the pair completed
  void check_deadlines();

  UringRelay&                  relay_;
  unsigned const               index_;
  io_uring                     ring_{};
  bool                         ring_ok_    = false;
  bool                         disabled_   = false;  // created disabled: enabled by run()
  int                          wakefd_     = -1;
  uint64_t                     wake_value_ = 0;
  Op                           wake_op_{.kind = Op::Kind::kWake};
  vector<unique_ptr<Listener>> listeners_;
  std::unordered_set<Pair*>    pairs_;
  uint64_t                     generation_ = 0;
};

class UringRelay : public Relay {
 public:
  UringRelay(Config const& config, ProxyOptions const& options, PrimaryFn primary)
      : config_(config), options_(options), primary_fn_(std::move(primary)) {}
  ~UringRelay() override { stop(); }

  Result<void> start() override;
  void         stop() override;
  void         primary_changed(std::optional<unsigned>) override;
  RelayCounts  counts() const override;

  std::optional<unsigned> primary() const { return primary_fn_(); }
  uint64_t                generation() const { return generation_.load(); }
  bool                    stopping() const { return stopping_.load(); }
  ProxyOptions const&     options() const { return options_; }
  Upstream*               upstream(unsigned node);
  void                    count(Pair const& p, int delta);

 private:
  Config const&            config_;
  ProxyOptions const&      options_;
  PrimaryFn                primary_fn_;
  vector<unique_ptr<Loop>> loops_;
  vector<std::thread>      threads_;
  std::atomic<uint64_t>    generation_{0};
  std::atomic_bool         stopping_{false};
  bool                     started_ = false;

  std::mutex       upstream_mtx_;
  vector<Upstream> upstreams_;

  std::atomic<uint64_t>               on_primary_{0};
  unique_ptr<std::atomic<uint64_t>[]> on_server_;
};

// ── UringRelay ────────────────────────────────────────────────────────────────

Upstream* UringRelay::upstream(unsigned node) {
  auto  lk = std::unique_lock{upstream_mtx_};
  auto& u  = upstreams_.at(node);
  if (u.len == 0 && !relay::resolve(u)) {
    log(DEBUG, "Proxy: unable to resolve {}", u.host);
    return nullptr;
  }
  return &u;
}

void UringRelay::count(Pair const& p, int delta) {
  auto& c = p.primary ? on_primary_ : on_server_[p.node];
  c.fetch_add(static_cast<uint64_t>(delta), std::memory_order_relaxed);
}

Result<void> UringRelay::start() {
  for (auto const& s : config_.servers) {
    upstreams_.push_back(Upstream{.host = s.hostname, .port = s.pg_port});
    (void)relay::resolve(upstreams_.back());
  }
  on_server_ = std::make_unique<std::atomic<uint64_t>[]>(config_.servers.size());

  vector<uint16_t> ports;
  for (size_t i = 0; i <= config_.servers.size(); ++i)
    ports.push_back(static_cast<uint16_t>(config_.proxy_pg_port + i));

  auto n = options_.threads ? options_.threads : std::max(1U, std::thread::hardware_concurrency());
  for (unsigned i = 0; i < n; ++i) {
    loops_.push_back(make_unique<Loop>(*this, i));
    if (auto r = loops_.back()->init(ports, options_.listen_address); !r) {
      loops_.clear();
      return r;
    }
  }
  // A ring with IORING_SETUP_SINGLE_ISSUER belongs to the thread that submits first: the loops'
  // threads (init() only prepares it).
  for (auto& l : loops_)
    threads_.emplace_back([&l] { l->run(); });
  started_ = true;
  log(INFO, "Proxy: io_uring engine, {} threads", n);
  return ok();
}

void UringRelay::stop() {
  if (stopping_.exchange(true) || !started_)
    return;
  for (auto& l : loops_)
    l->wake();
  for (auto& t : threads_)
    t.join();
  threads_.clear();
  loops_.clear();
}

void UringRelay::primary_changed(std::optional<unsigned>) {
  generation_.fetch_add(1);
  for (auto& l : loops_)
    l->wake();
}

RelayCounts UringRelay::counts() const {
  auto r = RelayCounts{.primary = on_primary_.load(), .servers = {}};
  for (size_t i = 0; i < config_.servers.size(); ++i)
    r.servers.push_back(on_server_ ? on_server_[i].load() : 0);
  return r;
}

// ── Loop ──────────────────────────────────────────────────────────────────────

Loop::~Loop() {
  for (auto* p : pairs_) {
    if (p->client >= 0)
      ::close(p->client);
    if (p->upstream >= 0)
      ::close(p->upstream);
    delete p;
  }
  for (auto& l : listeners_)
    ::close(l->fd);
  if (wakefd_ >= 0)
    ::close(wakefd_);
  if (ring_ok_)
    ::io_uring_queue_exit(&ring_);
}

Result<void> Loop::init(vector<uint16_t> const& ports, string const& address) {
  io_uring_params params{};
  // Single issuer: the loop's thread, which enables the ring (created disabled here, on the
  // starting thread, so that an error is reported by start()).
  params.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_R_DISABLED;
  if (int rc = ::io_uring_queue_init_params(kRingEntries, &ring_, &params); rc < 0) {
    // An older kernel: without the flags.
    params    = {};
    disabled_ = false;
    if (rc = ::io_uring_queue_init_params(kRingEntries, &ring_, &params); rc < 0)
      return elog(system_error_code(-rc), "Proxy: io_uring_queue_init");
  }
  ring_ok_  = true;
  disabled_ = (params.flags & IORING_SETUP_R_DISABLED) != 0;
  wakefd_   = ::eventfd(0, EFD_CLOEXEC);
  if (wakefd_ < 0)
    return elog(system_error_code(errno), "Proxy: eventfd");

  for (size_t i = 0; i < ports.size(); ++i) {
    auto fd = relay::listen_socket(address, ports[i], /*nonblocking=*/false);
    if (!fd)
      return fd.error();
    auto l             = make_unique<Listener>();
    l->fd              = *fd;
    l->port            = i;
    l->accept.listener = l.get();
    listeners_.push_back(std::move(l));
  }
  return ok();
}

void Loop::wake() {
  uint64_t              one = 1;
  [[maybe_unused]] auto n   = ::write(wakefd_, &one, sizeof(one));
}

io_uring_sqe* Loop::sqe() {
  auto* s = ::io_uring_get_sqe(&ring_);
  if (!s) {
    // The submission queue is full: hand it to the kernel, then there is room.
    ::io_uring_submit(&ring_);
    s = ::io_uring_get_sqe(&ring_);
  }
  return s;
}

void Loop::arm_accept(Listener& l) {
  auto* s = sqe();
  ::io_uring_prep_multishot_accept(s, l.fd, nullptr, nullptr, SOCK_CLOEXEC);
  ::io_uring_sqe_set_data(s, &l.accept);
}

void Loop::arm_wake() {
  auto* s = sqe();
  ::io_uring_prep_read(s, wakefd_, &wake_value_, sizeof(wake_value_), 0);
  ::io_uring_sqe_set_data(s, &wake_op_);
}

void Loop::recv(Pair& p, bool from_upstream) {
  auto& b = from_upstream ? p.to_client : p.to_upstream;
  auto* s = sqe();
  ::io_uring_prep_recv(s, from_upstream ? p.upstream : p.client, b.buf.data(), b.buf.size(), 0);
  ::io_uring_sqe_set_data(s, from_upstream ? &p.recv_upstream : &p.recv_client);
  ++p.inflight;
}

void Loop::send(Pair& p, bool to_upstream) {
  auto& b = to_upstream ? p.to_upstream : p.to_client;
  auto* s = sqe();
  ::io_uring_prep_send(
      s, to_upstream ? p.upstream : p.client, b.buf.data() + b.off, b.len - b.off, MSG_NOSIGNAL);
  ::io_uring_sqe_set_data(s, to_upstream ? &p.send_upstream : &p.send_client);
  ++p.inflight;
}

void Loop::run() {
  auto name = std::format("proxy-io-{}", index_);
  ::pthread_setname_np(::pthread_self(), name.substr(0, 15).c_str());

  // The ring was created disabled on the starting thread: enabling it here makes this thread its
  // single issuer.
  if (disabled_) {
    if (int rc = ::io_uring_enable_rings(&ring_); rc < 0) {
      elog(system_error_code(-rc), "Proxy: io_uring_enable_rings");
      return;
    }
  }
  for (auto& l : listeners_)
    arm_accept(*l);
  arm_wake();

  while (!relay_.stopping()) {
    // Clients waiting for a primary, and connects, are checked every 100 ms.
    bool const        timed = std::ranges::any_of(pairs_, [](Pair const* p) {
      return p->state == Pair::State::kHolding || p->state == Pair::State::kConnecting;
    });
    io_uring_cqe*     cqe   = nullptr;
    __kernel_timespec ts{.tv_sec = 0, .tv_nsec = 100'000'000};
    // Busy-poll first (spin_us): submit, then reap without sleeping for a while (with deferred task
    // work the completions are run by io_uring_get_events).
    if (auto const spin = relay_.options().spin_us; spin > 0) {
      ::io_uring_submit(&ring_);
      auto const end = Clock::now() + std::chrono::microseconds{spin};
      while (::io_uring_peek_cqe(&ring_, &cqe) != 0 && Clock::now() < end)
        ::io_uring_get_events(&ring_);
    }
    int rc = ::io_uring_submit_and_wait_timeout(&ring_, &cqe, 1, timed ? &ts : nullptr, nullptr);
    if (rc < 0 && rc != -ETIME && rc != -EINTR) {
      elog(system_error_code(-rc), "Proxy: io_uring_submit_and_wait");
      return;
    }

    unsigned head;
    unsigned n = 0;
    io_uring_for_each_cqe(&ring_, head, cqe) {
      on_cqe(*cqe);
      ++n;
    }
    ::io_uring_cq_advance(&ring_, n);
    if (timed)
      check_deadlines();
  }
}

void Loop::on_cqe(io_uring_cqe const& cqe) {
  auto* op = static_cast<Op*>(::io_uring_cqe_get_data(&cqe));
  if (!op)
    return;
  switch (op->kind) {
    case Op::Kind::kAccept:
      return on_accept(*op->listener, cqe.res, (cqe.flags & IORING_CQE_F_MORE) != 0);
    case Op::Kind::kWake:
      on_wake();
      return arm_wake();
    default:
      break;
  }

  auto& p = *op->pair;
  --p.inflight;
  if (p.state == Pair::State::kClosed)
    return done(p);

  switch (op->kind) {
    case Op::Kind::kConnect:
      if (cqe.res < 0) {
        log(DEBUG, "Proxy: unable to reach server {}: {}", p.node, strerror(-cqe.res));
        return close(p);
      }
      if (p.primary && relay_.primary() != std::optional<unsigned>{p.node})
        return close(p);  // the primary changed while connecting
      p.state = Pair::State::kRelaying;
      recv(p, false);
      recv(p, true);
      return;

    case Op::Kind::kRecv: {
      if (cqe.res <= 0)
        return close(p);  // EOF or an error: one side has gone
      auto& b = op->upstream ? p.to_client : p.to_upstream;
      b.off   = 0;
      b.len   = static_cast<size_t>(cqe.res);
      return send(p, !op->upstream);
    }

    case Op::Kind::kSend: {
      if (cqe.res < 0)
        return close(p);
      auto& b = op->upstream ? p.to_upstream : p.to_client;
      b.off += static_cast<size_t>(cqe.res);
      if (b.off < b.len)
        return send(p, op->upstream);  // the rest
      b.off = b.len = 0;
      return recv(p, !op->upstream);  // the side that sent it: the next message
    }

    default:
      return;
  }
}

void Loop::on_accept(Listener& l, int fd, bool more) {
  if (!more && !relay_.stopping())
    arm_accept(l);  // the multishot accept ended: arm it again
  if (fd < 0)
    return;
  relay::tune(fd);

  auto* p    = new Pair;
  p->client  = fd;
  p->primary = l.port == 0;
  for (auto* op : {&p->connect,
                   &p->recv_client,
                   &p->recv_upstream,
                   &p->send_client,
                   &p->send_upstream,
                   &p->cancel})
    op->pair = p;
  pairs_.insert(p);
  if (p->primary) {
    p->deadline = Clock::now() + relay_.options().failover_window;
    route(*p);
  } else {
    connect(*p, static_cast<unsigned>(l.port - 1));
  }
}

void Loop::on_wake() {
  if (relay_.stopping() || relay_.generation() == generation_)
    return;
  generation_ = relay_.generation();

  auto const primary = relay_.primary();
  for (auto* p : vector<Pair*>(pairs_.begin(), pairs_.end())) {
    if (!p->primary || p->state == Pair::State::kClosed)
      continue;
    if (p->state == Pair::State::kHolding)
      route(*p);
    else if (std::optional<unsigned>{p->node} != primary)
      close(*p);
  }
}

void Loop::route(Pair& p) {
  if (auto node = relay_.primary(); node)
    return connect(p, *node);
  p.state = Pair::State::kHolding;
}

void Loop::connect(Pair& p, unsigned node) {
  p.node   = node;
  auto* u  = relay_.upstream(node);
  int   fd = u ? ::socket(u->addr.ss_family, SOCK_STREAM | SOCK_CLOEXEC, 0) : -1;
  if (fd < 0)
    return close(p);
  relay::tune(fd);
  p.upstream = fd;
  p.counted  = true;
  relay_.count(p, 1);
  p.state    = Pair::State::kConnecting;
  p.deadline = Clock::now() + relay_.options().connect_timeout;

  auto* s = sqe();
  ::io_uring_prep_connect(s, fd, reinterpret_cast<sockaddr const*>(&u->addr), u->len);
  ::io_uring_sqe_set_data(s, &p.connect);
  ++p.inflight;
}

void Loop::close(Pair& p) {
  if (p.state == Pair::State::kClosed)
    return;
  if (p.counted)
    relay_.count(p, -1);
  p.state = Pair::State::kClosed;
  // Cancel what is in flight on its sockets; the pair goes once every completion has arrived.
  for (int fd : {p.client, p.upstream}) {
    if (fd < 0 || p.inflight == 0)
      continue;
    auto* s = sqe();
    ::io_uring_prep_cancel_fd(s, fd, IORING_ASYNC_CANCEL_ALL);
    ::io_uring_sqe_set_data(s, nullptr);  // its own completion is ignored
  }
  done(p);
}

void Loop::done(Pair& p) {
  if (p.state != Pair::State::kClosed || p.inflight > 0)
    return;
  if (p.client >= 0)
    ::close(p.client);
  if (p.upstream >= 0)
    ::close(p.upstream);
  pairs_.erase(&p);
  delete &p;
}

void Loop::check_deadlines() {
  auto const now = Clock::now();
  for (auto* p : vector<Pair*>(pairs_.begin(), pairs_.end())) {
    if (p->state == Pair::State::kHolding) {
      if (now >= p->deadline) {
        log(INFO, "Proxy: no primary within the failover window: closing a client");
        close(*p);
      } else {
        route(*p);
      }
    } else if (p->state == Pair::State::kConnecting && now >= p->deadline) {
      log(DEBUG, "Proxy: connect to server {} timed out", p->node);
      close(*p);  // cancels the connect
    }
  }
}

}  // namespace

unique_ptr<Relay> make_uring_relay(Config const&       config,
                                   ProxyOptions const& options,
                                   Relay::PrimaryFn    primary) {
  return make_unique<UringRelay>(config, options, std::move(primary));
}
