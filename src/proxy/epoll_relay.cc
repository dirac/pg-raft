// The epoll engine of the proxy's data path (relay.h), Linux only.
//
// One thread per core. Each thread has its own epoll instance and its own listening socket on every
// proxy port (SO_REUSEPORT: the kernel spreads the connections over the threads), and owns each
// connection it accepts until it closes: no locks, no shared queues, no reference counts and no
// cross-thread wake-ups on the data path. A connection is a Pair: the client socket, the upstream
// socket, and a buffer per direction. epoll is level-triggered: a message costs one read and one
// write. Only when the receiver can't take more does the pair stop reading that side and wait for
// the receiver to drain (EPOLLOUT), which is back-pressure.
//
// Other threads reach a loop through its eventfd: a primary change (the primary port's pairs to
// another node are closed, the waiting clients routed) and stop.

#include <pthread.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include "src/base/logger.h"
#include "src/proxy/proxy.h"
#include "src/proxy/relay.h"
#include "src/proxy/relay_util.h"

namespace {

using Clock = std::chrono::steady_clock;

constexpr size_t kBufSize = 16384;

using relay::tune;
using relay::Upstream;

/// What an epoll event points at.
struct Handler {
  enum class Kind { kListener, kWake, kEnd };
  explicit Handler(Kind k) : kind(k) {}
  Kind kind;
};

struct Listener : Handler {
  Listener(int f, size_t p) : Handler(Kind::kListener), fd(f), port(p) {}
  int    fd;
  size_t port;  // 0: the primary port, 1 + i: servers[i]
};

struct Pair;

/// One side of a pair: its socket, and which events it is registered for.
struct End : Handler {
  explicit End(Pair* p) : Handler(Kind::kEnd), pair(p) {}
  Pair* pair;
  int   fd         = -1;
  bool  registered = false;  // in the epoll set (always with EPOLLRDHUP)
  bool  reading    = false;  // EPOLLIN registered
  bool  writing    = false;  // EPOLLOUT registered
};

/// The bytes read from one side, not yet written to the other: [off, len) of buf.
struct Buffer {
  std::array<char, kBufSize> buf;
  size_t                     off = 0;
  size_t                     len = 0;
  bool                       empty() const { return off == len; }
};

struct Pair {
  enum class State { kHolding, kConnecting, kRelaying, kClosed };

  Pair() : client(this), upstream(this) {}

  End               client;
  End               upstream;
  Buffer            to_upstream;  // client -> upstream
  Buffer            to_client;    // upstream -> client
  State             state   = State::kHolding;
  bool              primary = false;  // came in on the primary port
  unsigned          node    = 0;
  Clock::time_point deadline;  // holding: the failover window; connecting: the connect timeout
};

class EpollRelay;

class Loop {
 public:
  Loop(EpollRelay& relay, unsigned index) : relay_(relay), index_(index) {}
  ~Loop();

  Result<void> init(vector<uint16_t> const& ports, string const& address);
  void         run();
  void         wake();

 private:
  void on_accept(Listener& l);
  void on_end(End& end, uint32_t events);
  void on_wake();

  void route(Pair& p);
  void connect(Pair& p, unsigned node);
  void connected(Pair& p);
  bool pump(Pair& p, End& from, End& to, Buffer& b);
  void flush(Pair& p, End& to, End& from, Buffer& b);
  void close(Pair& p);
  void check_deadlines();

  bool watch(End& e, bool reading, bool writing);  // EPOLL_CTL_ADD / MOD as needed

  EpollRelay&                  relay_;
  unsigned const               index_;
  int                          epfd_ = -1;
  Handler                      wake_handler_{Handler::Kind::kWake};
  int                          wakefd_ = -1;
  vector<unique_ptr<Listener>> listeners_;
  std::unordered_set<Pair*>    pairs_;
  vector<Pair*>                graveyard_;  // closed during this batch of events
  uint64_t                     generation_ = 0;
};

class EpollRelay : public Relay {
 public:
  EpollRelay(Config const& config, ProxyOptions const& options, PrimaryFn primary)
      : config_(config), options_(options), primary_fn_(std::move(primary)) {}
  ~EpollRelay() override { stop(); }

  Result<void> start() override;
  void         stop() override;
  void         primary_changed(std::optional<unsigned> primary) override;
  RelayCounts  counts() const override;

  // For the loops.
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

  std::mutex       upstream_mtx_;  // only resolution takes it
  vector<Upstream> upstreams_;

  std::atomic<uint64_t>               on_primary_{0};
  unique_ptr<std::atomic<uint64_t>[]> on_server_;
};

// ── EpollRelay ────────────────────────────────────────────────────────────────

Upstream* EpollRelay::upstream(unsigned node) {
  auto  lk = std::unique_lock{upstream_mtx_};
  auto& u  = upstreams_.at(node);
  if (u.len == 0 && !relay::resolve(u)) {
    log(DEBUG, "Proxy: unable to resolve {}", u.host);
    return nullptr;
  }
  return &u;
}

void EpollRelay::count(Pair const& p, int delta) {
  auto& c = p.primary ? on_primary_ : on_server_[p.node];
  c.fetch_add(static_cast<uint64_t>(delta), std::memory_order_relaxed);
}

Result<void> EpollRelay::start() {
  for (auto const& s : config_.servers) {
    upstreams_.push_back(Upstream{.host = s.hostname, .port = s.pg_port});
    (void)relay::resolve(upstreams_.back());  // again when connecting, if this fails
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
  for (auto& l : loops_)
    threads_.emplace_back([&l] { l->run(); });
  started_ = true;
  log(INFO, "Proxy: epoll engine, {} threads", n);
  return ok();
}

void EpollRelay::stop() {
  if (stopping_.exchange(true) || !started_)
    return;
  for (auto& l : loops_)
    l->wake();
  for (auto& t : threads_)
    t.join();
  threads_.clear();
  loops_.clear();
}

void EpollRelay::primary_changed(std::optional<unsigned>) {
  generation_.fetch_add(1);
  for (auto& l : loops_)
    l->wake();
}

RelayCounts EpollRelay::counts() const {
  auto r = RelayCounts{.primary = on_primary_.load(), .servers = {}};
  for (size_t i = 0; i < config_.servers.size(); ++i)
    r.servers.push_back(on_server_ ? on_server_[i].load() : 0);
  return r;
}

// ── Loop ──────────────────────────────────────────────────────────────────────

Loop::~Loop() {
  for (auto* p : pairs_) {
    if (p->client.fd >= 0)
      ::close(p->client.fd);
    if (p->upstream.fd >= 0)
      ::close(p->upstream.fd);
    delete p;
  }
  for (auto& l : listeners_)
    ::close(l->fd);
  if (wakefd_ >= 0)
    ::close(wakefd_);
  if (epfd_ >= 0)
    ::close(epfd_);
}

Result<void> Loop::init(vector<uint16_t> const& ports, string const& address) {
  epfd_   = ::epoll_create1(EPOLL_CLOEXEC);
  wakefd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (epfd_ < 0 || wakefd_ < 0)
    return elog(system_error_code(errno), "Proxy: epoll_create1 / eventfd");
  epoll_event ev{.events = EPOLLIN, .data = {.ptr = &wake_handler_}};
  ::epoll_ctl(epfd_, EPOLL_CTL_ADD, wakefd_, &ev);

  for (size_t i = 0; i < ports.size(); ++i) {
    auto fd = relay::listen_socket(address, ports[i], /*nonblocking=*/true);
    if (!fd)
      return fd.error();
    listeners_.push_back(make_unique<Listener>(*fd, i));
    epoll_event lev{.events = EPOLLIN, .data = {.ptr = listeners_.back().get()}};
    ::epoll_ctl(epfd_, EPOLL_CTL_ADD, *fd, &lev);
  }
  return ok();
}

void Loop::wake() {
  uint64_t              one = 1;
  [[maybe_unused]] auto n   = ::write(wakefd_, &one, sizeof(one));
}

void Loop::run() {
  auto name = std::format("proxy-io-{}", index_);
  ::pthread_setname_np(::pthread_self(), name.substr(0, 15).c_str());

  std::array<epoll_event, 128> events;
  while (!relay_.stopping()) {
    // Clients waiting for a primary, and connects, are checked every 100 ms.
    bool const timed = std::ranges::any_of(pairs_, [](Pair const* p) {
      return p->state == Pair::State::kHolding || p->state == Pair::State::kConnecting;
    });
    // Busy-poll first (spin_us): the next message is often there within microseconds, and is then
    // taken without the thread sleeping and being woken.
    int n = 0;
    if (auto const spin = relay_.options().spin_us; spin > 0) {
      auto const end = Clock::now() + std::chrono::microseconds{spin};
      do
        n = ::epoll_wait(epfd_, events.data(), static_cast<int>(events.size()), 0);
      while (n == 0 && Clock::now() < end);
    }
    if (n == 0)
      n = ::epoll_wait(epfd_, events.data(), static_cast<int>(events.size()), timed ? 100 : -1);
    if (n < 0 && errno != EINTR) {
      elog(system_error_code(errno), "Proxy: epoll_wait");
      return;
    }
    for (int i = 0; i < n; ++i) {
      auto* h = static_cast<Handler*>(events[static_cast<size_t>(i)].data.ptr);
      switch (h->kind) {
        case Handler::Kind::kListener:
          on_accept(*static_cast<Listener*>(h));
          break;
        case Handler::Kind::kWake:
          on_wake();
          break;
        case Handler::Kind::kEnd:
          on_end(*static_cast<End*>(h), events[static_cast<size_t>(i)].events);
          break;
      }
    }
    if (timed)
      check_deadlines();
    // Pairs closed in this batch: no event of theirs can be pending any more.
    for (auto* p : graveyard_)
      delete p;
    graveyard_.clear();
  }
}

void Loop::on_wake() {
  uint64_t              v;
  [[maybe_unused]] auto r = ::read(wakefd_, &v, sizeof(v));
  if (relay_.stopping() || relay_.generation() == generation_)
    return;
  generation_ = relay_.generation();

  // The primary changed: its old clients go (they reconnect and reach the new one), the waiting
  // ones are routed.
  auto const primary = relay_.primary();
  for (auto* p : vector<Pair*>(pairs_.begin(), pairs_.end())) {
    if (!p->primary)
      continue;
    if (p->state == Pair::State::kHolding)
      route(*p);
    else if (std::optional<unsigned>{p->node} != primary)
      close(*p);
  }
}

void Loop::on_accept(Listener& l) {
  for (int i = 0; i < 64; ++i) {
    int fd = ::accept4(l.fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd < 0)
      return;  // EAGAIN, or another thread took it, or an error that the next accept reports
    tune(fd);

    auto* p      = new Pair;
    p->client.fd = fd;
    p->primary   = l.port == 0;
    pairs_.insert(p);
    // Until it has an upstream, only the client's hang-up matters (its startup packet waits).
    watch(p->client, false, false);
    if (p->primary) {
      p->deadline = Clock::now() + relay_.options().failover_window;
      route(*p);
    } else {
      connect(*p, static_cast<unsigned>(l.port - 1));
    }
  }
}

void Loop::route(Pair& p) {
  if (auto node = relay_.primary(); node)
    return connect(p, *node);
  p.state = Pair::State::kHolding;  // until a primary, or the deadline
}

void Loop::connect(Pair& p, unsigned node) {
  p.node   = node;
  auto* u  = relay_.upstream(node);
  int   fd = u ? ::socket(u->addr.ss_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0) : -1;
  if (fd < 0)
    return close(p);
  tune(fd);
  p.upstream.fd = fd;
  relay_.count(p, 1);
  p.state    = Pair::State::kConnecting;
  p.deadline = Clock::now() + relay_.options().connect_timeout;
  if (::connect(fd, reinterpret_cast<sockaddr const*>(&u->addr), u->len) == 0)
    return connected(p);
  if (errno != EINPROGRESS) {
    log(DEBUG, "Proxy: unable to reach server {}: {}", node, strerror(errno));
    return close(p);
  }
  watch(p.upstream, false, true);  // writable: connected (or failed)
}

void Loop::connected(Pair& p) {
  int       err = 0;
  socklen_t len = sizeof(err);
  ::getsockopt(p.upstream.fd, SOL_SOCKET, SO_ERROR, &err, &len);
  if (err != 0) {
    log(DEBUG, "Proxy: unable to reach server {}: {}", p.node, strerror(err));
    return close(p);
  }
  // A primary that changed while connecting.
  if (p.primary && relay_.primary() != std::optional<unsigned>{p.node})
    return close(p);
  p.state = Pair::State::kRelaying;
  watch(p.client, true, false);
  watch(p.upstream, true, false);
}

void Loop::on_end(End& end, uint32_t events) {
  auto& p = *end.pair;
  if (p.state == Pair::State::kClosed)
    return;
  bool const up = &end == &p.upstream;

  if (p.state == Pair::State::kConnecting && up) {
    if (events & (EPOLLOUT | EPOLLERR | EPOLLHUP))
      connected(p);
    return;
  }
  if (p.state != Pair::State::kRelaying) {
    // Holding, or connecting and the client's side: only a hang-up (or an error) matters.
    if (events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR))
      close(p);
    return;
  }

  End&    other  = up ? p.client : p.upstream;
  Buffer& out    = up ? p.to_client : p.to_upstream;  // what this side sent
  Buffer& in     = up ? p.to_upstream : p.to_client;  // what this side is to receive
  bool    closed = false;

  // Writable: what was waiting for this side goes first.
  if (events & EPOLLOUT)
    flush(p, end, other, in);
  if (p.state == Pair::State::kClosed)
    return;
  // Readable, or hung up with data still to read: forward it.
  if (events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR))
    closed = !pump(p, end, other, out);
  if (closed && p.state != Pair::State::kClosed)
    close(p);
}

/// from -> to: read what from has (one read for a small message) and write it to |to|. False when
/// the pair must close (EOF or an error). When |to| can't take it all, the rest waits in |b|: stop
/// reading |from|, and wait for |to| to be writable.
bool Loop::pump(Pair& p, End& from, End& to, Buffer& b) {
  (void)p;
  while (b.empty()) {
    ssize_t r = ::recv(from.fd, b.buf.data(), b.buf.size(), 0);
    if (r == 0)
      return false;  // EOF: the pair closes, as one side has gone
    if (r < 0)
      return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR;
    b.off = 0;
    b.len = static_cast<size_t>(r);

    while (!b.empty()) {
      ssize_t w = ::send(to.fd, b.buf.data() + b.off, b.len - b.off, MSG_NOSIGNAL);
      if (w < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          // Back-pressure: stop reading |from| until |to| drains.
          return watch(from, false, from.writing) && watch(to, to.reading, true);
        }
        if (errno == EINTR)
          continue;
        return false;
      }
      b.off += static_cast<size_t>(w);
    }
    b.off = b.len = 0;
    // A short read: level-triggered epoll reports any more; no extra read for nothing.
    if (static_cast<size_t>(r) < b.buf.size())
      return true;
  }
  return true;
}

/// |to| is writable: write what waits for it; once all of it is written, read |from| again.
void Loop::flush(Pair& p, End& to, End& from, Buffer& b) {
  while (!b.empty()) {
    ssize_t w = ::send(to.fd, b.buf.data() + b.off, b.len - b.off, MSG_NOSIGNAL);
    if (w < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        return;
      if (errno == EINTR)
        continue;
      return close(p);
    }
    b.off += static_cast<size_t>(w);
  }
  b.off = b.len = 0;
  if (!watch(to, to.reading, false) || !watch(from, true, from.writing))
    close(p);
}

bool Loop::watch(End& e, bool reading, bool writing) {
  if (e.registered && e.reading == reading && e.writing == writing)
    return true;
  epoll_event ev{.events = EPOLLRDHUP | (reading ? EPOLLIN : 0U) | (writing ? EPOLLOUT : 0U),
                 .data   = {.ptr = &e}};
  if (::epoll_ctl(epfd_, e.registered ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, e.fd, &ev) != 0)
    return false;
  e.registered = true;
  e.reading    = reading;
  e.writing    = writing;
  return true;
}

void Loop::close(Pair& p) {
  if (p.state == Pair::State::kClosed)
    return;
  if (p.upstream.fd >= 0)
    relay_.count(p, -1);
  p.state = Pair::State::kClosed;
  for (auto* e : {&p.client, &p.upstream})
    if (e->fd >= 0) {
      ::close(e->fd);  // also leaves the epoll set
      e->fd = -1;
    }
  pairs_.erase(&p);
  graveyard_.push_back(&p);
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
      close(*p);
    }
  }
}

}  // namespace

unique_ptr<Relay> make_epoll_relay(Config const&       config,
                                   ProxyOptions const& options,
                                   Relay::PrimaryFn    primary) {
  return make_unique<EpollRelay>(config, options, std::move(primary));
}
