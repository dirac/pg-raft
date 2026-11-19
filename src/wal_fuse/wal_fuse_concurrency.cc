// Does PostgreSQL issue its WAL writes and fsyncs concurrently? One PostgreSQL with pg_wal on
// WalFuseOps (no raft), pgbench against it, and a WalFuseOps that counts how many WAL writes and
// fsyncs are in flight when each one enters, and which processes issue them.
//
// With the single-threaded FUSE loop (the default, as in the node) the daemon handles one request
// at a time, so nothing can overlap there: --mt runs fuse_loop_mt, one request per thread, which
// shows what PostgreSQL issues. --fsync-delay-us completes each fsync that much later (on an io
// thread), standing in for the raft round a node's fsync waits for.
//
//   wal_fuse_concurrency --dir /data/fuse_conc [--mt] [--fsync-delay-us N] [--clients 50]
//                        [--threads 8] [--scale 100] [--duration 60] [--port 15600]

#include "src/base/io_threads.h"
#include "src/base/logger.h"
#include "src/process/pg_instance.h"
#include "src/process/process.h"
#include "src/wal_fuse/fuse.h"
#include "src/wal_fuse/wal_fuse_ops.h"

#include <boost/asio/steady_timer.hpp>
#include <fstream>

static char const* const kPgBin = "/opt/pg_raft/postgresql/18/bin";

namespace {

// Completes each fsync after |delay_us| on an io thread (0: the default local msync).
struct DelayTracker : WalTracker {
  explicit DelayTracker(unsigned delay_us) : delay_us_(delay_us) {}
  void async_fsync(shared_ptr<WalSegment>            segment,
                   off_t                             offset,
                   size_t                            size,
                   std::function<void(Result<void>)> completion) override {
    if (delay_us_ == 0)
      return WalTracker::async_fsync(std::move(segment), offset, size, std::move(completion));
    auto timer = make_shared<asio::steady_timer>(io_context(), std::chrono::microseconds(delay_us_));
    timer->async_wait([timer, completion = std::move(completion)](auto) { completion(ok()); });
  }
  unsigned delay_us_;
};

string process_title(pid_t pid) {
  std::ifstream f(std::format("/proc/{}/cmdline", pid));
  string        s;
  std::getline(f, s, '\0');
  return s.empty() ? "(gone)" : s;
}

bool is_wal(char const* path) {
  std::string_view name{path};
  if (auto pos = name.rfind('/'); pos != std::string_view::npos)
    name.remove_prefix(pos + 1);
  return name.size() == 24 && name.find_first_not_of("0123456789ABCDEF") == std::string_view::npos;
}

void raise_max(std::atomic<int>& max, int v) {
  for (int m = max.load(); v > m && !max.compare_exchange_weak(m, v);) {
  }
}

struct Stats {
  std::atomic<int>      in_write{0}, in_fsync{0}, max_write{0}, max_fsync{0}, max_both{0};
  std::atomic<uint64_t> writes{0}, fsyncs{0};
  std::atomic<uint64_t> write_while_write{0}, write_while_fsync{0};
  std::atomic<uint64_t> fsync_while_fsync{0}, fsync_while_write{0};
  std::atomic<uint64_t> write_bytes{0};
  std::mutex            mtx;
  std::map<pid_t, std::pair<uint64_t, uint64_t>> by_pid;   // writes, fsyncs
  std::map<pid_t, string>                         titles;   // read when first seen

  void count_pid(bool write);
};

void Stats::count_pid(bool write) {
  auto pid = fuse_get_context()->pid;
  auto lk  = std::unique_lock{mtx};
  auto [pos, added] = by_pid.try_emplace(pid);
  (write ? pos->second.first : pos->second.second)++;
  if (added && !titles.contains(pid))
    titles[pid] = process_title(pid);
}

class CountingOps : public WalFuseOps {
 public:
  CountingOps(WalTracker& tracker, std::filesystem::path dir, Stats& stats)
      : WalFuseOps(tracker, std::move(dir)), stats_(stats) {}

  int op_write(char const*            path,
               char const*            buf,
               size_t                 size,
               off_t                  offset,
               struct fuse_file_info* fi) override {
    if (!is_wal(path))
      return WalFuseOps::op_write(path, buf, size, offset, fi);
    auto& s = stats_;
    int   w = s.in_write.fetch_add(1) + 1;
    int   f = s.in_fsync.load();
    s.writes++;
    s.write_bytes += size;
    if (w > 1)
      s.write_while_write++;
    if (f > 0)
      s.write_while_fsync++;
    raise_max(s.max_write, w);
    raise_max(s.max_both, w + f);
    s.count_pid(true);
    int r = WalFuseOps::op_write(path, buf, size, offset, fi);
    s.in_write--;
    return r;
  }

  int op_fsync(char const* path, int datasync, struct fuse_file_info* fi) override {
    if (!is_wal(path))
      return WalFuseOps::op_fsync(path, datasync, fi);
    auto& s = stats_;
    int   f = s.in_fsync.fetch_add(1) + 1;
    int   w = s.in_write.load();
    s.fsyncs++;
    if (f > 1)
      s.fsync_while_fsync++;
    if (w > 0)
      s.fsync_while_write++;
    raise_max(s.max_fsync, f);
    raise_max(s.max_both, w + f);
    s.count_pid(false);
    int r = WalFuseOps::op_fsync(path, datasync, fi);
    s.in_fsync--;
    return r;
  }

 private:
  Stats& stats_;
};

class Pgbench : public Process {
 public:
  using Process::Process;

 protected:
  void on_stdout_line(std::string_view line) override { print(line); }
  void on_stderr_line(std::string_view line) override { print(line); }

 private:
  static void print(std::string_view line) {
    if (line.starts_with("tps") || line.starts_with("latency") ||
        line.starts_with("number of transactions") || line.starts_with("pgbench:"))
      std::printf("  pgbench: %.*s\n", static_cast<int>(line.size()), line.data());
  }
};

}  // namespace

int main(int argc, char** argv) {
  std::filesystem::path root;
  bool                  mt = false;
  unsigned delay_us = 0, clients = 50, threads = 8, scale = 100, duration = 60;
  uint16_t port = 15600;
  for (int i = 1; i < argc; i++) {
    std::string_view a = argv[i];
    auto             next = [&] { return i + 1 < argc ? std::string{argv[++i]} : std::string{}; };
    if (a == "--dir")
      root = next();
    else if (a == "--mt")
      mt = true;
    else if (a == "--fsync-delay-us")
      delay_us = static_cast<unsigned>(std::stoul(next()));
    else if (a == "--clients")
      clients = static_cast<unsigned>(std::stoul(next()));
    else if (a == "--threads")
      threads = static_cast<unsigned>(std::stoul(next()));
    else if (a == "--scale")
      scale = static_cast<unsigned>(std::stoul(next()));
    else if (a == "--duration")
      duration = static_cast<unsigned>(std::stoul(next()));
    else if (a == "--port")
      port = static_cast<uint16_t>(std::stoul(next()));
    else {
      std::fprintf(stderr, "unknown argument %s\n", argv[i]);
      return 2;
    }
  }
  if (root.empty() || std::filesystem::exists(root)) {
    std::fprintf(stderr, "--dir must name a directory that doesn't exist yet\n");
    return 2;
  }

  logger::set_level(logger::Level::Warn);
  IoThreads io{4};

  std::filesystem::create_directories(root / "raft");
  auto data = root / "data";

  Stats        stats;
  DelayTracker tracker{delay_us};
  // Fuse before PgInstance: PostgreSQL stops before the mount goes.
  Fuse fuse(data / "pg_wal", std::make_unique<CountingOps>(tracker, root / "raft" / "pg_wal", stats));
  fuse.set_multithreaded(mt);
  PgInstance pg(kPgBin, data, port, PgInstance::Role::kPrimary);

  if (!pg.init({"-E", "UTF8", "-A", "trust", "--no-sync"}) || !pg.create_raft_wal(root / "raft") ||
      !pg.write_config({{"shared_buffers", "4GB"},
                        {"max_connections", "200"},
                        {"max_wal_size", "16GB"},
                        {"checkpoint_timeout", "15min"}}) ||
      !pg.write_hba({}) || !fuse.start() || !pg.start() || !pg.wait_ready(port)) {
    std::fprintf(stderr, "setup failed\n");
    return 1;
  }

  auto pgbench = [&](std::vector<std::string> args) {
    std::vector<std::string> all{"-h", "127.0.0.1", "-p", std::to_string(port)};
    all.insert(all.end(), args.begin(), args.end());
    all.push_back("postgres");
    Pgbench p(std::string{kPgBin} + "/pgbench", all);
    return p.start() && p.wait() == 0;
  };

  std::printf("loop=%s fsync_delay=%uus clients=%u threads=%u scale=%u duration=%us\n",
              mt ? "multithreaded" : "single", delay_us, clients, threads, scale, duration);
  if (!pgbench({"-q", "-i", "-s", std::to_string(scale)})) {
    std::fprintf(stderr, "pgbench -i failed\n");
    return 1;
  }

  // Count the measured run only.
  {
    auto lk = std::unique_lock{stats.mtx};
    stats.by_pid.clear();
  }
  for (auto* a : {&stats.writes, &stats.fsyncs, &stats.write_while_write, &stats.write_while_fsync,
                  &stats.fsync_while_fsync, &stats.fsync_while_write, &stats.write_bytes})
    a->store(0);
  for (auto* a : {&stats.max_write, &stats.max_fsync, &stats.max_both})
    a->store(0);

  bool ok = pgbench({"-c", std::to_string(clients), "-j", std::to_string(threads), "-T",
                     std::to_string(duration)});

  auto pct = [](uint64_t a, uint64_t b) { return b ? 100.0 * double(a) / double(b) : 0.0; };
  uint64_t w = stats.writes, f = stats.fsyncs;
  std::printf("WAL writes  %llu (%.0f B avg)   fsyncs %llu   writes per fsync %.2f\n",
              (unsigned long long)w, w ? double(stats.write_bytes) / double(w) : 0.0,
              (unsigned long long)f, f ? double(w) / double(f) : 0.0);
  std::printf("max in flight: writes %d, fsyncs %d, both %d\n", stats.max_write.load(),
              stats.max_fsync.load(), stats.max_both.load());
  std::printf("a write entered while another write was in flight: %llu (%.2f%%)\n",
              (unsigned long long)stats.write_while_write.load(), pct(stats.write_while_write, w));
  std::printf("a write entered while an fsync was in flight:       %llu (%.2f%%)\n",
              (unsigned long long)stats.write_while_fsync.load(), pct(stats.write_while_fsync, w));
  std::printf("an fsync entered while another fsync was in flight: %llu (%.2f%%)\n",
              (unsigned long long)stats.fsync_while_fsync.load(), pct(stats.fsync_while_fsync, f));
  std::printf("an fsync entered while a write was in flight:       %llu (%.2f%%)\n",
              (unsigned long long)stats.fsync_while_write.load(), pct(stats.fsync_while_write, f));
  {
    auto lk = std::unique_lock{stats.mtx};
    std::vector<std::tuple<uint64_t, uint64_t, pid_t>> v;
    for (auto& [pid, c] : stats.by_pid)
      v.emplace_back(c.first + c.second, c.second, pid);
    std::ranges::sort(v, std::greater{});
    std::printf("issuing processes: %zu; the top ones (writes, fsyncs, title):\n", v.size());
    for (size_t i = 0; i < v.size() && i < 8; i++) {
      auto [n, fs, pid] = v[i];
      std::printf("  %8llu %8llu  %s\n", (unsigned long long)(n - fs), (unsigned long long)fs,
                  stats.titles[pid].c_str());
    }
  }

  pg.stop(PgInstance::StopMode::Fast);
  fuse.stop();
  std::filesystem::remove_all(root);
  return ok ? 0 : 1;
}
