# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

**pg-raft** is a high-availability system for PostgreSQL that replicates the WAL stream itself through Raft. The current code implements:

1. **WAL interception** (`wal_fuse`) — a FUSE filesystem that mounts over `pg_wal` so that all WAL writes are transparently redirected to a separate `raft_dir/pg_wal` directory under pg-raft's control.
2. **WAL streaming** (`wal_sender`) — a C++ server that speaks the PostgreSQL physical streaming replication protocol, streaming WAL from disk to a real PostgreSQL standby.
3. **Networking** (`net`), **process control** (`process`) and a **PostgreSQL client** (`status`).
4. **Raft** (`raft`) — consensus over the WAL stream, the **node** process (`node`), and the client **proxy** (`proxy`).

Per-component design documents are in `docs/` (one per module, plus `README.md`).

## Build system

This project uses [Boost.Build (b2)](https://www.boost.org/build/) — not CMake or Make. The `b2` binary is at `/opt/pg_raft/bin/b2`. All external dependencies (Boost, OpenSSL, lmdb, fuse) are installed under `/opt/pg_raft/` and `/usr/local/` (see `Jamroot.jam` for exact search paths).

All `b2` commands must be run from the repo root (the directory containing `Jamroot.jam`).

```bash
# Build everything
/opt/pg_raft/bin/b2

# Build + run all tests
/opt/pg_raft/bin/b2 test

# Build only a module
/opt/pg_raft/bin/b2 src/process
/opt/pg_raft/bin/b2 src/wal_sender
/opt/pg_raft/bin/b2 src/wal_fuse
/opt/pg_raft/bin/b2 src/net
/opt/pg_raft/bin/b2 src/status
/opt/pg_raft/bin/b2 src/raft
/opt/pg_raft/bin/b2 src/backup
/opt/pg_raft/bin/b2 src/node

# Build + run a specific unit-test target (auto-runs after build)
/opt/pg_raft/bin/b2 src/base//semaphore_lock_test
/opt/pg_raft/bin/b2 src/base//xxhash_test
/opt/pg_raft/bin/b2 src/process//process_test
/opt/pg_raft/bin/b2 src/process//pg_instance_test
/opt/pg_raft/bin/b2 src/wal_sender//wal_sender_test
/opt/pg_raft/bin/b2 src/wal_sender//pg_wal_switch_test
/opt/pg_raft/bin/b2 src/wal_sender//standby_wal_reset_test
/opt/pg_raft/bin/b2 src/wal_sender//timeline_switch_test
/opt/pg_raft/bin/b2 src/wal_fuse//fuse_test
/opt/pg_raft/bin/b2 src/wal_fuse//wal_fuse_ops_test
/opt/pg_raft/bin/b2 src/wal_fuse//wal_state_test
/opt/pg_raft/bin/b2 src/wal_fuse//wal_shadow_test
/opt/pg_raft/bin/b2 src/net//async_connect_test
/opt/pg_raft/bin/b2 src/net//data_test
/opt/pg_raft/bin/b2 src/net//connection_test
/opt/pg_raft/bin/b2 src/status//pg_client_test
/opt/pg_raft/bin/b2 src/raft//raft_log_test
/opt/pg_raft/bin/b2 src/backup//backup_store_test
/opt/pg_raft/bin/b2 src/node//init_test
/opt/pg_raft/bin/b2 src/node//node_wal_tracker_test
/opt/pg_raft/bin/b2 src/node//node_test
/opt/pg_raft/bin/b2 src/node//node_backup_test
/opt/pg_raft/bin/b2 src/node//node_recovery_test
/opt/pg_raft/bin/b2 src/proxy//proxy_test
/opt/pg_raft/bin/b2 src/raft//raft_ensemble_sanity_test   # also _leader_stopped_, _stepdown_, _snapshot_, _needs_backup_, _keep_from_
/opt/pg_raft/bin/b2 src/base//config_test

# Run a single Boost.Test case within a unit-test binary (pass args after --):
/opt/pg_raft/bin/b2 src/process//process_test -- --run_test=TestCaseName
# Or run the binary directly for full Boost.Test flags (--log_level=all, etc.):
./src/process/bin/clang-darwin-17/debug/cxxstd-23-iso/link-static/visibility-hidden/process_test --run_test=TestCaseName

# Build with sanitizers
/opt/pg_raft/bin/b2 address-sanitizer=norecover
/opt/pg_raft/bin/b2 thread-sanitizer=norecover
```

Build artifacts land in `src/<module>/bin/clang-darwin-17/debug/cxxstd-23-iso/link-static/visibility-hidden/` (sanitizer builds add a `thread-sanitizer-norecover/` or `address-sanitizer-norecover/` path component).

Compiler flags (from `Jamroot.jam`):
- `-Wall -Werror` always on; `-Wextra -Wconversion -Wuninitialized` in debug builds (so sign conversions, e.g. an `int` initializer list iterated as `size_t`, are errors)
- Static linking, C++23 (`<cxxstd>23`), hidden visibility, debug symbols always on
- `-Wno-tsan` is passed only for the gcc toolset (Apple clang rejects it)
- Apple clang 17's libc++ has no `std::move_only_function`; use `std::function`
- `BOOST_ASIO_NO_DEPRECATED=1` — use only current Boost.Asio APIs

Include paths: the repo root (`.`) is on the include path, so headers are included as `#include "src/module/file.h"`.

## Environment setup

Dependencies (Boost, OpenSSL, LMDB, PostgreSQL, FUSE, etc.) are installed under `/opt/pg_raft/`. To build or rebuild them from source, run the scripts in `external/`:

```bash
./external/install.sh          # install all dependencies
./external/install-boost.sh    # or individually
```

## Code formatting and tooling

`src/.clang-format` configures clang-format (Chromium-based style, 100-column limit, 2-space indent, pointer alignment left). Run `clang-format -i <file>` to format a file in place.

`compile_flags.txt` at the repo root is for clang-tidy and IDE tooling (sets include paths and defines). It is not used by b2.

## Architecture

### Modules

**`src/base/`** — Foundational utilities used by all other modules:

- **`logger`** — Thread-safe, `std::format`-based logging to stderr. Levels: `Debug`, `Info`, `Warn`, `Error`, `Fatal`. Default level is `Info`. Call `logger::set_level()` to adjust. Only the short prefix of a line goes through `std::format`; the message is appended (formatting a long message overflowed a stack buffer in libc++'s `vformat_to`). Use `log(LEVEL, fmt, args...)` globally (constants `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL` are in scope via the header). `logger::elog(ec, fmt, args...)` logs an `std::error_code` at `Error` level and returns the code. `logger::plog(prefix, line)` logs a line prefixed with a process name tag.
- **`Result<T>`** — `std::expected<T, std::error_code>` wrapper with implicit construction from both `std::error_code` and `boost::system::error_code`. Use `ok()` to return a successful `Result<void>`.
- **`Config`** — TOML-based system configuration (`config.toml`). Fields: `wdir`, `install_dir`, `pg_major_version`, `servers[]` (hostname + pg_port + ha_port), `pg_config`, `backup_store`, `backup_interval_secs`, `backup_wal_size`, `backup_pending_timeout_secs`, `socket_dir` (WAL sender socket, default `/tmp`), `proxy_pg_port`/`proxy_http_port`.
- **`IoThreads`** — `IoThreads(threads)` or `IoThreads(ctx, threads)` runs a pool of threads on an `asio::io_context` (the global one by default). Access the global context via `io_context()`; post work with `post(func)`.
- **`Int<T, L>`** — Strongly-typed integer wrapper template. Defined aliases: `RaftTerm` (uint64_t), `RaftIndex` (uint64_t), `RaftNodeId` (uint16_t). Use `INT(x)` or `.value()` to extract the underlying value. `std::atomic<Int<T,L>>` is specialized. `std::format("{:v}", x)` prints `TypeName(value)`; `"{}"` prints the bare integer.
- **`convert.h`** — Checked narrowing casts: `u8()`, `u4()`, `u2()`, `u1()`, `i8()`, `i4()`, `i2()`, `i1()`, `i<U>(v)`. All assert that the value fits in the target type.
- **`formatter.h`** — Specializes `std::formatter<std::filesystem::path>` so paths work directly in `std::format` calls.
- **`using.h`** — Project-wide type aliases bringing `std::` and `boost::` containers into the global namespace (`string`, `vector`, `map`, `unordered_map`, `unordered_flat_map`, `concurrent_unordered_map`, `circular_buffer`, `shared_ptr`, `Bytes`, etc.) and `asio` as an alias for `boost::asio`.
- **`base.h`** — Also exposes: `kWalBlockSize` (8192), `kWalSegmentSize` (16 MiB), `LogSeqNumber`, `log_seq_number(path)` (parses a 24-hex-char WAL file name), `filename(lsn)`, `epoch_msec()` (wall-clock ms), `pcounter()`/`gcounter()` (monotonic counters), `tmp_dir()`/`tmp_file()` (temp path helpers), `file_mode()`, `system_error_code(errno)`.
- **`xxhash.h`** — `xxhash32(buffer | vector<const_buffer> | ptr+size | Bytes, seed)`. The hash of a buffer vector equals the hash of their concatenation. Used as the frame checksum in `net`.
- **`WalSegment`** (`wal_segment.h/cc`) — Shared, ref-counted mmap wrapper for a WAL segment file on disk: `WalSegment(shared_ptr<WalSegmentCache>, dir, lsn)`. `open()` mmap's the file `PROT_READ|PROT_WRITE|MAP_SHARED` and registers it in the `WalSegmentCache` (`mutex` + `map<LogSeqNumber, Mapping>`) so that openers sharing a cache share the mapping; if `X` doesn't exist it opens `.X` (see `WalFuseOps::op_unlink`). `mapping()` returns the base pointer; `write(buf, off, size)` copies into it; `const_buffer(off, size)` is a zero-copy view; `fsync(off, size)` page-aligns and `msync`s. GC on every `open`/destructor evicts unreferenced mappings idle for ≥ 5 s, and all unreferenced ones when more than 5 remain. Used by `wal_fuse` and `raft`.
- **`WalTracker`** (`wal_segment.h`) — Receiver of WAL events, with default implementations: `async_publish(segment, offset, size)` (no-op) is called for WAL bytes as the FUSE parser walks over them; `async_fsync(segment, offset, size, completion)` is called on fsync and must call `completion(Result<void>)` once the range is durable (default: local `msync`); `async_timeline(timeline, content)` (no-op) is called when PostgreSQL installs a timeline history file. The raft layer is meant to implement it. `wal_segment.h` also has the history-file helpers `history_timeline(path)`, `history_filename(tli)`, `write_history_file(dir, tli, content)` (atomic) and `read_history_file(dir, tli)`.
- **`testing.h`** — Test utilities: `TmpDir` (RAII temp directory, auto-deleted), `test_config()` (loads `config.toml`), `top_dir()`. Every test file must `#define BOOST_TEST_MODULE <name>` before including this header (enforced by a `#error`).
- **`SemaphoreLock`** (`semaphore_lock.h`) — RAII guard for `std::binary_semaphore`, analogous to `std::unique_lock`. Constructor does a non-blocking `try_acquire`; call `acquire()` to block if needed. `owns()` / `operator bool()` test ownership. Explicit `release()` available before destruction.

**`src/net/`** — Async framed messaging on Boost.Asio. No TLS setup yet (no certificates in `base`), `std::function` callbacks, `from_data` returns `Result<T>`, and up to two attached buffers per message. See `docs/net.md`.

- **`socket.h`** — `tcp_socket` / `ssl_socket` wrappers that serialize socket calls with a mutex and count reads (`counter()`). `socket_type = std::variant<tcp_socket, ssl_socket>`; keep the `ssl_socket` alternative so TLS can be enabled later (nothing creates one yet).
- **`Connection`** — Framed-message connection. Every frame has a 6-word little-endian header (magic `0xDEADBEEF`, frame counter, kind/sizes, xxhash32 checksums). `write_data(Data, {buf0, buf1}, handler)` / `write_data(Data, handler)` send a message plus up to two attached buffers (caller keeps them alive until the handler runs); `read_data_and_buff(handler)` receives `Result<tuple<Data, Bytes>>` (attached bytes concatenated), `read_data(handler)` receives messages without attached bytes. `read_file()` / `write_file()` stream files in 64 KiB chunks, each xxhash32-checked. Checksum mismatches close the connection with `EBADMSG`. All calls are thread-safe and callbacks are posted. With a timeout, a status request goes out every `clamp(timeout/10, 100 ms, 1 s)` and the connection closes with `ETIMEDOUT` if the socket read counter stops moving (`timeout_and_times()`). `status()` / `set_peer_status()` carry a one-byte peer status on heartbeats. `write_test_delay()` delays writes 5 s (tests). Instantiate via `Connection::create<DerivedType>(...)`, which calls `start()` (once). Override `on_shutdown()` to clean up.
- **`Server`** — Abstract TCP accept loop. Subclass and implement `start()` (call `do_accept()`), `new_connection(peer, socket)`, `on_shutdown()`. Handshake: the server sends a one-byte TLS flag (always 0), then reads the client's 64-byte NUL-terminated identity (`peer`) within 10 s. Transient accept errors keep accepting; resource errors retry after 100 ms. `shutdown()` cancels the acceptor; existing connections keep running.
- **`Data`** — Pool-backed buffer of 8 KiB pages (`kPageSize`), a `std::basic_streambuf`. Serialize with `to_data<T>(obj)` (binary by default); deserialize with `from_data<T>(data)` → `Result<T>` (`EBADMSG` on a malformed message).
- **`HostAddress`** — Plain `{address, port}` struct with serialization support and `to_string()`.
- **`async_connect`** / **`connect`** — `connect(io, self_identity, host)`: resolve, connect, read the server's TLS byte (non-zero → `ENOTSUP`), send the identity (≤ 63 chars).

**`src/process/`** — Two classes:

- **`Process`** — wraps `fork`/`exec`. Manages child process lifecycle: `start()`, `stop()` (SIGTERM → SIGKILL), `kill()`, `wait()`, `try_wait()`.
- **`PgInstance`** — higher-level wrapper around `Process` for a full PostgreSQL instance. Constructor: `PgInstance(pg_bin, data_dir, port, role=Role::kStandby)`. `port` is always required and passed as `-p` to postgres. `Role` is `kPrimary` or `kStandby`; `promote()` (creates the `promote` file, then SIGUSR1) updates it to `kPrimary`. Provides: `init(initdb_args)` (runs `initdb`), `create_raft_wal(raft_dir)` (creates `raft_dir/pg_wal`, moves initial WAL segments from `data_dir/pg_wal` into it, leaves `pg_wal` empty for the FUSE mount — call after `init()`, before `start()`), `write_config(map)` / `write_hba(lines)` (append to `postgresql.conf` / `pg_hba.conf`; `write_config` also injects required pg-raft defaults — `wal_sync_method=fsync`, `wal_recycle=off`, `wal_level=replica`, `listen_addresses='*'`, `fsync=on`, `synchronous_commit=on` — unless overridden; `write_hba` appends loopback-trust and scram-sha-256 defaults; `init()` adds `--data-checksums` unless `-k` is given), `start()` / `stop(StopMode)` / `reload()` (SIGHUP) / `promote()` (SIGUSR1), `wait_ready(port)` (polls `pg_isready`, and checks the server answering is this instance: `postmaster.pid` has its pid and status `ready` or `standby`, so a PostgreSQL that failed to bind a port another holds is not ready). `StopMode` maps to `SIGTERM` (Smart), `SIGINT` (Fast), `SIGQUIT` (Immediate). `kill()` kills PostgreSQL at once and takes no lock (SIGSTOP the postmaster, SIGKILL each child from `child_pids()` and its group, then the postmaster; every PostgreSQL child calls `setsid()`, so a group signal to the postmaster misses them); `stop()` reaps afterwards, and when it reaps a postmaster killed by a signal it calls `release_ipc()`, which removes the System V segment named in `postmaster.pid` (postmaster gone, nothing attached) and the semaphore sets that pass PostgreSQL's own ownership test (last semaphore = 537, last operated on by that postmaster). `restore()` keeps the data directory and replaces its contents: PostgreSQL's System V shared memory and semaphores (which survive a SIGKILL) are keyed by the directory's inode and are only reclaimed by a postmaster in the same directory. `Process` runs each child in its own process group (`kill_group()`) and closes every inherited descriptor above stderr before `exec` (asio sockets are not close-on-exec on macOS: a PostgreSQL would otherwise hold the node's raft ports).

  Backup/restore: `backup(dest_dir)` runs a backup and returns a `SemaphoreLock` (true = success, false = failure). It is always a `pg_basebackup` (`-X stream`, `--checkpoint=fast`), on a primary or a standby; the instance keeps running. At most one backup runs at a time — a concurrent call blocks until the in-progress backup finishes and then returns the same lock without re-running. `verify_backup(backup_dir, lk)` takes the lock returned by `backup()`, inspects `pg_controldata` output to determine backup type (shut-down state → `pg_checksums --check`; in-production state → `pg_verifybackup`), and returns the lock on success or releases it and returns a non-owning lock on failure. `restore(src)` / `restore_and_start(src)` copy a backup into `data_dir`.

**`src/wal_sender/`** — Two cooperating Asio-based classes:

- **`WalSender`** — Handles a single physical replication connection. Internally goes through three phases: **Startup** (handles PostgreSQL startup packet, SSL decline, auth-ok, parameter status); **Command** (responds to `IDENTIFY_SYSTEM`, `START_REPLICATION`, `TIMELINE_HISTORY`, `SHOW`, `SET`, simple `SELECT`); **Streaming** (enters `CopyBoth` mode, streams `XLogData` messages, sends keepalives every 5 s, handles `StandbyStatusUpdate`). Reads WAL directly from `wal_dir` on disk with `open`/`pread` (not `WalSegment`); segment filenames are derived from the LSN as `%08X%08X%08X` (timeline, seg-high, seg-low). Call `advance(LogSeqNumber flush_lsn)` from any thread (posted onto the strand) to notify that WAL is committed up to `flush_lsn` on its timeline. **Timelines**: later timelines are learned from the history files (`%08X.history`) in `wal_dir` (raft writes them when it applies a `TimelineOp`); `TIMELINE_HISTORY n` serves them; `START_REPLICATION … TIMELINE t` streams `t` up to its switch point, then CopyDone and, after the standby's CopyDone, the `(next_tli, next_tli_startpos)` row, so the standby moves on; bytes before a timeline's start point are read from the parent timeline's segment (PostgreSQL copies them through an `xlogtemp` file that FUSE doesn't replicate). The caller is responsible for driving `advance()` — e.g. from a `WalFuseOps` fsync hook or by polling `pg_current_wal_flush_lsn()`. An optional `StatusCb` fires on each `StandbyStatusUpdate` (`WalSenderServer::create(..., on_sender, status_cb)` passes it to every sender). `shutdown()` closes the socket. No authentication; `server_version` is reported as `17.0`.

- **`WalSenderServer`** — Async **Unix-domain-socket** accept loop. `start()` binds `socket_dir/.s.PGSQL.<port>`, incrementing `port` until the socket file is free, and returns `false` on error; `bound_port()` / `socket_path()` report what was bound. The standby connects with `primary_conninfo='host=<socket_dir> port=<bound_port>'`. Creates one `WalSender` per connection, starts it and calls the user-supplied `SenderCb` with it. `shutdown()` closes the acceptor and removes the socket file; existing senders keep running.

  ```cpp
  auto server = WalSenderServer::create(
      socket_dir, port, wal_dir, system_id, timeline, initial_flush_lsn,
      [](shared_ptr<WalSender> s) { /* store s, call s->advance() later */ });
  server->start();
  server->shutdown();
  ```

**`src/wal_fuse/`** — FUSE-based interception layer for `pg_wal`. Primary classes:

- **`Fuse`** — Passthrough FUSE filesystem. Constructor: `Fuse(mount_point, unique_ptr<FuseOps>)`. Mounts at `mount_point` and delegates all file operations to the `FuseOps` instance (passed as `fuse_get_context()->private_data`). `start(timeout_ms)` mounts and blocks until the mount is live (detected via `st_dev` change); `stop()` unmounts and joins the loop thread. Runs the FUSE event loop in its own thread.

- **`FuseOps`** — Concrete FUSE operations base class. Constructed with a `backing_dir` path; every FUSE op is forwarded to that directory. Subclass and override virtual `op_*` methods to intercept specific operations. `static make_ops()` returns a `struct fuse_operations` whose entries are static trampolines that dispatch through `fuse_get_context()->private_data`. All ops expose `std::atomic<uint64_t> n_<opname>` counters (relaxed, observation-only) for testing.

- **`WalFuseOps`** (in `wal_fuse_ops.h/cc`) — `WalFuseOps(WalTracker&, backing_dir)`. `FuseOps` subclass that identifies WAL segment files by their 24-hex-char filename (TLI+LOGID+LOGSEG). It owns one `WalSegmentCache`, so all handles to a segment share a mapping. On `op_open`, opens a `WalSegment` (the base mmap wrapper) and wraps it in a heap-allocated `WalFuseSegment`, storing the raw pointer directly in `fuse_file_info::fh` with a tagged bit (bit 63 set, with alignment disambiguation via bit 0) to distinguish WAL files from normal fds. Sets `direct_io`. On `op_release` the `WalFuseSegment` is deleted. `op_unlink` of a WAL file **renames `X` to `.X`** instead of deleting it (the HA layer owns WAL retention). `op_rename` to a timeline history file (`%08X.history`, installed on promotion) reads it and calls `WalTracker::async_timeline(timeline, content)`. `op_flush` is a no-op. Non-WAL files fall through to the `FuseOps` passthrough.

- **`WalFuseSegment`** (in `wal_fuse_segment.h/cc`) — Owns a `shared_ptr<WalSegment>` (the mmap layer) and implements WAL parsing atop it. On the first write, `initialize()` parses the existing file to set `next_`/`synced_`/`written_`. `write()` stores the whole write in the mmap, like any file (`written_` = highest offset written), compares and re-publishes rewrites of already-walked bytes (pulling `synced_` back), then walks the **stored** segment from `next_` up to `written_` with the nested `WalState::update()` (returns `tuple<off_t, size_t, bool>` — cursor before the call, bytes consumed, and whether a record starts there; it advances at most one page header or record piece per call, stops at a zero `xlp_magic`, and never reads past what is stored, so a header cut by a write boundary is walked once the rest arrives) and calls `WalTracker::async_publish()` for the newly walked range. `fsync()` calls `WalTracker::async_fsync(segment, synced_, max(next_, written_) - synced_, cb)` (everything stored) and blocks the FUSE thread on a `std::future` until it completes; on failure it returns `-errno`.

- **`WalTracker`** — defined in `src/base/wal_segment.h` (see `base`).

Two additional executables live in `src/wal_fuse/` but are not part of any test suite: `debug_wal_ops` (interactive WAL operation debugger) and `wal_fuse_bench` (microbenchmark for FUSE throughput). Build them with `/opt/pg_raft/bin/b2 src/wal_fuse//debug_wal_ops` or `//wal_fuse_bench`.

**`src/raft/`** — Raft over the WAL stream. Built and tested. See `docs/raft.md`.

- **Entries** (`raft_types.h`, `wal_types.h`): `Entry{time, term, WalCommand}`. The primary generates the `WalCommand`s itself (`WriteOp`/`FlushOp`/... on a segment, plus its `flushed_lsn`); a tick entry has a `monostate` command. A `RaftRequest` carries up to two `MemBlock`s, whose WAL bytes follow the request as an attached buffer. `send_request` copies them out of the mmap, because PostgreSQL keeps writing to the segment and the frame checksum must match.
- **`RaftState`** (`raft_state.h`, the state machine): `log_idx`, `timestamp`, committed `flushed_lsn`, `needs_backup`, `wal_end` (highest end of the applied `WriteOp`s; a new leader's standby replays to it before promotion). `RaftState` is persisted in `meta_` on compaction and InstallSnapshot. `RaftApplier::applied()` / `installed()` notify the node (for example to advance the WalSender).
- **`RaftLog`**: LMDB log (xxhash32 page checksums, 16 KiB pages, background sync unless `sync_commits`, `no_sync` for tests) with a `meta_` table holding `RaftState` and the **boundary state** (the state at the entry before the first one, `load_boundary()`). Provides `compact(first, state, boundary)` (deletes the entries before `first` and saves both states in one transaction), `save_snapshot(state, keep)` and `install_snapshot(log_idx, entries, state, boundary)`.
- **`RaftServer`**: elections, two-phase leadership, step-down and compaction; the leader also compacts (stepping down to compact would be a PostgreSQL failover). `exec(WalCommand, handler)` is leader-only; the handler gets the index on commit or `ErrorCode::kNotLeader`, and runs with the server lock held. `segment(lsn)` returns `wal_dir` segments through one `WalSegmentCache`. A follower writes WAL blocks only after it has accepted the request. Applying a `TimelineOp` writes its history file into `wal_dir` on every node. `raft_log()` is the log accessor (not `log()`, which would hide the global logger). The node sets `set_serving()`, `set_pg_status()` and `set_needs_backup()` (persisted; no elections while set). A `RaftObserver` gets `applied()`, `installed()`, `became_leader(term)` (full leadership) and `stepped_down(term)`, with the server lock held.
- **Compaction**: with `set_wal_keep_from(segment)` (the node passes the latest backup's segment) the first entry kept is the first whose segment is at or after it, else `keep_old_logs`; never past the last applied entry. `RaftState::first_idx` / `first_segment` (per node) record it, and the `wal_gc` thread deletes `wal_dir` segments wholly before `first_segment` (by address). `restored_backup(end)` clears `needs_backup` only if the backup reaches `first_segment`. A follower behind within what it committed is continued with AppendEntries, not a snapshot.
- **InstallSnapshot**: the **oldest** entries (as many as write to at most 2 segments) plus the full contents of those segments, the leader's boundary state (the follower's state before the entries), the and the leader's latest timeline history file (`InstallSnapshot::timeline`), which the follower writes into `wal_dir` first. `Entry` is in `entry.h` (so that `raft_types.h` can include `raft_state.h`). The follower then has `needs_backup` set and must fetch a PostgreSQL backup (phase 2, not yet wired).
- **Connections**: `RaftPeerConnection` (outbound, identity `Raft:<id>`), `RaftFollowerConnection` (inbound), `PeerConnectionMgr`, and `RaftClientConnection` / `RaftServerConnection` for the client channel (`client_types.h`: `StatusRequest`; `SnapshotRequest` → `SnapshotResponse` followed by the backup files). The proxy uses it to find the leader; nodes restore backups from a `BackupStore` (`docs/backup.md`, planned), so `SnapshotRequest` and `set_backup_provider()` are unused by the node design.
- **Tests**: `raft_log_test`; `raft_ensemble_{sanity,leader_stopped,stepdown,snapshot}_test` run in-process ensembles (`RaftTestService`) with a `Writer` that plays the primary, checked by `check_consistency` (log matching, states, WAL bytes of every acknowledged write).

**`src/backup/`** — The cluster's PostgreSQL backups. See `docs/backup.md`.

- **`BackupStore`** (`backup_store.h`) — abstract store: `begin(node, time)` → `PendingBackup` (a lease the node `refresh`es while `pg_basebackup` runs), `put(pending, dir, end_lsn)` (atomic), `abort(node)`, `pending()`, `list()` / `latest()`, `get(backup, dest)`, `remove()`. Backups are named `TTTTTTTT-XXXXXXXXXXXXXXXX-SSSSSSSSSS-N` (timeline, end LSN, start time, node index; `BackupInfo::name()` / `parse_backup_name()`), sorting by end LSN. Pure helpers: `backup_due(policy, index, nodes, latest, now, wal_end)` (staggered deadlines: `interval·(1+i/N)` or `wal_size·(1+i/N)`), `pending_blocks()` (another node's lease refreshed within `pending_timeout`), `keep_from(latest)` (segment start of the end LSN).
- **`DirBackupStore(root)`** (`dir_backup_store.h`) — one directory per backup; `put` copies into `.tmp-<name>`, fsyncs and renames; leases are `.pending-<node>` files (start time inside, mtime = refresh time). An S3 store is planned.

**`src/node/`** — The pg-raft node process (in progress; `docs/node.md`). So far:

- **`Node`** (`node.h`) / **`pg_raft_node --config config.toml --id N`** — `Node::create(config, id, NodeOptions)`, `start()`, `shutdown()`. Layout `wdir/node<N>/{data, pg/pg_wal, raft, raft/wal}`. `start()`: raft first (a `RaftObserver` forwards `applied` / `became_leader` / `stepped_down`); with no usable data directory (or `needs_backup`) it sets `needs_backup`, restores the latest backup into `data/` (same directory), moves its `pg_wal` into `pg/pg_wal` and copies its WAL into `raft/wal` where missing; mounts FUSE (`WalFuseOps` + `NodeWalTracker`); starts a `WalSenderServer` on `raft/wal` (socket `socket_dir/.s.PGSQL.<pg_port+20000>`); writes `standby.signal` and `pg_raft.conf` (included once from `postgresql.conf`); starts PostgreSQL as a standby; starts the transition thread and the 500 ms status poller (`set_flushed_lsn`, `set_pg_status`). `became_leader` → the transition thread advances the WAL sender to `wal_end`, waits for the standby's flush (`StatusCb`), switches the tracker to leader mode, `promote()`s, waits until the new timeline's history file is in `raft/wal`, then `set_serving(true)`. `stepped_down` → in the hook: tracker fenced, `PgInstance::kill()`; the transition thread reaps it. `installed` (an InstallSnapshot replaced the log) → the standby is killed and the transition thread restores the latest backup and restarts it (`restore_and_restart`); the backup thread also calls `set_wal_keep_from(keep_from(latest))` and deletes `pg/pg_wal`'s unlinked `.X` segments before `first_segment`. Demotion (`demote` / `restart_as_standby`, `docs/node.md` §3.3): reap it, unmount FUSE, `rebuild_pg_wal` (`wal_rebuild.h`: `pg/pg_wal` rebuilt from `raft/wal` up to `max(wal_end, the restored backup's end rounded up to its segment)`), remount, restart as a standby; a promotion that finds PostgreSQL killed does that first. The restored backup's end LSN is kept in `node<N>/backup_end`. Backups after bootstrap: a backup thread checks every `NodeOptions::backup_check_interval` (60 s); a standby (not the leader, no `needs_backup`, PostgreSQL running) takes one when `backup_due()` says its staggered deadline has passed and `pending_blocks()` finds no live lease: `store.begin`, a refresher thread renews the lease every `pending_timeout/4`, `PgInstance::backup` (`pg_basebackup` on the standby) + `verify_backup`, then `store.put` with the manifest's end LSN, or `store.abort`. On start a node aborts its own leftover lease.

- **`pg_raft_initdb --config config.toml [--port N]`** (`pg_raft_initdb.cc`, `init_cluster()` in `init.h`) — creates a cluster's first backup: initdb (with `pg_config` and the pg-raft defaults) into a scratch dir under `wdir`, start PostgreSQL on `--port` (default `servers[0].pg_port`) without FUSE, `pg_basebackup`, `verify_backup`, then `store.begin(0)` / `put` with the end LSN from `backup_manifest` (`backup_end_lsn()`). Refuses a store that already holds a backup. `pg_bin(config)` is `install_dir/<pg_major_version>/bin`.
- **`NodeWalTracker(shared_ptr<RaftServer>)`** (`node_wal_tracker.h`) — the node's `WalTracker`. `set_mode(kLeader | kFollower)`, `set_flushed_lsn()` (from the status poller). Leader mode: `async_publish` copies the bytes into raft's `wal_dir` segment, then `exec(WriteOp)`; `async_fsync` msyncs both copies, then `exec(FlushOp)` and completes on commit, or with `WalSegment::kNoConsensus` if not leader; `async_timeline` → `exec(TimelineOp)`. Follower mode: publish/timeline ignored, fsync is a local msync. Fenced mode (set when the node loses leadership, until PostgreSQL restarts as a standby): every fsync fails with `kNoConsensus`.

**`src/proxy/`** — The client entry point (`docs/proxy.md`), a plain Boost.Asio TCP proxy (not on `net`): `proxy_pg_port` → the PostgreSQL of the node raft reports as leader and serving, `proxy_pg_port + 1 + i` → `servers[i]`. `LeaderTracker` keeps a `RaftServerConnection` (identity `proxy`) to each node's `ha_port` and reads the heartbeat status byte (`0x3` = serving); the primary is the one node serving. `Bridge` pumps both directions (one read in flight each way, 16 KiB buffers) on its own strand and closes both sockets once; `TCP_NODELAY` and keepalive. A primary-port client with no primary is held up to `ProxyOptions::failover_window` (30 s), then closed; a primary change closes the old primary's primary-port bridges. `pg_raft_proxy --config config.toml`. **Admin interface** (`admin.{h,cc}`, Boost.Beast) on `proxy_http_port`, read-only: `GET /health`, `/primary`, `/status`; `.json` (or no extension) returns JSON, `.html` an HTML page refreshing every 5 s; `/status` gathers each node's raft status (`RaftServerConnection::node_status`) on a worker thread and answers 500 on an exception.

**`src/status/`** — PostgreSQL client for querying a running instance:

- **`PgClient`** — Async PostgreSQL extended-protocol client over a raw TCP socket (not libpq). `close()` runs on the strand (cancels an exec in flight). `connect(port)` synchronously establishes a connection. `prepare(sql)` synchronously sends a `Parse`/`Describe`/`Sync` sequence and returns a `PgStmt` handle. `async_exec(stmt, cb)` executes a prepared statement asynchronously and delivers `Result<Rows>` to the callback; multiple calls queue up and run one at a time. Only `trust` auth, loopback, `database=postgres`, no bind parameters; don't call the synchronous `prepare()` while an `async_exec` is in flight (they share read buffers). `close()` tears down the socket. Used in tests to verify replication state (e.g. row counts, LSNs) without needing an external psql binary.

### Data flow (end-to-end)

```
initdb
  └─▶  PgInstance::create_raft_wal(raft_dir)  ──(moves WAL)──▶  raft_dir/pg_wal/
                                                                        │
                 Fuse (FUSE)  ◀──(backed by)──────────────────────────┘
                    │  mounted at data_dir/pg_wal
                    ▼
               postgres write(WAL) ──▶ WalFuseOps::op_write
                                             │  WalFuseSegment::write() — walks page headers
                                             │  and record lengths (no CRC or xlp_pageaddr
                                             │  check), calls WalTracker::async_publish()
                                             ▼
                                       postgres fsync ──▶ WalFuseOps::op_fsync
                                             │  WalFuseSegment::fsync() — msync dirty pages
                                             │  calls WalTracker::async_fsync() and
                                             │  blocks on std::future result
                                             ▼
                          (planned) raft replicates WAL to followers; on commit:
                                       WalSender::advance(flush_lsn)
                                             │  reads WAL from wal_dir with pread
                                       WalSenderServer  ──(replication protocol, Unix socket)──▶  PostgreSQL standby
```

WAL segment files are memory-mapped on first open via `WalSegment` (base layer); the mapping is shared (through the `WalSegmentCache`) across all `WalFuseSegment` instances for the same LSN and is ref-counted with a 5-second idle GC. `WalFuseSegment::write()` parses XLog page and record headers inline and stops at the first stale or pre-zeroed page. macOS `F_FULLFSYNC` is not intercepted by FUSE, but regular `fsync` is — which is sufficient for PostgreSQL's WAL flush path.


### Key data types

```cpp
// In src/base/base.h
struct LogSeqNumber { uint32_t timeline; uint64_t lsn; };
// lsn encoding: high 32 bits = segment number, low 32 bits = offset within segment

// In src/wal_sender/wal_sender.h
struct StandbyStatus { uint64_t written_lsn, flushed_lsn, applied_lsn; };

// In src/status/pg_client.h
using Row  = vector<std::optional<string>>;
using Rows = vector<Row>;
struct PgStmt { string name; };  // handle returned by PgClient::prepare()
```

LSNs in `wal_sender` are `uint64_t` (high 32 bits = segment number, low 32 bits = offset). PostgreSQL timestamps are microseconds since 2000-01-01 (not Unix epoch).

### FUSE include requirements

`src/base/pch.h` already defines `FUSE_USE_VERSION 26` and `_FILE_OFFSET_BITS 64` before including `<fuse/fuse.h>`, so files compiled through the PCH get these automatically. Any file that includes `<fuse/fuse.h>` **outside** the PCH must define them first:

```cpp
#define FUSE_USE_VERSION 26
#define _FILE_OFFSET_BITS 64
```

## Tests

Integration tests use PostgreSQL at `/opt/pg_raft/postgresql/17/bin`. All auto-skip if the binaries are absent.

- **`semaphore_lock_test`** — unit tests for `SemaphoreLock`. No PostgreSQL needed.
- **`xxhash_test`** — known xxHash32 vectors and multi-buffer equivalence.
- **`data_test`** — `Data` text/binary round-trips. Takes ~50 s (60 s limit), so it times out under ASAN.
- **`connection_test`** — `Connection` messages (with one and two attached buffers), status, background heartbeats, files, the read-silence timeout and `timeout_and_times`. Passes under ASAN and TSAN.
- **`async_connect_test`** — connect and handshake.
- **`process_test`** — unit tests for `Process`.
- **`pg_instance_test`** — integration tests for `PgInstance`: init, write_config, write_hba, create_raft_wal, start/stop, backup/restore.
- **`wal_sender_test`** — integration test for `WalSender`/`WalSenderServer`. Starts a real primary and standby PostgreSQL, configures the standby to replicate via `WalSenderServer`, runs pgbench, and verifies the standby catches up by LSN and row count. Uses `PgClient` internally to query both instances. Requires PostgreSQL; auto-skips if absent.
- **`pg_wal_switch_test`** — integration test for WAL segment boundary handling. Same setup as `wal_sender_test` but calls `pg_switch_wal()` four times, 150 ms apart, while pgbench runs in the background, forcing the WalSender to follow WAL across multiple forced segment transitions. Requires PostgreSQL; auto-skips if absent.
- **`standby_wal_reset_test`** — integration test for standby reconnection after WAL loss. Runs a first pgbench batch and waits for the standby to catch up, then stops the standby and deletes all WAL segment files from its `pg_wal` directory. Runs a second pgbench batch while the standby is down, then restarts it. Verifies the WalSender re-streams the missing WAL from the earlier position and the standby converges to the final primary LSN. Requires PostgreSQL; auto-skips if absent.
- **`timeline_switch_test`** — integration test for timelines: a standby (S2) of the primary is promoted to timeline 2 after the primary stops; another standby (S1) streaming through `WalSender` from a WAL directory that mirrors the primary's and S2's WAL (as hard links, playing `raft/wal`) must follow onto timeline 2 and converge with S2. Requires PostgreSQL; auto-skips if absent.
- **`fuse_test`** — tests for `Fuse` and `FuseOps`. The majority of cases (lifecycle, passthrough reads/writes, getattr, xattr, rename, concurrency, etc.) run without PostgreSQL. Two cases (`wal_moved_to_raft_dir`, `wal_writes_to_backing_dir`) start a real PostgreSQL instance and skip if it's absent.
- **`wal_fuse_ops_test`** — unit tests for `WalFuseOps` and `WalFuseSegment` interaction. Exercises open/write/fsync/read/release ops against a `WalFuseOps` instance backed by a `TmpDir`. No real PostgreSQL needed.
- **`wal_state_test`** — unit tests for `WalFuseSegment::WalState` XLog parsing. Feeds real captured WAL bytes (from `wal_data.cc`) through `WalState::update()` and checks the returned `(offset, size, complete)` tuples. No real PostgreSQL needed.
- **`wal_shadow_test`** — integration test using `WalFuseOps` directly (via a `ShadowWalOps` subclass that mirrors writes into an in-memory buffer). Starts a real PostgreSQL instance, runs pgbench, then cross-checks the shadow buffer against the on-disk WAL bytes. Auto-skips if PostgreSQL is absent.
- **`backup_store_test`** — backup names (round trip, rejects, sort order), `keep_from`, the staggered deadlines, leases, and `DirBackupStore` put/list/get/remove, partial copies and leases. No PostgreSQL needed.
- **`init_test`** — `backup_end_lsn()` from a manifest, and `init_cluster()` with real PostgreSQL: one verified backup in the store, which restores and starts as a primary with the configured settings; a second init is refused. Requires PostgreSQL for the second case.
- **`node_wal_tracker_test`** — `NodeWalTracker` against a 3-node in-process raft ensemble: leader mode replicates the published bytes to every `wal_dir` and completes the fsync on commit (with `flushed_lsn` and `wal_end` in every state); leader mode on a follower fails the fsync with `kNoConsensus`; follower mode adds no entry and copies nothing; a timeline's history file reaches every node.
- **`proxy_test`** — the proxy against an in-process raft ensemble and fake PostgreSQL echo servers: per-server routing, holding a primary-port client until a primary serves, closing the old primary's clients on a hand-over, closing after the failover window; the admin interface in JSON and HTML.
- **`node_test`** — `docs/node.md` test 1 (pgbench through the proxy): `pg_raft_initdb`, then 3 `Node`s in one process (3 raft servers, 3 FUSE mounts, 3 PostgreSQL); one becomes the primary on timeline 2, pgbench runs on it, the standbys converge by LSN and row count; then the primary steps down, another promotes to timeline 3, the old one is demoted to a standby, and all converge again (1200 rows). ~2 minutes.
- **`node_recovery_test`** — `docs/node.md` test 3: backups every 15 s move `keep_from`, the leader compacts; a follower's whole directory is deleted, it restarts, restores, gets an InstallSnapshot of the oldest entries, and catches up (1200 rows, `needs_backup` cleared).
- **`raft_ensemble_keep_from_test`** — `set_wal_keep_from` compacts every server to the first entry of the kept segment, the earlier segments are deleted, and a wiped server catches up through InstallSnapshot of the oldest entries with the leader's boundary state.
- **`node_backup_test`** — 3 nodes with `backup_interval_secs = 15`: a standby (never the primary) stores a backup on timeline 2 after pgbench, no lease is left, and the backup restores and holds the 100000 accounts.
- **`wal_rebuild_test`** — `rebuild_pg_wal`: raft's copies kept, the end's segment zeroed from the end, uncommitted segments, timelines and leftovers removed; the backup-end case.
- **`config_test`** — `Config` parsing, including the node settings and their defaults.
- **`pg_client_test`** — integration tests for `PgClient`. Starts a real PostgreSQL instance, exercises `connect`/`prepare`/`async_exec`. Requires PostgreSQL; auto-skips if absent.
