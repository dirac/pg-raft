# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

**pg-raft** is a high-availability system for PostgreSQL that replicates the WAL stream itself through Raft. The current code implements:

1. **WAL interception** (`wal_fuse`) — a FUSE filesystem that mounts over `pg_wal` so that all WAL writes are transparently redirected to a separate `raft_dir/pg_wal` directory under pg-raft's control.
2. **WAL streaming** (`wal_sender`) — a C++ server that speaks the PostgreSQL physical streaming replication protocol, streaming WAL from disk to a real PostgreSQL standby.
3. **Networking** (`net`), **process control** (`process`) and a **PostgreSQL client** (`status`).
4. **Raft** (`raft`) — partially implemented and **not built** (see below). `src/proxy/` is empty.

Per-component design documents are in `docs/design/` (one per module, plus `README.md`).

## Build system

This project uses [Boost.Build (b2)](https://www.boost.org/build/) — not CMake or Make. The `b2` binary is at `/opt/sparrow/bin/b2`. All external dependencies (Boost, OpenSSL, lmdb, fuse) are installed under `/opt/pg_raft/` and `/usr/local/` (see `Jamroot.jam` for exact search paths).

All `b2` commands must be run from the repo root (the directory containing `Jamroot.jam`).

```bash
# Build everything
/opt/sparrow/bin/b2

# Build + run all tests
/opt/sparrow/bin/b2 test

# Build only a module
/opt/sparrow/bin/b2 src/process
/opt/sparrow/bin/b2 src/wal_sender
/opt/sparrow/bin/b2 src/wal_fuse
/opt/sparrow/bin/b2 src/net
/opt/sparrow/bin/b2 src/status

# Build + run a specific unit-test target (auto-runs after build)
/opt/sparrow/bin/b2 src/base//semaphore_lock_test
/opt/sparrow/bin/b2 src/base//xxhash_test
/opt/sparrow/bin/b2 src/process//process_test
/opt/sparrow/bin/b2 src/process//pg_instance_test
/opt/sparrow/bin/b2 src/wal_sender//wal_sender_test
/opt/sparrow/bin/b2 src/wal_sender//pg_wal_switch_test
/opt/sparrow/bin/b2 src/wal_sender//standby_wal_reset_test
/opt/sparrow/bin/b2 src/wal_fuse//fuse_test
/opt/sparrow/bin/b2 src/wal_fuse//wal_fuse_ops_test
/opt/sparrow/bin/b2 src/wal_fuse//wal_state_test
/opt/sparrow/bin/b2 src/wal_fuse//wal_shadow_test
/opt/sparrow/bin/b2 src/net//async_connect_test
/opt/sparrow/bin/b2 src/net//data_test
/opt/sparrow/bin/b2 src/net//connection_test
/opt/sparrow/bin/b2 src/status//pg_client_test

# Run a single Boost.Test case within a unit-test binary (pass args after --):
/opt/sparrow/bin/b2 src/process//process_test -- --run_test=TestCaseName
# Or run the binary directly for full Boost.Test flags (--log_level=all, etc.):
./src/process/bin/clang-darwin-17/debug/cxxstd-23-iso/link-static/visibility-hidden/process_test --run_test=TestCaseName

# Build with sanitizers
/opt/sparrow/bin/b2 address-sanitizer=norecover
/opt/sparrow/bin/b2 thread-sanitizer=norecover
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

- **`logger`** — Thread-safe, `std::format`-based logging to stderr. Levels: `Debug`, `Info`, `Warn`, `Error`, `Fatal`. Default level is `Info`. Call `logger::set_level()` to adjust. Use `log(LEVEL, fmt, args...)` globally (constants `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL` are in scope via the header). `logger::elog(ec, fmt, args...)` logs an `std::error_code` at `Error` level and returns the code. `logger::plog(prefix, line)` logs a line prefixed with a process name tag.
- **`Result<T>`** — `std::expected<T, std::error_code>` wrapper with implicit construction from both `std::error_code` and `boost::system::error_code`. Use `ok()` to return a successful `Result<void>`.
- **`Config`** — TOML-based system configuration (`config.toml`). Fields: `wdir`, `install_dir`, `pg_major_version`, `servers[]` (hostname + pg_port + ha_port), `pg_config`, `proxy_pg_port`/`proxy_http_port`.
- **`IoThreads`** — `IoThreads(threads)` or `IoThreads(ctx, threads)` runs a pool of threads on an `asio::io_context` (the global one by default). Access the global context via `io_context()`; post work with `post(func)`.
- **`Int<T, L>`** — Strongly-typed integer wrapper template. Defined aliases: `RaftTerm` (uint64_t), `RaftIndex` (uint64_t), `RaftNodeId` (uint16_t). Use `INT(x)` or `.value()` to extract the underlying value. `std::atomic<Int<T,L>>` is specialized. `std::format("{:v}", x)` prints `TypeName(value)`; `"{}"` prints the bare integer.
- **`convert.h`** — Checked narrowing casts: `u8()`, `u4()`, `u2()`, `u1()`, `i8()`, `i4()`, `i2()`, `i1()`, `i<U>(v)`. All assert that the value fits in the target type.
- **`formatter.h`** — Specializes `std::formatter<std::filesystem::path>` so paths work directly in `std::format` calls.
- **`using.h`** — Project-wide type aliases bringing `std::` and `boost::` containers into the global namespace (`string`, `vector`, `map`, `unordered_map`, `unordered_flat_map`, `concurrent_unordered_map`, `circular_buffer`, `shared_ptr`, `Bytes`, etc.) and `asio` as an alias for `boost::asio`.
- **`base.h`** — Also exposes: `kWalBlockSize` (8192), `kWalSegmentSize` (16 MiB), `LogSeqNumber`, `log_seq_number(path)` (parses a 24-hex-char WAL file name), `filename(lsn)`, `epoch_msec()` (wall-clock ms), `pcounter()`/`gcounter()` (monotonic counters), `tmp_dir()`/`tmp_file()` (temp path helpers), `file_mode()`, `system_error_code(errno)`.
- **`xxhash.h`** — `xxhash32(buffer | vector<const_buffer> | ptr+size | Bytes, seed)`, ported from sparrowdb. The hash of a buffer vector equals the hash of their concatenation. Used as the frame checksum in `net`.
- **`WalSegment`** (`wal_segment.h/cc`) — Shared, ref-counted mmap wrapper for a WAL segment file on disk: `WalSegment(shared_ptr<WalSegmentCache>, dir, lsn)`. `open()` mmap's the file `PROT_READ|PROT_WRITE|MAP_SHARED` and registers it in the `WalSegmentCache` (`mutex` + `map<LogSeqNumber, Mapping>`) so that openers sharing a cache share the mapping; if `X` doesn't exist it opens `.X` (see `WalFuseOps::op_unlink`). `mapping()` returns the base pointer; `write(buf, off, size)` copies into it; `const_buffer(off, size)` is a zero-copy view; `fsync(off, size)` page-aligns and `msync`s. GC on every `open`/destructor evicts unreferenced mappings idle for ≥ 5 s, and all unreferenced ones when more than 5 remain. Used by `wal_fuse` and `raft`.
- **`WalTracker`** (`wal_segment.h`) — Receiver of WAL events, with default implementations: `async_publish(segment, offset, size)` (no-op) is called as WAL bytes are validated; `async_fsync(segment, offset, size, completion)` is called on fsync and must call `completion(Result<void>)` once the range is durable (default: local `msync`). The raft layer is meant to implement it.
- **`testing.h`** — Test utilities: `TmpDir` (RAII temp directory, auto-deleted), `test_config()` (loads `config.toml`), `top_dir()`. Every test file must `#define BOOST_TEST_MODULE <name>` before including this header (enforced by a `#error`).
- **`SemaphoreLock`** (`semaphore_lock.h`) — RAII guard for `std::binary_semaphore`, analogous to `std::unique_lock`. Constructor does a non-blocking `try_acquire`; call `acquire()` to block if needed. `owns()` / `operator bool()` test ownership. Explicit `release()` available before destruction.

**`src/net/`** — Async framed messaging on Boost.Asio, **ported from sparrowdb `lib/net`, which is the reference implementation** — follow it when changing this module. Deliberate differences: no TLS setup (no certificates in `base` yet), no roles, no metrics, `std::function`, `from_data` returns `Result<T>`, and up to two attached buffers per message. See `docs/design/net.md`.

- **`socket.h`** — `tcp_socket` / `ssl_socket` wrappers that serialize socket calls with a mutex and count reads (`counter()`). `socket_type = std::variant<tcp_socket, ssl_socket>`; keep the `ssl_socket` alternative so TLS can be enabled later (nothing creates one yet).
- **`Connection`** — Framed-message connection. Every frame has a 6-word little-endian header (magic `0xDEADBEEF`, frame counter, kind/sizes, xxhash32 checksums). `write_data(Data, {buf0, buf1}, handler)` / `write_data(Data, handler)` send a message plus up to two attached buffers (caller keeps them alive until the handler runs); `read_data_and_buff(handler)` receives `Result<tuple<Data, Bytes>>` (attached bytes concatenated), `read_data(handler)` receives messages without attached bytes. `read_file()` / `write_file()` stream files in 64 KiB chunks, each xxhash32-checked. Checksum mismatches close the connection with `EBADMSG`. All calls are thread-safe and callbacks are posted. With a timeout, a status request goes out every `clamp(timeout/10, 100 ms, 1 s)` and the connection closes with `ETIMEDOUT` if the socket read counter stops moving (`timeout_and_times()`). `status()` / `set_peer_status()` carry a one-byte peer status on heartbeats. `write_test_delay()` delays writes 5 s (tests). Instantiate via `Connection::create<DerivedType>(...)`, which calls `start()` (once). Override `on_shutdown()` to clean up.
- **`Server`** — Abstract TCP accept loop. Subclass and implement `start()` (call `do_accept()`), `new_connection(peer, socket)`, `on_shutdown()`. Handshake: the server sends a one-byte TLS flag (always 0), then reads the client's 64-byte NUL-terminated identity (`peer`) within 10 s. Transient accept errors keep accepting; resource errors retry after 100 ms. `shutdown()` cancels the acceptor; existing connections keep running.
- **`Data`** — Pool-backed buffer of 8 KiB pages (`kPageSize`), a `std::basic_streambuf`. Serialize with `to_data<T>(obj)` (binary by default); deserialize with `from_data<T>(data)` → `Result<T>` (`EBADMSG` on a malformed message).
- **`HostAddress`** — Plain `{address, port}` struct with serialization support and `to_string()`.
- **`async_connect`** / **`connect`** — `connect(io, self_identity, host)`: resolve, connect, read the server's TLS byte (non-zero → `ENOTSUP`), send the identity (≤ 63 chars).

**`src/process/`** — Two classes:

- **`Process`** — wraps `fork`/`exec`. Manages child process lifecycle: `start()`, `stop()` (SIGTERM → SIGKILL), `kill()`, `wait()`, `try_wait()`.
- **`PgInstance`** — higher-level wrapper around `Process` for a full PostgreSQL instance. Constructor: `PgInstance(pg_bin, data_dir, port, role=Role::kStandby)`. `port` is always required and passed as `-p` to postgres. `Role` is `kPrimary` or `kStandby`; `promote()` updates it to `kPrimary`. Provides: `init(initdb_args)` (runs `initdb`), `create_raft_wal(raft_dir)` (creates `raft_dir/pg_wal`, moves initial WAL segments from `data_dir/pg_wal` into it, leaves `pg_wal` empty for the FUSE mount — call after `init()`, before `start()`), `write_config(map)` / `write_hba(lines)` (append to `postgresql.conf` / `pg_hba.conf`; `write_config` also injects required pg-raft defaults — `wal_sync_method=fsync`, `wal_recycle=off`, `wal_level=replica`, `listen_addresses='*'`, `fsync=on`, `synchronous_commit=on` — unless overridden; `write_hba` appends loopback-trust and scram-sha-256 defaults; `init()` adds `--data-checksums` unless `-k` is given), `start()` / `stop(StopMode)` / `reload()` (SIGHUP) / `promote()` (SIGUSR1), `wait_ready(port)` (polls `pg_isready`). `StopMode` maps to `SIGTERM` (Smart), `SIGINT` (Fast), `SIGQUIT` (Immediate).

  Backup/restore: `backup(dest_dir)` runs a backup and returns a `SemaphoreLock` (true = success, false = failure). On primary it uses `pg_basebackup` (live); on standby it stops the instance and copies the data directory. At most one backup runs at a time — a concurrent call blocks until the in-progress backup finishes and then returns the same lock without re-running. `verify_backup(backup_dir, lk)` takes the lock returned by `backup()`, inspects `pg_controldata` output to determine backup type (shut-down state → `pg_checksums --check`; in-production state → `pg_verifybackup`), and returns the lock on success or releases it and returns a non-owning lock on failure. `restore(src)` / `restore_and_start(src)` copy a backup into `data_dir`.

**`src/wal_sender/`** — Two cooperating Asio-based classes:

- **`WalSender`** — Handles a single physical replication connection. Internally goes through three phases: **Startup** (handles PostgreSQL startup packet, SSL decline, auth-ok, parameter status); **Command** (responds to `IDENTIFY_SYSTEM`, `START_REPLICATION`, `TIMELINE_HISTORY`, `SHOW`, `SET`, simple `SELECT`); **Streaming** (enters `CopyBoth` mode, streams `XLogData` messages, sends keepalives every 5 s, handles `StandbyStatusUpdate`). Reads WAL directly from `wal_dir` on disk with `open`/`pread` (not `WalSegment`); segment filenames are derived from the LSN as `%08X%08X%08X` (timeline, seg-high, seg-low). Call `advance(flush_lsn)` from any thread (posted onto the strand) to notify that WAL is available. The caller is responsible for driving `advance()` — e.g. from a `WalFuseOps` fsync hook or by polling `pg_current_wal_flush_lsn()`. An optional `StatusCb` fires on each `StandbyStatusUpdate` (note: `WalSenderServer` currently passes none). `shutdown()` closes the socket. No authentication; `server_version` is reported as `17.0`.

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

- **`WalFuseOps`** (in `wal_fuse_ops.h/cc`) — `WalFuseOps(WalTracker&, backing_dir)`. `FuseOps` subclass that identifies WAL segment files by their 24-hex-char filename (TLI+LOGID+LOGSEG). It owns one `WalSegmentCache`, so all handles to a segment share a mapping. On `op_open`, opens a `WalSegment` (the base mmap wrapper) and wraps it in a heap-allocated `WalFuseSegment`, storing the raw pointer directly in `fuse_file_info::fh` with a tagged bit (bit 63 set, with alignment disambiguation via bit 0) to distinguish WAL files from normal fds. Sets `direct_io`. On `op_release` the `WalFuseSegment` is deleted. `op_unlink` of a WAL file **renames `X` to `.X`** instead of deleting it (the HA layer owns WAL retention). `op_flush` is a no-op. Non-WAL files fall through to the `FuseOps` passthrough.

- **`WalFuseSegment`** (in `wal_fuse_segment.h/cc`) — Owns a `shared_ptr<WalSegment>` (the mmap layer) and implements WAL parsing atop it. On the first write, `initialize()` parses the existing file to set `next_`/`synced_`. `write()` advances the parse cursor `next_` via the nested `WalState::update()` (returns `tuple<off_t, size_t, bool>` — cursor before the call, bytes consumed, and whether a record starts there; it advances at most one page header or record piece per call and stops at a zero `xlp_magic`), copies only validated bytes into the mmap and calls `WalTracker::async_publish()` for them. Rewrites of already-parsed bytes are compared and, if changed, copied and re-published (pulling `synced_` back); writes beyond `next_` are ignored. `fsync()` calls `WalTracker::async_fsync(segment, synced_, next_ - synced_, cb)` and blocks the FUSE thread on a `std::future` until it completes; on success `synced_ = next_`, on failure it returns `-errno`.

- **`WalTracker`** — defined in `src/base/wal_segment.h` (see `base`).

Two additional executables live in `src/wal_fuse/` but are not part of any test suite: `debug_wal_ops` (interactive WAL operation debugger) and `wal_fuse_bench` (microbenchmark for FUSE throughput). Build them with `/opt/sparrow/bin/b2 src/wal_fuse//debug_wal_ops` or `//wal_fuse_bench`.

**`src/raft/`** — Raft over the WAL stream. **Not built**: `raft` is missing from `src/Jamfile`, and `raft_server.{h,cc}` is a partial port from an older ZooKeeper-style Raft that still references types and headers that do not exist here (`ZkRequest`, `ZkState`, `snapshot.h`, `zk_state.h`, `net/ssl_context.h`, `banyandb::`, `LOG_*`). Written for pg-raft: `raft_types.h` (`RaftRequest`/`RaftResponse` envelopes, `AppendEntries`, `RequestVote`, `InstallSnapshot`, `RequestElection`, `MemBlock`, `Status`), `wal_types.h` (`Entry` payload `WalCommand`: open/write/flush/close/timeline ops on a segment plus the leader's `flushed_lsn`), `client_types.h`, `options.h`, `RaftLog` (LMDB `term.lmdb` + `logs.lmdb`, keys `(idx, term)` big-endian), `RaftPeerConnection` / `RaftFollowerConnection` / `PeerConnectionMgr` (WAL bytes are sent as attached buffers straight from the leader's `WalSegment` mmap and written + msync'd by the follower before it handles the request), `RaftClientConnection` / `RaftServerConnection` (leadership via the heartbeat status byte), `MatchIndexes`, `VoteTally`. `raft_log_test` exists but is not wired into the build. See `docs/design/raft.md`.

**`src/proxy/`** — empty (planned client entry point forwarding to the leader; see `docs/design/proxy.md`).

**`src/status/`** — PostgreSQL client for querying a running instance:

- **`PgClient`** — Async PostgreSQL extended-protocol client over a raw TCP socket (not libpq). `connect(port)` synchronously establishes a connection. `prepare(sql)` synchronously sends a `Parse`/`Describe`/`Sync` sequence and returns a `PgStmt` handle. `async_exec(stmt, cb)` executes a prepared statement asynchronously and delivers `Result<Rows>` to the callback; multiple calls queue up and run one at a time. Only `trust` auth, loopback, `database=postgres`, no bind parameters; don't call the synchronous `prepare()` while an `async_exec` is in flight (they share read buffers). `close()` tears down the socket. Used in tests to verify replication state (e.g. row counts, LSNs) without needing an external psql binary.

### Data flow (end-to-end)

```
initdb
  └─▶  PgInstance::create_raft_wal(raft_dir)  ──(moves WAL)──▶  raft_dir/pg_wal/
                                                                        │
                 Fuse (FUSE)  ◀──(backed by)──────────────────────────┘
                    │  mounted at data_dir/pg_wal
                    ▼
               postgres write(WAL) ──▶ WalFuseOps::op_write
                                             │  WalFuseSegment::write() — validates XLog
                                             │  page headers (magic 0xD116, xlp_pageaddr)
                                             │  and calls WalTracker::async_publish()
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
- **`pg_wal_switch_test`** — integration test for WAL segment boundary handling. Same setup as `wal_sender_test` but calls `pg_switch_wal()` four times concurrently while pgbench is running, forcing the WalSender to follow WAL across multiple forced segment transitions. Requires PostgreSQL; auto-skips if absent.
- **`standby_wal_reset_test`** — integration test for standby reconnection after WAL loss. Runs a first pgbench batch and waits for the standby to catch up, then stops the standby and deletes all WAL segment files from its `pg_wal` directory. Runs a second pgbench batch while the standby is down, then restarts it. Verifies the WalSender re-streams the missing WAL from the earlier position and the standby converges to the final primary LSN. Requires PostgreSQL; auto-skips if absent.
- **`fuse_test`** — tests for `Fuse` and `FuseOps`. The majority of cases (lifecycle, passthrough reads/writes, getattr, xattr, rename, concurrency, etc.) run without PostgreSQL. Two cases (`wal_moved_to_raft_dir`, `wal_writes_to_backing_dir`) start a real PostgreSQL instance and skip if it's absent.
- **`wal_fuse_ops_test`** — unit tests for `WalFuseOps` and `WalFuseSegment` interaction. Exercises open/write/fsync/read/release ops against a `WalFuseOps` instance backed by a `TmpDir`. No real PostgreSQL needed.
- **`wal_state_test`** — unit tests for `WalFuseSegment::WalState` XLog parsing. Feeds real captured WAL bytes (from `wal_data.cc`) through `WalState::update()` and checks the returned `(offset, size, complete)` tuples. No real PostgreSQL needed.
- **`wal_shadow_test`** — integration test using `WalFuseOps` directly (via a `ShadowWalOps` subclass that mirrors writes into an in-memory buffer). Starts a real PostgreSQL instance, runs pgbench, then cross-checks the shadow buffer against the on-disk WAL bytes. Auto-skips if PostgreSQL is absent.
- **`pg_client_test`** — integration tests for `PgClient`. Starts a real PostgreSQL instance, exercises `connect`/`prepare`/`async_exec`. Requires PostgreSQL; auto-skips if absent.
