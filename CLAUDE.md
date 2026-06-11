# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

**pg-raft** is a high-availability system for PostgreSQL. The current code implements two layers:

1. **WAL interception** (`wal_fuse`) — a FUSE filesystem that mounts over `pg_wal` so that all WAL writes are transparently redirected to a separate `pg_raft_wal` directory under pg-raft's control.
2. **WAL streaming** (`wal_sender`) — a C++ server that speaks the PostgreSQL physical streaming replication protocol, capturing WAL from the FUSE write hook and streaming to a real PostgreSQL standby.

## Build system

This project uses [Boost.Build (b2)](https://www.boost.org/build/) — not CMake or Make. The `b2` binary is at `/opt/sparrow/bin/b2`. All external dependencies (Boost, libpq, PostgreSQL internals, lmdb, sqlite3, fuse) are installed under `/opt/pg_raft/` and `/usr/local/` (see `Jamroot.jam` for exact search paths).

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
./src/process/bin/clang-darwin-17/debug/cxxstd-latest-iso/process_test --run_test=TestCaseName

# Build with sanitizers
/opt/sparrow/bin/b2 address-sanitizer=norecover
/opt/sparrow/bin/b2 thread-sanitizer=norecover
```

Build artifacts land in `src/<module>/bin/clang-darwin-17/debug/cxxstd-latest-iso/`.

Compiler flags (from `Jamroot.jam`):
- `-Wall -Werror` always on; `-Wextra -Wconversion -Wuninitialized` in debug builds
- Static linking, C++latest standard, debug symbols always on
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
- **`Config`** — TOML-based system configuration (`config.toml`). Fields: `wdir`, `install_dir`, `pg_major_version`, `servers[]` (hostname + pg_port + ha_port), `pg_config`, `proxy_pg_port`/`proxy_http_port`, `s3_endpoints[]`.
- **`IoThreads`** — Manages a pool of threads running a shared `boost::asio::io_context`. Access the global context via `io_context()`; post work with `post(func)`.
- **`Int<T, L>`** — Strongly-typed integer wrapper template. Defined aliases: `RaftTerm` (uint64_t), `RaftIndex` (uint64_t), `RaftNodeId` (uint16_t). Use `INT(x)` or `.value()` to extract the underlying value. `std::atomic<Int<T,L>>` is specialized. `std::format("{:v}", x)` prints `TypeName(value)`; `"{}"` prints the bare integer.
- **`convert.h`** — Checked narrowing casts: `u8()`, `u4()`, `u2()`, `u1()`, `i8()`, `i4()`, `i2()`, `i1()`, `i<U>(v)`. All assert that the value fits in the target type.
- **`formatter.h`** — Specializes `std::formatter<std::filesystem::path>` so paths work directly in `std::format` calls.
- **`using.h`** — Project-wide type aliases bringing `std::` and `boost::` containers into the global namespace (`string`, `vector`, `map`, `unordered_map`, `unordered_flat_map`, `concurrent_unordered_map`, `circular_buffer`, `shared_ptr`, `Bytes`, etc.) and `asio` as an alias for `boost::asio`.
- **`base.h`** — Also exposes: `kWalBlockSize` (8192), `kWalSegmentSize` (16 MiB), `epoch_msec()` (wall-clock ms), `pcounter()`/`gcounter()` (monotonic counters), `tmp_dir()`/`tmp_file()` (temp path helpers), `system_error_code(errno)`.
- **`WalSegment`** (`wal_segment.h/cc`) — Shared, ref-counted mmap wrapper for a WAL segment file on disk. `open()` mmap's the file `PROT_READ|PROT_WRITE|MAP_SHARED` and inserts into a static `map<LogSeqNumber, Mapping>` so that multiple openers share the same mapping. `mapping()` returns the raw `unsigned char*` base pointer. `gc()` evicts mappings idle for > 5 s (called automatically on every `open`/destructor). Lives in `src/base/` and is used by both `wal_fuse` and `wal_sender`.
- **`testing.h`** — Test utilities: `TmpDir` (RAII temp directory, auto-deleted), `test_config()` (loads `config.toml`), `top_dir()`. Every test file must `#define BOOST_TEST_MODULE <name>` before including this header (enforced by a `#error`).
- **`SemaphoreLock`** (`semaphore_lock.h`) — RAII guard for `std::binary_semaphore`, analogous to `std::unique_lock`. Constructor does a non-blocking `try_acquire`; call `acquire()` to block if needed. `owns()` / `operator bool()` test ownership. Explicit `release()` available before destruction.

**`src/net/`** — Async networking layer built on Boost.Asio with optional TLS:

- **`Connection`** — Async framed-message connection over TCP or SSL (`socket_type = std::variant<tcp_socket, ssl_socket>`). Messages are framed with a 4-word header. `read_data()` / `write_data()` send `Data` objects (serialized via Boost.Serialization); `read_file()` / `write_file()` stream raw files directly from/to disk without buffering the full content in memory. All four are callback-based and safe to call from any thread; internal strand serializes all I/O. Status exchange (peer liveness) and read-timeout are managed automatically. Instantiate via `Connection::create<DerivedType>(...)`, which calls `start()` automatically. Override `on_shutdown()` to clean up.
- **`Server`** — Abstract TCP accept loop. Subclass and implement `start()`, `new_connection()`, `on_shutdown()`. Instantiate via `Server::create<DerivedType>(...)`. `shutdown()` cancels the acceptor.
- **`Data`** — Pool-backed scatter/gather I/O buffer (`kPageSize = 8192`). Implements `std::basic_streambuf` so it works with `std::ostream`/`std::istream`. Serialize with `to_data<T>(obj)` / deserialize with `from_data<T>(data)` using Boost.Serialization (binary by default).
- **`HostAddress`** — Plain `{address, port}` struct with serialization support and `to_string()`.
- **`async_connect`** / **`connect`** — Establish outbound TCP/SSL connections; both sync (`connect`) and async (`async_connect`) variants.

**`src/process/`** — Two classes:

- **`Process`** — wraps `fork`/`exec`. Manages child process lifecycle: `start()`, `stop()` (SIGTERM → SIGKILL), `kill()`, `wait()`, `try_wait()`.
- **`PgInstance`** — higher-level wrapper around `Process` for a full PostgreSQL instance. Constructor: `PgInstance(pg_bin, data_dir, port, role=Role::kStandby)`. `port` is always required and passed as `-p` to postgres. `Role` is `kPrimary` or `kStandby`; `promote()` updates it to `kPrimary`. Provides: `init(initdb_args)` (runs `initdb`), `create_raft_wal(raft_dir)` (creates `raft_dir/pg_wal`, moves initial WAL segments from `data_dir/pg_wal` into it, leaves `pg_wal` empty for the FUSE mount — call after `init()`, before `start()`), `write_config(map)` / `write_hba(lines)` (append to `postgresql.conf` / `pg_hba.conf`; `write_config` also injects required pg-raft defaults like `wal_sync_method` and `wal_recycle=off` unless overridden), `start()` / `stop(StopMode)` / `reload()` (SIGHUP) / `promote()` (SIGUSR1), `wait_ready(port)` (polls `pg_isready`). `StopMode` maps to `SIGTERM` (Smart), `SIGINT` (Fast), `SIGQUIT` (Immediate).

  Backup/restore: `backup(dest_dir)` runs a backup and returns a `SemaphoreLock` (true = success, false = failure). On primary it uses `pg_basebackup` (live); on standby it stops the instance and copies the data directory. At most one backup runs at a time — a concurrent call blocks until the in-progress backup finishes and then returns the same lock without re-running. `verify_backup(backup_dir, lk)` takes the lock returned by `backup()`, inspects `pg_controldata` output to determine backup type (shut-down state → `pg_checksums --check`; in-production state → `pg_verifybackup`), and returns the lock on success or releases it and returns a non-owning lock on failure. `restore(src)` / `restore_and_start(src)` copy a backup into `data_dir`.

**`src/wal_sender/`** — Two cooperating Asio-based classes:

- **`WalSender`** — Handles a single physical replication connection. Internally goes through three phases: **Startup** (handles PostgreSQL startup packet, SSL decline, auth-ok, parameter status); **Command** (responds to `IDENTIFY_SYSTEM`, `START_REPLICATION`, `TIMELINE_HISTORY`, `SHOW`, `SET`, simple `SELECT`); **Streaming** (enters `CopyBoth` mode, streams `XLogData` messages, sends keepalives every 5 s, handles `StandbyStatusUpdate`). Reads WAL directly from `wal_dir` on disk using `WalSegment` (the base mmap layer); segment filenames are derived from the LSN as `%08X%08X%08X` (timeline, seg-high, seg-low). Call `advance(flush_lsn)` from any thread (posted onto the strand) to notify that WAL is available. The caller is responsible for driving `advance()` — e.g. from a `WalFuseOps` fsync hook or by polling `pg_current_wal_flush_lsn()`. An optional `StatusCb` fires on each `StandbyStatusUpdate`. `shutdown()` cancels pending I/O.

- **`WalSenderServer`** — Async TCP accept loop. Creates one `WalSender` per incoming connection and calls a user-supplied `SenderCb` with it. `bound_port()` returns the actual port (useful when port `0` is passed). `shutdown()` cancels the acceptor; existing senders keep running.

  ```cpp
  auto server = WalSenderServer::create(
      port, wal_dir, system_id, timeline, initial_flush_lsn,
      [](shared_ptr<WalSender> s) { /* store s, call s->advance() later */ });
  server->start();
  server->shutdown();
  ```

**`src/wal_fuse/`** — FUSE-based interception layer for `pg_wal`. Primary classes:

- **`Fuse`** — Passthrough FUSE filesystem. Constructor: `Fuse(mount_point, unique_ptr<FuseOps>)`. Mounts at `mount_point` and delegates all file operations to the `FuseOps` instance (passed as `fuse_get_context()->private_data`). `start(timeout_ms)` mounts and blocks until the mount is live (detected via `st_dev` change); `stop()` unmounts and joins the loop thread. Runs the FUSE event loop in its own thread.

- **`FuseOps`** — Concrete FUSE operations base class. Constructed with a `backing_dir` path; every FUSE op is forwarded to that directory. Subclass and override virtual `op_*` methods to intercept specific operations. `static make_ops()` returns a `struct fuse_operations` whose entries are static trampolines that dispatch through `fuse_get_context()->private_data`. All ops expose `std::atomic<uint64_t> n_<opname>` counters (relaxed, observation-only) for testing.

- **`WalFuseOps`** (in `wal_fuse_ops.h/cc`) — `FuseOps` subclass that identifies WAL segment files by their 24-hex-char filename (TLI+LOGID+LOGSEG). On `op_open`, opens a `WalSegment` (the base mmap wrapper) and wraps it in a heap-allocated `WalFuseSegment`, storing the raw pointer directly in `fuse_file_info::fh` with a tagged bit (bit 63 set, with alignment disambiguation via bit 0) to distinguish WAL files from normal fds. On `op_release` the `WalFuseSegment` is deleted. Non-WAL files fall through to the `FuseOps` passthrough. There is no per-instance segments map — lifetime is tied to the open file handle.

- **`WalFuseSegment`** (in `wal_fuse_segment.h/cc`) — Owns a `shared_ptr<WalSegment>` (the mmap layer) and implements WAL parsing atop it. `write()` advances a parse cursor via the nested `WalState::update()` (returns `tuple<off_t, size_t, bool>` — cursor after the call, bytes consumed, and whether a complete XLogRecord was reached) and calls `WalTracker::async_publish()` for validated byte ranges. `fsync()` msyncs the dirty page range (from `synced_` to `next_`, page-aligned) and delegates to `WalTracker::async_fsync()`, blocking on the `std::future` result. After each fsync, `synced_` advances but keeps one page of overlap to re-cover any still-receiving writes.

- **`WalTracker`** — Interface for receiving WAL events. Has default (non-pure-virtual) no-op implementations. `async_publish(segment, offset, size)` is called as new WAL bytes are validated; `async_fsync(promise, segment, buf, size)` is called on `fsync` with a page-aligned dirty range; the implementor resolves the promise when the data is durable. The default `async_fsync` just calls `msync`.

Two additional executables live in `src/wal_fuse/` but are not part of any test suite: `debug_wal_ops` (interactive WAL operation debugger) and `wal_fuse_bench` (microbenchmark for FUSE throughput). Build them with `/opt/sparrow/bin/b2 src/wal_fuse//debug_wal_ops` or `//wal_fuse_bench`.

**`src/status/`** — PostgreSQL client for querying a running instance:

- **`PgClient`** — Async PostgreSQL extended-protocol client over a raw TCP socket (not libpq). `connect(port)` synchronously establishes a connection. `prepare(sql)` synchronously sends a `Parse`/`Describe`/`Sync` sequence and returns a `PgStmt` handle. `async_exec(stmt, cb)` executes a prepared statement asynchronously and delivers `Result<Rows>` to the callback; multiple calls queue up and run one at a time. `close()` tears down the socket. Used in tests to verify replication state (e.g. row counts, LSNs) without needing an external psql binary.

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
                                       WalSender::advance(flush_lsn)
                                             │  reads WAL from wal_dir via WalSegment mmap
                                       WalSenderServer  ──(wire protocol TCP)──▶  PostgreSQL standby
```

WAL segment files are memory-mapped on first open via `WalSegment` (base layer); the mapping is shared across all `WalFuseSegment` instances for the same LSN and is ref-counted with a 5-second idle GC. `WalFuseSegment::write()` parses XLog page and record headers inline and stops at the first stale or pre-zeroed page. macOS `F_FULLFSYNC` is not intercepted by FUSE, but regular `fsync` is — which is sufficient for PostgreSQL's WAL flush path.


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
