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

# Build + run a specific unit-test target (auto-runs after build)
/opt/sparrow/bin/b2 src/process//process_test
/opt/sparrow/bin/b2 src/process//pg_instance_test
/opt/sparrow/bin/b2 src/wal_sender//wal_sender_test
/opt/sparrow/bin/b2 src/wal_fuse//wal_fuse_test
/opt/sparrow/bin/b2 src/net//async_connect_test
/opt/sparrow/bin/b2 src/net//data_test
/opt/sparrow/bin/b2 src/net//connection_test

# pg_replication_test is declared as exe (not unit-test) — b2 only builds it:
/opt/sparrow/bin/b2 src/wal_sender//pg_replication_test
# Run it manually:
./src/wal_sender/bin/clang-darwin-17/debug/cxxstd-latest-iso/pg_replication_test

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
- **`testing.h`** — Test utilities: `TmpDir` (RAII temp directory, auto-deleted), `test_config()` (loads `config.toml`), `top_dir()`. Every test file must `#define BOOST_TEST_MODULE <name>` before including this header (enforced by a `#error`).

**`src/net/`** — Async networking layer built on Boost.Asio with optional TLS:

- **`Connection`** — Async framed-message connection over TCP or SSL (`socket_type = std::variant<tcp_socket, ssl_socket>`). Messages are framed with a 4-word header. `read_data()` / `write_data()` are callback-based and safe to call from any thread; internal strand serializes all I/O. Status exchange (peer liveness) and read-timeout are managed automatically. Instantiate via `Connection::create<DerivedType>(...)`, which calls `start()` automatically. Override `on_shutdown()` to clean up.
- **`Server`** — Abstract TCP accept loop. Subclass and implement `start()`, `new_connection()`, `on_shutdown()`. Instantiate via `Server::create<DerivedType>(...)`. `shutdown()` cancels the acceptor.
- **`Data`** — Pool-backed scatter/gather I/O buffer (`kPageSize = 8192`). Implements `std::basic_streambuf` so it works with `std::ostream`/`std::istream`. Serialize with `to_data<T>(obj)` / deserialize with `from_data<T>(data)` using Boost.Serialization (binary by default).
- **`HostAddress`** — Plain `{address, port}` struct with serialization support and `to_string()`.
- **`async_connect`** / **`connect`** — Establish outbound TCP/SSL connections; both sync (`connect`) and async (`async_connect`) variants.

**`src/process/`** — Two classes:

- **`Process`** — wraps `fork`/`exec`. Manages child process lifecycle: `start()`, `stop()` (SIGTERM → SIGKILL), `kill()`, `wait()`, `try_wait()`.
- **`PgInstance`** — higher-level wrapper around `Process` for a full PostgreSQL instance. Constructor: `PgInstance(pg_bin, data_dir, raft_dir, extra_args)`. Provides: `init()` (runs `initdb`), `create_raft_wal()` (creates `raft_dir/pg_wal`, moves initial WAL segments from `data_dir/pg_wal` into it, leaves `pg_wal` empty for the FUSE mount — call after `init()`, before `start()`), `write_config(map)` / `write_hba(lines)` (append to `postgresql.conf` / `pg_hba.conf`; `write_config` also injects required pg-raft defaults like `wal_sync_method` and `wal_recycle=off` unless overridden), `start()` / `stop(StopMode)` / `reload()` (SIGHUP) / `promote()` (SIGUSR1), `wait_ready(port)` (polls `pg_isready`), and `backup()` / `restore()` / `restore_and_start()` (recursive copy). `StopMode` maps to `SIGTERM` (Smart), `SIGINT` (Fast), `SIGQUIT` (Immediate).

**`src/wal_sender/`** — Two cooperating classes:

- **`WalSender`** — Handles a single physical replication connection. Speaks the PostgreSQL frontend/backend wire protocol (startup handshake, `IDENTIFY_SYSTEM`, `START_REPLICATION`). WAL data is enqueued via `send(WalSegment)` from any thread; `run()` blocks on the connection and drives the streaming loop. A pipe (`wakeup_[2]`) is used to wake the `poll()` loop when new data arrives. A `ReplicationStartCallback` fires once when `START_REPLICATION` is received, letting callers drain buffered WAL and register themselves as the live producer.

- **`WalSenderServer`** — TCP accept loop. Binds a port, spawns a thread per connection, and calls a user-supplied `SenderCallback` with each new `WalSender`. Supports port `0` for OS-assigned ephemeral ports (used in tests).

**`src/wal_fuse/`** — FUSE-based interception layer for `pg_wal`. Two primary classes:

- **`WalFuse`** — Passthrough FUSE filesystem. Constructor: `WalFuse(mount_point, unique_ptr<WalFuseOps>)`. Mounts at `mount_point` and delegates all file operations to the `WalFuseOps` instance (passed as `fuse_get_context()->private_data`). `start(timeout_ms)` mounts and blocks until the mount is live (detected via `st_dev` change); `stop()` unmounts and joins the loop thread. Runs the FUSE event loop in its own thread.

- **`WalFuseOps`** — Concrete FUSE operations base class. Constructed with a `backing_dir` path; every FUSE op is forwarded to that directory. Subclass and override virtual `op_*` methods to intercept specific operations. `static make_ops()` returns a `struct fuse_operations` whose entries are static trampolines that dispatch through `fuse_get_context()->private_data`. Tests supply a derived class (e.g. `WalSendingOps` in `pg_replication_test.cc`) that overrides `op_write` and `op_fsync` to capture WAL.

**`src/raft/`** and **`src/proxy/`** — Reserved directories for future modules; currently empty and not referenced in `src/Jamfile.jam`.

### Data flow (end-to-end)

```
initdb
  └─▶  PgInstance::create_raft_wal()  ──(moves WAL)──▶  pg_raft_wal/
                                                               │
                 WalFuse (FUSE)  ◀──(backs)────────────────────┘
                    │  mounted at pg_wal
                    ▼
               postgres write(WAL) ──▶ FUSE op_write (WalSendingOps subclass)
                                             │  accumulates bytes in WalFileAccum
                                             │  (per WAL segment file, keyed by FUSE path)
                                             ▼
                                       postgres fsync ──▶ FUSE op_fsync → flush_wal_accum()
                                             │  validates XLog page header magic (0xD116)
                                             │  and xlp_pageaddr before building WalSegment
                                             ▼
                                       WalSender.send(WalSegment)
                                             │
                                       WalSenderServer  ──(wire protocol TCP)──▶  PostgreSQL standby
```

WAL bytes are accumulated per-file in `WalFileAccum` on each FUSE `write`, then validated and delivered as a single `WalSegment` on `fsync`. Validation walks pages checking magic (`0xD116`) and `xlp_pageaddr`; it stops at the first stale or pre-zeroed page. macOS `F_FULLFSYNC` is not intercepted by FUSE, but regular `fsync` is — which is sufficient for PostgreSQL's WAL flush path.

The accumulation and validation logic lives in the test (`pg_replication_test.cc`) as local helpers (`WalFileAccum`, `accum_wal_write`, `flush_wal_accum`), not in shared library code.

### Key data types

```cpp
// In src/wal_sender/wal_sender.h
struct WalSegment { uint64_t start_lsn; std::vector<uint8_t> data; };
struct StandbyStatus { uint64_t written_lsn, flushed_lsn, applied_lsn; };
```

LSNs are `uint64_t` (high 32 bits = segment number, low 32 bits = offset). PostgreSQL timestamps are microseconds since 2000-01-01 (not Unix epoch).

### FUSE include requirements

`src/base/pch.h` already defines `FUSE_USE_VERSION 26` and `_FILE_OFFSET_BITS 64` before including `<fuse/fuse.h>`, so files compiled through the PCH get these automatically. Any file that includes `<fuse/fuse.h>` **outside** the PCH must define them first:

```cpp
#define FUSE_USE_VERSION 26
#define _FILE_OFFSET_BITS 64
```

## Tests

Integration tests in `wal_fuse_test` and `pg_instance_test` use PostgreSQL at `/opt/pg_raft/postgresql/17/bin`; those in `pg_replication_test` use `/opt/pg_raft/pgsql/v17/bin`. Both auto-skip if the binaries are absent.

- **`process_test`** — unit tests for `Process`.
- **`pg_instance_test`** — integration tests for `PgInstance`: init, write_config, write_hba, create_raft_wal, start/stop, backup/restore.
- **`wal_sender_test`** — unit tests using a `MiniStandby` helper that speaks just enough of the replication protocol over a `socketpair`. No real PostgreSQL needed.
- **`pg_replication_test`** — end-to-end integration test. Uses `PgInstance::create_raft_wal()` to set up WAL directories, manages primary and standby processes, mounts `WalFuse` with a local `WalSendingOps` subclass that accumulates WAL per-file and enqueues on fsync, starts a `WalSenderServer`, seeds the standby via cold backup (`backup()`/`restore()`), runs pgbench, then verifies the standby catches up via LSN comparison and row count equality. WAL capture is enabled only after the primary restarts post-backup, since the standby already has pre-backup WAL in its local `pg_wal`.
- **`wal_fuse_test`** — integration test. Uses `PgInstance::create_raft_wal()` + `WalFuse` to start a real postgres instance with the FUSE layer active, generates WAL, and verifies WAL files land in `pg_raft_wal` rather than `pg_wal`. Note: `WalMovedToRaftDir` and `WalWritesToBackingDir` are marked `*boost::unit_test::disabled()` and never run automatically; only `ReadDirLargeDirectory` (no PostgreSQL needed) runs in CI.
