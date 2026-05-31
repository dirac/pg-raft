# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

**pg-raft** is a high-availability system for PostgreSQL. The current code implements two layers:

1. **WAL interception** (`wal_fuse`) — a FUSE filesystem that mounts over `pg_wal` so that all WAL writes are transparently redirected to a separate `pg_raft_wal` directory under pg-raft's control.
2. **WAL streaming** (`wal_sender`) — a C++ server that speaks the PostgreSQL physical streaming replication protocol, capturing WAL from the FUSE write hook and streaming to a real PostgreSQL standby.

## Build system

This project uses [Boost.Build (b2)](https://www.boost.org/build/) — not CMake or Make. The `b2` binary is at `/opt/sparrow/bin/b2`. All external dependencies (Boost, libpq, PostgreSQL internals, lmdb, sqlite3, fuse) are installed under `/opt/sparrow/` and `/usr/local/`.

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

# Run a specific test target (specify by Jamfile target name)
/opt/sparrow/bin/b2 src/process//process_test
/opt/sparrow/bin/b2 src/wal_sender//wal_sender_test
/opt/sparrow/bin/b2 src/wal_sender//pg_replication_test
/opt/sparrow/bin/b2 src/wal_fuse//wal_fuse_test
/opt/sparrow/bin/b2 src/net//async_connect_test
/opt/sparrow/bin/b2 src/net//data_test
/opt/sparrow/bin/b2 src/net//connection_test

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

## Architecture

### Modules

**`src/base/`** — Foundational utilities used by all other modules:

- **`logger`** — Thread-safe, `std::format`-based logging to stderr. Levels: `Debug`, `Info`, `Warn`, `Error`. Default level is `Debug`. Call `logger::set_level()` to suppress noise in tests. Free functions `debug()`, `info()`, `warn()`, `error()` are available globally via `using.h`.
- **`Result<T>`** — `std::expected<T, std::error_code>` wrapper with implicit construction from both `std::error_code` and `boost::system::error_code`. Use `ok()` to return a successful `Result<void>`.
- **`Config`** — TOML-based system configuration (`config.toml`). Fields: `wdir`, `install_dir`, `pg_major_version`, `servers[]` (hostname + pg_port + ha_port), `pg_config`, `proxy_pg_port`/`proxy_http_port`, `s3_endpoints[]`.
- **`IoThreads`** — Manages a pool of threads running a shared `boost::asio::io_context`. Access the global context via `io_context()`; post work with `post(func)`.
- **`Int<T, L>`** — Strongly-typed integer wrapper template. Defined aliases: `RaftTerm` (uint64_t), `RaftIndex` (uint64_t), `RaftNodeId` (uint16_t). Use `INT(x)` macro to extract the underlying value. `std::atomic<Int<T,L>>` is specialized.
- **`convert.h`** — Checked narrowing casts: `u8()`, `u4()`, `u2()`, `u1()`, `i8()`, `i4()`, `i2()`, `i1()`, `i<U>(v)`. All assert that the value fits in the target type.
- **`using.h`** — Project-wide type aliases bringing `std::` and `boost::` containers into the global namespace (`string`, `vector`, `map`, `unordered_map`, `unordered_flat_map`, `concurrent_unordered_map`, `circular_buffer`, `shared_ptr`, etc.) and `asio` as an alias for `boost::asio`.
- **`testing.h`** — Test utilities: `TmpDir` (RAII temp directory, auto-deleted), `test_config()` (loads `config.toml`), `top_dir()`. Every test file must `#define BOOST_TEST_MODULE <name>` before including this header (enforced by a `#error`).

**`src/net/`** — Async networking layer built on Boost.Asio with optional TLS:

- **`Connection`** — Async framed-message connection over TCP or SSL (`socket_type = std::variant<tcp_socket, ssl_socket>`). Messages are framed with a 4-word header. `read_data()` / `write_data()` are callback-based and safe to call from any thread; internal strand serializes all I/O. Status exchange (peer liveness) and read-timeout are managed automatically. Instantiate via `Connection::create<DerivedType>(...)`, which calls `start()` automatically. Override `on_shutdown()` to clean up.
- **`Server`** — Abstract TCP accept loop. Subclass and implement `start()`, `new_connection()`, `on_shutdown()`. Instantiate via `Server::create<DerivedType>(...)`. `shutdown()` cancels the acceptor.
- **`Data`** — Pool-backed scatter/gather I/O buffer (`kPageSize = 8192`). Implements `std::basic_streambuf` so it works with `std::ostream`/`std::istream`. Serialize with `to_data<T>(obj)` / deserialize with `from_data<T>(data)` using Boost.Serialization (binary by default).
- **`HostAddress`** — Plain `{address, port}` struct with serialization support and `to_string()`.
- **`async_connect`** / **`connect`** — Establish outbound TCP/SSL connections; both sync (`connect`) and async (`async_connect`) variants.

**`src/process/`** — Two classes:

- **`Process`** — wraps `fork`/`exec`. Manages child process lifecycle: `start()`, `stop()` (SIGTERM → SIGKILL), `kill()`, `wait()`, `try_wait()`.
- **`PgInstance`** — higher-level wrapper around `Process` for a full PostgreSQL instance. Provides: `init()` (runs `initdb`), `write_config()` / `write_hba()` (append to `postgresql.conf` / `pg_hba.conf`), `start()` / `stop(StopMode)` / `reload()` (SIGHUP) / `promote()` (SIGUSR1), `wait_ready()` (polls `pg_isready`), and `backup()` / `restore()` / `restore_and_start()` (recursive `cp -Rp`). `StopMode` maps to `SIGTERM` (Smart), `SIGINT` (Fast), `SIGQUIT` (Immediate).

**`src/wal_sender/`** — Two cooperating classes:

- **`WalSender`** — Handles a single physical replication connection. Speaks the PostgreSQL frontend/backend wire protocol (startup handshake, `IDENTIFY_SYSTEM`, `START_REPLICATION`). WAL data is enqueued via `send(WalSegment)` from any thread; `run()` blocks on the connection and drives the streaming loop. A pipe (`wakeup_[2]`) is used to wake the `poll()` loop when new data arrives. A `ReplicationStartCallback` fires once when `START_REPLICATION` is received, letting callers drain buffered WAL and register themselves as the live producer.

- **`WalSenderServer`** — TCP accept loop. Binds a port, spawns a thread per connection, and calls a user-supplied `SenderCallback` with each new `WalSender`. Supports port `0` for OS-assigned ephemeral ports (used in tests).

**`src/wal_fuse/`** — FUSE-based interception layer for `pg_wal`. Four cooperating classes:

- **`WalFuse`** — Passthrough FUSE filesystem. Mounts at a given `mount_point` and redirects all file operations to a `backing_dir`. The constructor is templated on an `Ops` policy that provides a static `make_ops()` returning a `struct fuse_operations`; pass an `Ops{}` instance as the third argument for type deduction only. The resulting `fuse_operations` is stored by value. Runs the FUSE event loop in its own thread.

- **`WalFuseOpBase`** — Concrete FUSE operations policy for `WalFuse`. Every FUSE op (open, read, write, readdir, …) is forwarded to the backing directory stored in `fuse_get_context()->private_data`. Tests supply a derived policy (e.g. `WalSendingOps`) that wraps `WalFuseOpBase::make_ops()` and overrides specific ops.

- **`WalFuseManager`** — Orchestrates startup: mounts `WalFuse` over `$PGDATA/pg_wal` (backed by `$PGDATA/pg_raft_wal`), waits for the mount to become live, then starts the `postgres` process via `Process`. `stop()` reverses the sequence.

- **`PgInitializer`** — One-shot setup helper. Runs `initdb`, creates the `pg_raft_wal` directory, and moves the initial WAL segments out of `pg_wal` into `pg_raft_wal`, leaving `pg_wal` as an empty directory ready for the FUSE mount.

### Data flow (end-to-end)

```
initdb
  └─▶  PgInitializer  ──(moves WAL)──▶  pg_raft_wal/
                                               │
             WalFuse (FUSE)  ◀──(backs)────────┘
                │  mounted at pg_wal
                ▼
           postgres write(WAL) ──▶ FUSE op_write (WalSendingOps)
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

The integration tests wire these components together with real PostgreSQL processes.

### Key data types

```cpp
struct WalSegment { uint64_t start_lsn; std::vector<uint8_t> data; };
struct StandbyStatus { uint64_t written_lsn, flushed_lsn, applied_lsn; };
```

LSNs are `uint64_t` (high 32 bits = segment number, low 32 bits = offset). PostgreSQL timestamps are microseconds since 2000-01-01 (not Unix epoch).

### FUSE include requirements

Any file that includes `<fuse/fuse.h>` must define these macros first:

```cpp
#define FUSE_USE_VERSION 26
#define _FILE_OFFSET_BITS 64
```

## Tests

All integration tests require PostgreSQL 17 binaries at `/opt/pg_raft/pgsql/v17/bin` and auto-skip if absent.

- **`process_test`** — unit tests for `Process`.
- **`wal_sender_test`** — unit tests using a `MiniStandby` helper that speaks just enough of the replication protocol over a `socketpair`. No real PostgreSQL needed.
- **`pg_replication_test`** — end-to-end integration test. Uses `PgInitializer` to set up the WAL directories, `PgInstance` to manage primary and standby processes, mounts `WalFuse` with a custom `WalSendingOps` policy that accumulates WAL per-file and enqueues on fsync, starts a `WalSenderServer`, seeds the standby via cold backup (`PgInstance::backup()`/`restore()`, i.e. `cp -Rp`), runs pgbench, then verifies the standby catches up via LSN comparison and row count equality. WAL capture is enabled only after the primary restarts post-backup, since the standby already has pre-backup WAL in its local `pg_wal`.
- **`wal_fuse_test`** — integration test. Uses `PgInitializer` + `WalFuseManager` to start a real postgres instance with the FUSE layer active, generates WAL, and verifies WAL files land in `pg_raft_wal` rather than `pg_wal`.
