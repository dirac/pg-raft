# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

**pg-raft** is a high-availability system for PostgreSQL. The current code implements two layers:

1. **WAL interception** (`wal_fuse`) — a FUSE filesystem that mounts over `pg_wal` so that all WAL writes are transparently redirected to a separate `pg_raft_wal` directory under pg-raft's control.
2. **WAL streaming** (`wal_sender`) — a C++ server that speaks the PostgreSQL physical streaming replication protocol, reading from `pg_raft_wal` and streaming to a real PostgreSQL standby.

## Build system

This project uses [Boost.Build (b2)](https://www.boost.org/build/) — not CMake or Make. The `b2` binary is at `/opt/sparrow/bin/b2`. All external dependencies (Boost, libpq, PostgreSQL internals, lmdb, sqlite3, fuse) are installed under `/opt/sparrow/` and `/usr/local/`.

All `b2` commands must be run from the repo root (`/Users/venkat/pg-raft`), which contains `Jamroot.jam`.

```bash
# Build everything
b2

# Build + run all tests
b2 test

# Build only a module
b2 src/process
b2 src/wal_sender
b2 src/wal_fuse

# Run a specific test target (specify by Jamfile target name)
b2 src/process//process_test
b2 src/wal_sender//wal_sender_test
b2 src/wal_sender//pg_replication_test
b2 src/wal_fuse//wal_fuse_test
```

Build artifacts land in `src/<module>/bin/clang-darwin-17/debug/cxxstd-latest-iso/`.

Compiler flags (from `Jamroot.jam`):
- `-Wall -Werror` always on; `-Wextra -Wconversion -Wuninitialized` in debug builds
- Static linking, C++latest standard, debug symbols always on
- `BOOST_ASIO_NO_DEPRECATED=1` — use only current Boost.Asio APIs

Include paths: the repo root (`.`) is on the include path, so headers are included as `#include "src/module/file.h"`.

## Architecture

### Modules

**`src/process/`** — `Process` class wrapping `fork`/`exec`. Manages child process lifecycle: `start()`, `stop()` (SIGTERM → SIGKILL), `kill()`, `wait()`, `try_wait()`. Used by tests to spawn real PostgreSQL processes.

**`src/wal_sender/`** — Three cooperating classes:

- **`WalSender`** — Handles a single physical replication connection. Speaks the PostgreSQL frontend/backend wire protocol (startup handshake, `IDENTIFY_SYSTEM`, `START_REPLICATION`). WAL data is enqueued via `send(WalSegment)` from any thread; `run()` blocks on the connection and drives the streaming loop. A pipe (`wakeup_[2]`) is used to wake the `poll()` loop when new data arrives.

- **`WalSenderServer`** — TCP accept loop. Binds a port, spawns a thread per connection, and calls a user-supplied `SenderCallback` with each new `WalSender`. Supports port `0` for OS-assigned ephemeral ports (used in tests).

- **`WalFileReader`** — Reads WAL segment files directly from a `pg_wal` directory. Polls for new content when the current segment isn't fully written yet. Invokes a `SegmentCallback` for each chunk and runs until `stop()` is called.

**`src/wal_fuse/`** — FUSE-based interception layer for `pg_wal`. Four cooperating classes:

- **`WalFuse`** — Passthrough FUSE filesystem. Mounts at a given `mount_point` and redirects all file operations to a `backing_dir`. Templated on an `Ops` policy that provides `make_ops()` returning a `struct fuse_operations`. Runs the FUSE event loop in its own thread.

- **`WalFuseOpBase`** — Concrete FUSE operations policy for `WalFuse`. Every FUSE op (open, read, write, readdir, …) is forwarded to the backing directory stored in `fuse_get_context()->private_data`.

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
           postgres writes WAL ──▶ pg_raft_wal/ (via FUSE passthrough)
                                         │
                                   WalFileReader   ──(WalSegment)──▶  WalSender  ──(wire protocol)──▶  PostgreSQL standby
                                         │                                  │
                                         └── runs in its own thread         └── WalSenderServer accepts the TCP connection
```

The integration tests wire these components together with real PostgreSQL processes.

### Key data types

```cpp
struct WalSegment { uint64_t start_lsn; std::vector<uint8_t> data; };
struct StandbyStatus { uint64_t written_lsn, flushed_lsn, applied_lsn; };
```

LSNs are `uint64_t` (high 32 bits = segment number, low 32 bits = offset). PostgreSQL timestamps are microseconds since 2000-01-01 (not Unix epoch).

## Tests

All integration tests require PostgreSQL 17 binaries at `/opt/pg_raft/pgsql/v17/bin` and auto-skip if absent.

- **`process_test`** — unit tests for `Process`.
- **`wal_sender_test`** — unit tests using a `MiniStandby` helper that speaks just enough of the replication protocol over a `socketpair`. No real PostgreSQL needed.
- **`pg_replication_test`** — integration test. Spins up a real primary, runs pgbench, streams WAL through `WalFileReader` + `WalSenderServer`, and verifies a real standby catches up.
- **`wal_fuse_test`** — integration test. Uses `PgInitializer` + `WalFuseManager` to start a real postgres instance with the FUSE layer active, generates WAL, and verifies WAL files land in `pg_raft_wal` rather than `pg_wal`.
