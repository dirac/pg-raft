# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

**pg-raft** is a high-availability system for PostgreSQL. The current code implements the WAL streaming layer: a C++ server that speaks the PostgreSQL physical streaming replication protocol, allowing a real PostgreSQL standby to replicate from it instead of directly from the primary.

## Build system

This project uses [Boost.Build (b2)](https://www.boost.org/build/) — not CMake or Make. The `b2` binary is at `/opt/sparrow/bin/b2`. All external dependencies (Boost, libpq, PostgreSQL internals, lmdb, sqlite3) are installed under `/opt/sparrow/`.

All `b2` commands must be run from the repo root (`/Users/venkat/pg-raft`), which contains `Jamroot.jam`.

```bash
# Build everything
b2

# Build + run all tests
b2 test

# Build only the process module
b2 src/process

# Run a specific test target (specify by Jamfile target name)
b2 src/process//process_test
b2 src/wal_sender//wal_sender_test
b2 src/wal_sender//pg_replication_test
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

### Data flow (end-to-end)

```
PostgreSQL pg_wal dir
        │
   WalFileReader   ──(WalSegment)──▶  WalSender  ──(wire protocol)──▶  PostgreSQL standby
        │                                  │
        └── runs in its own thread         └── WalSenderServer accepts the TCP connection
```

The integration test in `pg_replication_test.cc` wires all three components together with real PostgreSQL processes (primary + standby + pgbench load).

### Key data types

```cpp
struct WalSegment { uint64_t start_lsn; std::vector<uint8_t> data; };
struct StandbyStatus { uint64_t written_lsn, flushed_lsn, applied_lsn; };
```

LSNs are `uint64_t` (high 32 bits = segment number, low 32 bits = offset). PostgreSQL timestamps are microseconds since 2000-01-01 (not Unix epoch).

## Tests

- **`wal_sender_test`** — unit tests using a `MiniStandby` helper that speaks just enough of the replication protocol over a `socketpair`. No real PostgreSQL needed.
- **`pg_replication_test`** — integration test. Requires PostgreSQL 17 binaries at `/opt/pg_raft/pgsql/v17/bin`. The test auto-skips if binaries are absent. Spins up a real primary, runs pgbench, streams WAL through `WalFileReader` + `WalSenderServer`, and verifies a real standby catches up.
- **`process_test`** — unit tests for `Process`.
