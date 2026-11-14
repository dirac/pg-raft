# `base` — Shared types and utilities

Source: `src/base/`. Every other module depends on `base`; `base` depends on nothing in this repo.

## 1. Headers included everywhere

- `using.h` brings project-wide type aliases into the global namespace: `string`, `vector`,
  `map`, `unordered_map`, `unordered_flat_map`, `concurrent_unordered_map`, `circular_buffer`,
  `inplace_vector` (boost `static_vector`), `shared_ptr`, `Bytes` (`vector<uint8_t>`),
  `MemoryPool<N>`, and `asio` as an alias for `boost::asio`. Include `base.h` to get these plus
  everything below.
- `pch.h` is the precompiled header. It defines `FUSE_USE_VERSION 26` and `_FILE_OFFSET_BITS 64`
  before it includes `<fuse/fuse.h>`, so every translation unit compiled through the PCH gets them.
  It also includes `lmdb.h` and `<boost/json.hpp>`. If an installed header changes (for example
  after reinstalling LMDB), delete the `pch.pch` build artifacts so that they are rebuilt.

## 2. Types

| Type | Header | What |
|---|---|---|
| `Result<T>` | `base.h` | `std::expected<T, std::error_code>`, constructible from `std::error_code` or `boost::system::error_code`. `ok()` is a successful `Result<void>`. |
| `ErrorCode` | `error_code.h` | pg-raft's error category: `kProgramBug`, and `kNotLeader` (a raft server is not the leader, is stepping down, or has closed). `make_error_code(ErrorCode::…)`. |
| `Int<T, Label>` | `int_type.h` | A strongly typed integer, so that `RaftTerm` and `RaftIndex` (both `uint64_t`) can't be mixed up. Aliases `RaftTerm`, `RaftIndex`, `RaftNodeId` (`uint16_t`). `INT(x)` or `.value()` extracts the value; `std::atomic<Int<T,L>>` is specialized; `"{:v}"` formats as `TypeName(value)`, `"{}"` as the bare integer. |
| `LogSeqNumber` | `base.h` | `{uint32_t timeline; uint64_t lsn}`, ordered by timeline, then LSN. `log_seq_number(path)` parses a 24-hex-character WAL file name (the segment's start LSN); `filename(lsn)` formats one. |
| `Config` | `config.h` | The TOML configuration (below). |

## 3. Configuration (`config.toml`)

`Config(filename)` or `config(filename)` (which logs). `config_test` covers the parsing.

| Key | Field | Default |
|---|---|---|
| `dir` | `wdir`: the nodes' directories go under it | |
| | `install_dir` | `/opt/pg_raft/postgresql` |
| | `pg_major_version` (the binaries are `install_dir/<version>/bin`) | `17` |
| `[[server]]` `hostname`, `pg_port`, `ha_port` | `servers[i]`: node `i` | |
| `[pg_config]` | `pg_config`: settings written into `postgresql.conf` by `pg_raft_initdb` | |
| `pg_hba` | lines written into `pg_hba.conf` by `pg_raft_initdb`, ahead of pg-raft's defaults (loopback trusted, other addresses `scram-sha-256`), e.g. `["host all all 10.0.0.0/16 trust"]` | `[]` |
| `backup_store` | the backup store's directory ([backup.md](backup.md)) | |
| `backup_interval_secs` | when a standby takes the next backup | `21600` (6 h) |
| `backup_wal_size` | or after this much WAL, in bytes | 4 GiB |
| `backup_pending_timeout_secs` | how long a backup lease counts without a refresh | `120` |
| `socket_dir` | the WAL sender's Unix socket directory (macOS limits socket paths to 104 bytes) | `/tmp` |
| `proxy_pg_port`, `proxy_http_port` | the proxy's primary port and admin port ([proxy.md](proxy.md)) | `5432`, `8080` |
| `leader_sync` | the leader msyncs its WAL (PostgreSQL's `pg_wal` and raft's copy) before appending the `FlushOp` | `false` |
| `follower_sync` | a follower msyncs the WAL blocks it receives before acknowledging them (raft `Options::follower_sync`), and its standby's fsync msyncs | `false` |
| `write_op_bytes` | on the leader, the WAL written since the last flush goes with the next flush (one raft entry per fsync), or as a `WriteOp` of its own as soon as this many bytes of a segment are pending | `65536` |
| `frame_checksums` | xxhash32 of every network frame raft sends; TCP checksums the segments already (the raft log's entries carry a CRC-32C, checked by `RaftLog::verify()`) | `false` |
| `log_sync` | the raft log (LMDB) is synced on every commit (raft `Options::sync_commits`); off, it is `MDB_NOSYNC` and synced in the background within a second | `false` |

With the three syncs off (the default), a commit is durable once a majority has the WAL in its
page cache, written through the mmap: durability comes from replication, and the kernel writes
the pages back on its own. Only a power loss of a majority at once loses acknowledged commits.

## 4. Utilities

- **`logger`** — thread-safe logging to stderr. `log(LEVEL, fmt, args...)` with `DEBUG`, `INFO`,
  `WARN`, `ERROR`, `FATAL` in scope; `elog(ec, fmt, args...)` logs an error code at Error level and
  returns it (`return elog(ec, "...")`); `logger::plog(prefix, line)` relays a child process's
  output. The message is formatted into a thread-local buffer; only the short line prefix goes
  through `std::format` in `_log`, the message is appended (formatting a long message there
  overflowed a stack buffer in libc++'s `vformat_to`). Default level `Info`, `logger::set_level()`.
- **`IoThreads`** — `IoThreads(n)` or `IoThreads(ctx, n)` runs `n` threads on an
  `asio::io_context` (the global one by default) and stops them on destruction. `io_context()` is
  the global context, `post(func)` posts to it.
- **`convert.h`** — checked narrowing casts (`u8`, `u4`, `u2`, `u1`, `i8`, `i4`, `i2`, `i1`,
  `i<U>`) that assert the value fits. Use them instead of `static_cast` whenever a conversion
  narrows (the debug build has `-Wconversion -Werror`).
- **`formatter.h`** — `std::formatter`s for `std::filesystem::path`, `LogSeqNumber` (`tli/X/Y`),
  error codes, `std::variant`, `std::optional`, `vector` and `inplace_vector`, and `parse_fn`, a
  base for formatters without format arguments.
- **`xxhash.h`** — `xxhash32(buffer | vector<const_buffer> | ptr, size | Bytes, seed)`; the hash
  of a vector of buffers equals the hash of their concatenation. The frame checksum of `net` and
  the page checksum of the raft log.
- **`SemaphoreLock`** (`semaphore_lock.h`) — an RAII guard for `std::binary_semaphore`, like
  `std::unique_lock`: the constructor tries to acquire without blocking, `acquire()` blocks,
  `owns()` / `operator bool()` report ownership, `release()` releases early.
- **`base.h`** also has `kWalBlockSize` (8192), `kWalSegmentSize` (16 MiB), `epoch_msec()`
  (wall-clock milliseconds), `pcounter()` / `gcounter()` (monotonic counters), `tmp_dir()` /
  `tmp_file()`, `file_mode()` and `system_error_code(errno)`.

## 5. WAL segments (`wal_segment.h`)

- **`WalSegment(shared_ptr<WalSegmentCache>, dir, lsn)`** — a shared, ref-counted mmap of one
  16 MiB segment file. `open()` maps it `PROT_READ|PROT_WRITE, MAP_SHARED` and registers the
  mapping in the cache (a mutex and a `map<LogSeqNumber, Mapping>`), so that everything opened
  through one cache shares a mapping; if `X` doesn't exist it opens `.X` (an unlinked segment, see
  `WalFuseOps::op_unlink`). `mapping()` is the base pointer, `write(buf, off, size)` copies into it,
  `const_buffer(off, size)` is a view, `fsync(off, size)` rounds the range out to pages and
  `msync`s. On every open and destruction, unreferenced mappings idle for at least 5 s are
  unmapped, and all unreferenced ones once more than 5 remain. Used by `wal_fuse` and `raft`.
  The cache is keyed by LSN: a file replaced while a mapping of it is cached must not be opened
  through that cache again (the node unmounts FUSE before it rebuilds `pg/pg_wal`).
- **`WalTracker`** — the receiver of what the FUSE layer sees, with default implementations:
  - `async_publish(segment, offset, size)`: WAL bytes walked by the FUSE parser (no-op);
  - `async_timeline(timeline, content)`: PostgreSQL installed a timeline history file (no-op);
  - `async_fsync(segment, offset, size, completion)`: an fsync; `completion(Result<void>)` must be
    called once the range is durable (default: a local `msync`). `WalSegment::kNoConsensus`
    (`ECANCELED`) reports "not on a quorum".

  The node's implementation is `NodeWalTracker` ([node.md](node.md#21-leader-postgresql-is-the-primary)).
- **History files** — `history_timeline(path)` parses a `%08X.history` name, `history_filename(tli)`
  builds one, `write_history_file(dir, tli, content)` writes one atomically (temporary name, fsync,
  rename), `read_history_file(dir, tli)` reads one, `latest_history_timeline(dir)` finds the
  highest. Raft writes them when it applies a `TimelineOp`; `WalSender` reads them.

## 6. Testing

`testing.h` provides `TmpDir` (an RAII temporary directory, removed on destruction; `tmp()` is its
path), `test_config()` (loads `src/base/config.toml`) and `top_dir()` (the repo root). Every test
file must `#define BOOST_TEST_MODULE <name>` before it includes the header (a `#error` enforces
it).

Tests: `semaphore_lock_test`, `xxhash_test` (known vectors and multi-buffer equivalence),
`config_test` (the node settings and their defaults).
