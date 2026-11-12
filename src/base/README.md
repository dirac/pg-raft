# base

Foundational types and utilities shared by every module. All other modules depend on `base`;
`base` depends on nothing in this repo.

## Headers included everywhere

`using.h` brings project-wide type aliases into the global namespace: `string`, `vector`, `map`,
`unordered_map`, `unordered_flat_map`, `concurrent_unordered_map`, `circular_buffer`,
`inplace_vector` (boost `static_vector`), `shared_ptr`, `Bytes` (`vector<uint8_t>`),
`MemoryPool<N>`, and `asio` as an alias for `boost::asio`. Include `base.h` to get these plus
everything else below.

`pch.h` is the precompiled header for the build. It defines `FUSE_USE_VERSION 26` and
`_FILE_OFFSET_BITS 64` before it includes `<fuse/fuse.h>`, so any translation unit compiled
through the PCH gets them automatically. It also includes `lmdb.h`. If an installed header changes
(for example after reinstalling LMDB), delete the `pch.pch` build artifacts so they are rebuilt.

## Key types

**`Result<T>`** — `std::expected<T, std::error_code>`. It can be constructed implicitly from
`std::error_code` or `boost::system::error_code`. Return `ok()` for a successful `Result<void>`.

**`ErrorCode`** (`error_code.h`) — pg-raft's error category: `kProgramBug`, and `kNotLeader` (a
raft server is not the leader, is stepping down, or has closed). Use
`make_error_code(ErrorCode::…)`.

**`Int<T, Label>`** — strongly typed integer wrapper, so that `RaftTerm` and `RaftIndex` can't be
mixed up even though both are `uint64_t`. The defined aliases are `RaftTerm`, `RaftIndex` and
`RaftNodeId` (`uint16_t`). Extract the value with the `INT(x)` macro or `.value()`.
`std::atomic<Int<T,L>>` is specialized. Format with `"{:v}"` for `TypeName(value)` or `"{}"` for
the bare integer.

**`LogSeqNumber`** — `{ uint32_t timeline; uint64_t lsn }`, ordered and serializable.
`log_seq_number(path)` parses a 24-hex-character WAL file name; `filename(lsn)` formats one.

## Utilities

**`logger`** — thread-safe, `std::format`-based logging to stderr. The constants `DEBUG`, `INFO`,
`WARN`, `ERROR`, `FATAL` are in scope through the header. Use `log(LEVEL, fmt, args...)`.
`elog(ec, fmt, args...)` logs an error code at Error level and returns it, which suits
`return elog(ec, "...")`. `logger::plog(prefix, line)` prefixes a line with a process name; it
relays child-process output.

**`IoThreads`** — `IoThreads(n)` or `IoThreads(ctx, n)` runs `n` threads on an `asio::io_context`
(the global one by default) and stops them on destruction. Access the global context with
`io_context()`, and post work to it with `post(func)`.

**`Config`** (`config.h`) — the TOML system configuration, loaded from the file given to
`Config(filename)` / `config(filename)`. Fields: `wdir`, `install_dir`, `pg_major_version`,
`servers[]` (`hostname`, `pg_port`, `ha_port`), `pg_config`, `proxy_pg_port`, `proxy_http_port`.

**`convert.h`** — checked narrowing casts (`u8`, `u4`, `u2`, `u1`, `i8`, `i4`, `i2`, `i1`,
`i<U>`). All of them assert that the value fits. Use them instead of `static_cast` whenever a
conversion narrows.

**`formatter.h`** — `std::formatter`s for `std::filesystem::path`, `LogSeqNumber`, error codes,
`std::variant`, `std::optional`, `vector` and `inplace_vector`, plus `parse_fn`, a base for
formatters that take no format arguments.

**`xxhash.h`** — `xxhash32(buffer | vector<const_buffer> | ptr, size | Bytes, seed)`. The hash of a
vector of buffers equals the hash of their concatenation. It is the frame checksum in `net` and
the page checksum of the raft log.

**`SemaphoreLock`** (`semaphore_lock.h`) — an RAII guard for `std::binary_semaphore`, like
`std::unique_lock`. The constructor does a non-blocking `try_acquire`; call `acquire()` to block.
`owns()` / `operator bool()` report ownership, and `release()` releases early.

**`base.h`** also exposes `kWalBlockSize` (8192), `kWalSegmentSize` (16 MiB), `epoch_msec()`
(wall-clock milliseconds), `pcounter()` / `gcounter()` (monotonic counters), `tmp_dir()` /
`tmp_file()`, `file_mode()`, and `system_error_code(errno)`.

## WAL segments

**`WalSegment`** (`wal_segment.h`) — a shared, ref-counted mmap of one 16 MiB WAL segment file:
`WalSegment(shared_ptr<WalSegmentCache>, dir, lsn)`.

- `open()` maps the file `PROT_READ|PROT_WRITE, MAP_SHARED` and registers the mapping in the
  `WalSegmentCache` (a mutex plus `map<LogSeqNumber, Mapping>`), so that segments opened through
  the same cache share one mapping. If the file `X` doesn't exist, it opens `.X` (an unlinked
  segment; see `WalFuseOps::op_unlink`).
- `mapping()` returns the base pointer, `write(buf, off, size)` copies into it,
  `const_buffer(off, size)` gives a view of it, and `fsync(off, size)` rounds the range out to
  whole pages and calls `msync`.
- On every `open()` and destruction, unreferenced mappings idle for at least 5 s are unmapped, and
  all unreferenced ones once more than 5 remain.

**`WalTracker`** (`wal_segment.h`) — receives WAL events from `wal_fuse`. It has default
implementations:

- `async_publish(segment, offset, size)` — called for WAL bytes as the FUSE parser walks over
  them (no-op by default).
- `async_fsync(segment, offset, size, completion)` — called on fsync. It must call
  `completion(Result<void>)` once the range is durable; the default does a local `msync`. The raft
  node implements this so that durability means a quorum (`WalSegment::kNoConsensus` reports
  "local only").

## Testing

`testing.h` provides `TmpDir` (an RAII temp directory, removed on destruction; `dir()` is its
path), `test_config()` (loads `config.toml`) and `top_dir()` (the repo root). Every test file must
`#define BOOST_TEST_MODULE <name>` before it includes this header; the header enforces this with
`#error`.

Tests: `semaphore_lock_test`, `xxhash_test` (known vectors and multi-buffer equivalence).
