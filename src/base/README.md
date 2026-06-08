# base

Foundational types and utilities shared by every module. All other modules depend on `base`; `base` depends on nothing in this repo.

## Headers included everywhere

`using.h` pulls project-wide type aliases into the global namespace: `string`, `vector`, `map`, `unordered_map`, `unordered_flat_map`, `concurrent_unordered_map`, `circular_buffer`, `shared_ptr`, `Bytes` (`vector<uint8_t>`), and `asio` as an alias for `boost::asio`. Include `base.h` to get these plus everything else below.

`pch.h` is the precompiled header for the build. It defines `FUSE_USE_VERSION 26` and `_FILE_OFFSET_BITS 64` before including `<fuse/fuse.h>`, so any translation unit compiled through the PCH gets them automatically.

## Key types

**`Result<T>`** — `std::expected<T, std::error_code>` with implicit construction from both `std::error_code` and `boost::system::error_code`. Return `ok()` for a successful `Result<void>`.

**`Int<T, Label>`** — Strongly-typed integer wrapper. Prevents accidental mixing of e.g. `RaftTerm` and `RaftIndex` even though both are `uint64_t` underneath. Defined aliases: `RaftTerm`, `RaftIndex`, `RaftNodeId`. Extract the value with `INT(x)` macro or `.value()`. `std::atomic<Int<T,L>>` is specialized. Format with `"{:v}"` for `TypeName(value)` or `"{}"` for the bare integer.

**`LogSeqNumber`** — `{ uint32_t timeline; uint64_t lsn }`. The `lsn` field encodes segment number × `kWalSegmentSize` in its high 32 bits; offset within the segment in its low 32 bits.

## Utilities

**`logger`** — Thread-safe, `std::format`-based logging to stderr. Constants `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL` are in scope via the header. Use `log(LEVEL, fmt, args...)` globally. `logger::elog(ec, fmt, args...)` logs an `std::error_code` at Error level and returns it (useful in `return logger::elog(ec, "...")` patterns). `logger::plog(prefix, line)` prefixes a line with a process name tag — used to relay child-process output.

**`IoThreads`** — Constructs a thread pool all sharing one `boost::asio::io_context`. Access the global context via `io_context()`; post work with `post(func)`. Constructed once near `main()`; the global context is available from any thread thereafter.

**`Config`** — Loads `config.toml` from the working directory. Fields: `wdir`, `install_dir`, `pg_major_version`, `servers[]` (hostname + pg_port + ha_port), `pg_config`, `proxy_pg_port`, `proxy_http_port`, `s3_endpoints[]`.

**`convert.h`** — Checked narrowing casts (`u8`, `u4`, `u2`, `u1`, `i8`, `i4`, `i2`, `i1`, `i<U>`). All assert the value fits. Use instead of `static_cast` whenever narrowing.

**`base.h`** constants/helpers: `kWalBlockSize` (8192), `kWalSegmentSize` (16 MiB), `epoch_msec()` (wall-clock milliseconds), `pcounter()`/`gcounter()` (per-process / global monotonic counters), `tmp_dir()`/`tmp_file()`, `system_error_code(errno)`.

## Testing

`testing.h` provides `TmpDir` (RAII temp directory, auto-deleted on destruction), `test_config()` (loads `config.toml`), and `top_dir()` (repo root path). Every test file must `#define BOOST_TEST_MODULE <name>` before including this header — the header enforces this with `#error`.
