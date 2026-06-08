# wal_fuse

FUSE-based WAL interception layer. Mounts a passthrough filesystem over PostgreSQL's `pg_wal` directory so that all WAL writes are transparently captured and routed through pg-raft before reaching the backing storage.

## How it fits together

`pg_wal/` (empty, FUSE mount point)
  ↕ FUSE
`pg_raft_wal/pg_wal/` (backing directory, real WAL files)

PostgreSQL writes WAL as if to `pg_wal/` normally. Every `write` and `fsync` call is intercepted by the FUSE layer and delivered to `WalFuseOps` (or a test subclass), which parses and forwards the data before passing it through to the backing directory.

## Classes

**`Fuse`** — Owns the mount point. `start(timeout_ms)` mounts and blocks until the mount is live (detected by watching the `st_dev` of the mount point change). `stop()` unmounts and joins the FUSE event loop thread. The event loop runs in its own thread; all FUSE callbacks are dispatched on that thread.

**`FuseOps`** — Concrete passthrough base class. Constructed with a `backing_dir`; every FUSE operation is forwarded to that directory by default. Subclass and override `op_write`, `op_fsync`, etc. to intercept specific operations. `static make_ops()` returns the `struct fuse_operations` filled with static trampolines that dispatch through `fuse_get_context()->private_data`. Each operation exposes an `std::atomic<uint64_t> n_<opname>` counter for observation in tests.

**`WalFuseOps`** — Production `FuseOps` subclass. Identifies WAL segment files by their 24-hex-character name (`TLI + LOGID + LOGSEG`). Memory-maps each segment file via `mmap` and routes writes/reads through a `WalSegment` object. Non-WAL files fall through to the passthrough. Stores a raw tagged `WalSegment*` in `fuse_file_info::fh` (bit 63 set = WAL file) to avoid a map lookup on every I/O call. Requires a concrete `WalTracker` (see below) to receive WAL events.

**`WalSegment`** — Represents one memory-mapped WAL segment file. Tracks a parse cursor (`next_`) and a sync cursor (`synced_`). On `write`, advances the cursor through `WalState::update()` which parses XLog page and record headers inline, then calls `WalTracker::async_publish()`. On `fsync`, calls `WalTracker::async_fsync()` with the dirty page range and blocks on the returned `std::future` until the tracker resolves the promise.

**`WalState`** — XLog stream parser embedded in `WalSegment`. Validates page headers (magic `0xD116`, `xlp_pageaddr`) and tracks completed `XLogRecord` byte ranges. Returns `(offset, size)` tuples of validated WAL data.

**`WalTracker`** — Abstract interface implemented by the layer above (e.g. the raft module). Two virtual methods:
- `async_publish(file, offset, size)` — called as new validated WAL bytes arrive
- `async_fsync(promise, file, buf, size)` — called on fsync with a page-aligned dirty range; the implementor resolves the promise when the data is durable

## Additional tools

- `debug_wal_ops` — interactive WAL operation debugger (not in any test suite)
- `wal_fuse_bench` — microbenchmark for FUSE write throughput

Build either with `/opt/sparrow/bin/b2 src/wal_fuse//debug_wal_ops` or `//wal_fuse_bench`.

## FUSE include note

`src/base/pch.h` defines `FUSE_USE_VERSION 26` and `_FILE_OFFSET_BITS 64` before including `<fuse/fuse.h>`. Files compiled through the PCH get these automatically. Any file that includes `<fuse/fuse.h>` outside the PCH must define them first.
