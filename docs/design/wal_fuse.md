# `wal_fuse` — WAL interception through FUSE

Source: `src/wal_fuse/` — `fuse.{h,cc}`, `fuse_ops.{h,cc}`, `wal_fuse_ops.{h,cc}`,
`wal_fuse_segment.{h,cc}`. Also `src/base/wal_segment.{h,cc}` (shared mmap layer).

## 1. Purpose

pg-raft replicates PostgreSQL by replicating its WAL **at the moment PostgreSQL writes it**, and
it does this without patching PostgreSQL. `wal_fuse` mounts a user-space filesystem over
`$PGDATA/pg_wal`. PostgreSQL keeps doing ordinary file I/O, and pg-raft sees each WAL `write`
and `fsync`:

- **write**: the bytes are parsed and validated as XLog, copied into the real segment file
  through mmap, and *published* to a `WalTracker`.
- **fsync**: the call blocks PostgreSQL until the `WalTracker` reports that the dirty range is
  durable. The Raft implementation of `WalTracker` reports durability only after a quorum of
  nodes has the range. That turns PostgreSQL's local `synchronous_commit` into cluster-wide
  quorum commit.

```
$PGDATA/pg_wal/        ← FUSE mount point (empty dir, see PgInstance::create_raft_wal)
      │  FUSE
      ▼
raft_dir/pg_wal/       ← backing directory holding the real segment files
```

## 2. Layering

```
Fuse                  mount lifecycle, event-loop thread
 └─ FuseOps           passthrough of every FUSE op to backing_dir; virtual op_* hooks
     └─ WalFuseOps    routes 24-hex-char WAL files to WalFuseSegment; everything else passes through
         └─ WalFuseSegment   per-open-handle XLog parser + publish/fsync logic
             └─ WalSegment   (base) shared, ref-counted 16 MiB mmap of one segment file
                 └─ WalTracker  (interface) receiver of publish / fsync events — implemented by raft
```

### 2.1 `Fuse`

- `Fuse(mount_point, unique_ptr<FuseOps>)` takes ownership of the ops object.
- `start(timeout_ms)` calls `fuse_mount` with `-onoappledouble` (so macOS does not create `._*`
  files) and `fuse_new(…, ops_instance_)`, then starts `fuse_loop` on its own thread. It **waits
  until the mount is live**, which it detects when `st_dev(mount_point) != st_dev(parent)`. This
  avoids a race where PostgreSQL starts before the mount takes effect and writes WAL to the
  underlying directory.
- `stop()` calls `fuse_unmount`, which makes `fuse_loop` return, then joins the thread and calls
  `fuse_destroy`.
- Built against the FUSE 2.6 API (`FUSE_USE_VERSION 26`) with 64-bit offsets.

### 2.2 `FuseOps`: the passthrough base

- `make_ops()` fills a `fuse_operations` table with static trampolines. Each trampoline
  recovers `FuseOps*` from `fuse_get_context()->private_data` and makes a virtual call.
  Subclasses intercept an operation by overriding it.
- Each operation maps a path to `backing_dir / path` (`real_path`) and calls the matching
  syscall: `lstat`, `open`, `pread`, `pwrite`, `fsync`, `rename`, xattrs, and so on.
  `fi->fh` holds the real file descriptor. For directories it holds a `DirInfo*` that keeps the
  `DIR*` and resumes from the readdir offset.
- Each operation increments an `n_<op>` atomic counter. These counters are for tests only.

### 2.3 `WalFuseOps`: WAL routing

A path is a WAL segment if `log_seq_number(path)` parses it as 24 hex characters
`TTTTTTTTLLLLLLLLSSSSSSSS` (timeline, log, segment). Those files are handled as follows:

| Op | Behaviour |
|---|---|
| `op_open` | Creates `WalSegment(cache_, backing_dir, lsn)` and calls `open()` (mmap). Wraps it in a heap-allocated `WalFuseSegment` and stores a **tagged pointer** in `fi->fh`. Sets `direct_io=1` so every read and write reaches the handler without the kernel page cache. |
| `op_write` | `segment->write(buf, size, off)`. Always returns `size`. |
| `op_read` | `memcpy` out of the mmap. |
| `op_fsync` | `segment->fsync()`, which can block (see below). |
| `op_flush` | No-op (logged). |
| `op_release` | Deletes the `WalFuseSegment`, which releases its `WalSegment` reference. |
| `op_unlink` | **Never deletes a WAL file.** Renames `X` to `.X` in the backing directory. Followers and lagging WAL senders may still need the segment, so the HA layer owns WAL retention, not PostgreSQL. `WalSegment::open()` falls back to the `.X` name when it opens a segment. |

**Tagged `fh`**: `fh` is a single `uint64_t` that must distinguish "real fd" from
"`WalFuseSegment*`" without a lookup table. A WAL handle has bit 63 set. If the pointer itself
already has bit 63 set, the code sets bit 0 instead. That is safe because the pointer is at
least 2-byte aligned, which is asserted. A real fd never has bit 63 set.

`WalFuseOps` owns one `WalSegmentCache`, so all handles to the same segment share one mapping.

### 2.4 `WalSegment` (base layer)

- Maps the whole 16 MiB file with `PROT_READ|PROT_WRITE, MAP_SHARED`.
- **Shared mapping cache**: `WalSegmentCache { mutex; map<LogSeqNumber, Mapping> }`. A
  `Mapping` holds `{map, fd, ref, last_epoch}`. `open()` reuses an existing mapping or creates
  one and increments `ref`. The destructor decrements `ref`.
- **GC**: runs on every open and destroy. It drops mappings with `ref==0` that have been idle for
  at least 5 s. If more than 5 mappings remain, it also drops every mapping with `ref==0`.
- `fsync(off, size)` rounds the range out to whole pages and calls `msync(MS_SYNC)`.
- `write(buf, off, size)` is a plain `memcpy`. A Raft follower uses it to apply received WAL
  bytes. `const_buffer(off, size)` gives a zero-copy view that the Raft leader uses to send WAL
  bytes.
- `WalTracker` has two virtual hooks. Their default implementations are a no-op publish and a
  plain local `msync`:
  - `async_publish(segment, off, size)`
  - `async_fsync(segment, off, size, completion)`

### 2.5 `WalFuseSegment`: parse, publish, and fsync

State for each open handle:

| Field | Meaning |
|---|---|
| `next_` | Parse cursor. Bytes in `[0, next_)` have been validated as XLog and published. |
| `synced_` | Bytes in `[0, synced_)` are known durable. |
| `state_` (`WalState`) | Incremental XLog parser state: `curr_` and the remaining bytes of the record currently being consumed (`rsize_`). |

**`WalState::update(ptr, size, off) → (new_curr, consumed, record_start)`** advances by **at
most one unit** per call. Each call does one of these:

- At an 8 KiB page boundary, it reads the page header. If `xlp_magic == 0` (a zeroed,
  never-written page), it stops. Otherwise it consumes the long header (40 bytes at offset 0) or
  the short header (24 bytes), and sets `rsize_ = MAXALIGN(xlp_rem_len)`, which is the remainder
  of a record that started on an earlier page.
- If `rsize_ == 0`, a record starts here. It sets `rsize_ = MAXALIGN(xl_tot_len)`.
- It consumes `min(available, space left on the page, rsize_)` bytes.

The caller loops until `consumed == 0`. The result is that **`next_` only ever lands on
validated record or page boundaries, and never runs past the bytes PostgreSQL has actually
produced**. PostgreSQL pre-zeroes pages ahead of time, and the zero magic marks where its output
ends.

**`write(buf, size, off)`**:

1. On the first call, `initialize()` parses the existing file contents from offset 0. This sets
   `next_` and `synced_` to the end of the valid data already on disk. This matters when the
   segment is reopened after a restart.
2. If the write lies entirely before `next_`, it is a **rewrite of bytes already processed**.
   PostgreSQL legitimately rewrites the partial last page on each flush. `check_modified` compares
   the bytes. If they differ (the tail of a page was completed), it copies them, re-publishes the
   range, and pulls `synced_` back to the changed offset.
3. If the write starts after `next_`, it is ignored. This happens with pre-zeroing or out-of-order
   writes ahead of the parse cursor. The bytes will be written again when they become valid.
4. Otherwise the overlapping prefix is checked as in step 2, and the new suffix is parsed with
   `WalState`. Only the validated prefix `[begin, next_)` is `memcpy`'d into the mmap and
   published through `tracker_.async_publish(map_, begin, len)`.

**`fsync()`**: if `next_ > synced_`, it calls
`tracker_.async_fsync(map_, synced_, next_-synced_, cb)` and **blocks the FUSE thread on a
`std::future`** until the tracker completes. On success, `synced_ = next_`. On failure, it
returns `-errno`, and PostgreSQL's WAL flush fails, which PANICs the backend. That failure mode
is intended: PostgreSQL never acknowledges a commit that is not durable on a quorum.
`WalSegment::kNoConsensus = ECANCELED` is reserved for "msync succeeded locally, but there is no
quorum".

## 3. Concurrency

- libfuse runs operations on its loop thread. `fuse_loop` is single-threaded in this build. Each
  `WalFuseSegment` belongs to one open handle, so its parse state is not shared.
- The mapping cache is guarded by `WalSegmentCache::mtx`. Mappings are shared across handles and
  with the Raft layer (`shared_ptr<WalSegment>`).
- An `fsync` blocks the FUSE loop. While PostgreSQL waits for durability, no other WAL operation
  on that mount can proceed. That is acceptable because the WAL writer issues its fsync
  serially, but it limits group commit to whatever PostgreSQL batches by itself.

## 4. Tools

- `debug_wal_ops`: an interactive driver for WAL operations.
- `wal_fuse_bench`: a FUSE throughput benchmark.
- `wal_data.cc` / `wal_data_exp.cc`: captured real WAL bytes used as test fixtures.

## 5. Testing

| Test | Covers |
|---|---|
| `fuse_test` | Mount/unmount and every passthrough op (rename, xattr, readdir, concurrency). Two cases run real PostgreSQL to confirm WAL lands in the backing directory. |
| `wal_fuse_ops_test` | open/write/fsync/read/release against `WalFuseOps` backed by a `TmpDir`. |
| `wal_state_test` | Feeds the parser captured WAL and checks the `(off, size, record_start)` sequence. |
| `wal_shadow_test` | Real PostgreSQL plus pgbench through a `ShadowWalOps` subclass. Checks that published bytes match the on-disk WAL. |

## 6. Known issues and open items

1. **`WalState` relies on assertions.** A corrupt `xl_tot_len` or `xlp_rem_len` (for example
   after a crash in the middle of a page) is not range-checked. CRC checks are not done.
2. **Non-WAL writes are not replicated.** Files such as timeline history files
   (`00000002.history`) and `archive_status/` go straight to the backing directory, even though
   `WalCommand` has a `TimelineOp` for them. Timeline history needs an intercept path.
3. **Retention.** Renamed `.X` files are never removed. The Raft layer needs a GC policy based
   on the minimum applied LSN across the cluster.
4. `op_write` always returns `size`, even when the bytes are ignored (step 3 above). That is
   fine because PostgreSQL rewrites them, but it hides real I/O errors such as a failed mmap.
5. `WalFuseSegment::fsync` advances `synced_` to the *original* `next_` captured before the
   wait. That is correct for the range it synced, but writes that arrive during the wait are
   counted only by the next fsync. This is by design, but undocumented.
6. The `WalSegment::gc` comment says "5 mins". The code uses 5 s.
