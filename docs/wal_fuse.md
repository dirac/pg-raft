# `wal_fuse` — WAL interception through FUSE

Source: `src/wal_fuse/` — `fuse.{h,cc}`, `fuse_ops.{h,cc}`, `wal_fuse_ops.{h,cc}`,
`wal_fuse_segment.{h,cc}`. Also `src/base/wal_segment.{h,cc}` (shared mmap layer).

## 1. Purpose

pg-raft replicates PostgreSQL by replicating its WAL **at the moment PostgreSQL writes it**, and
it does this without patching PostgreSQL. `wal_fuse` mounts a user-space filesystem over
`$PGDATA/pg_wal`. PostgreSQL keeps doing ordinary file I/O, and pg-raft sees each WAL `write`
and `fsync`:

- **write**: the bytes are parsed for XLog page and record boundaries, copied into the real
  segment file through mmap, and *published* to a `WalTracker`.
- **fsync**: the call blocks PostgreSQL until the `WalTracker` reports that the dirty range is
  durable. The Raft implementation of `WalTracker` reports durability only after a quorum of
  nodes has the range. That turns PostgreSQL's local `synchronous_commit` into cluster-wide
  quorum commit.

```
data/pg_wal/      ← FUSE mount point (empty; see PgInstance::create_raft_wal and the node's restore)
      │  FUSE
      ▼
pg/pg_wal/        ← backing directory holding the real segment files (in a node:
                    wdir/node<N>/pg/pg_wal, the WAL PostgreSQL itself writes)
```

The node's `WalTracker` is `NodeWalTracker` ([node.md](node.md#2-the-wal-path)): in leader mode it
turns publishes, fsyncs and timelines into raft entries; in follower mode an fsync is local; in
fenced mode (a node that lost leadership) every fsync fails.

## 2. Layering

```
Fuse                  mount lifecycle, event-loop thread
 └─ FuseOps           passthrough of every FUSE op to backing_dir; virtual op_* hooks
     └─ WalFuseOps    routes 24-hex-char WAL files to WalFuseSegment; everything else passes through
         └─ WalFuseSegment   per-segment XLog parser + publish/fsync logic, shared by its handles
             └─ WalSegment   (base) shared, ref-counted 16 MiB mmap of one segment file
                 └─ WalTracker  (interface) receiver of publish / fsync events — implemented by raft
```

### 2.1 `Fuse`

- `Fuse(mount_point, unique_ptr<FuseOps>)` takes ownership of the ops object.
- `start(timeout_ms)` calls `fuse_mount` with `-onoappledouble` on macOS (so it does not create
  `._*` files; Linux's libfuse rejects the option) and `fuse_new(…, ops_instance_)`, then starts `fuse_loop` on its own thread. It **waits
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
  syscall: `lstat`, `open`, `pread`, `pwrite`, `fsync`, `rename`, xattrs, and so on. The xattr ops
  keep macFUSE's signatures (with a `position`); on Linux the trampolines pass 0 and the ops call
  `setxattr` / `lgetxattr` / `llistxattr` / `lremovexattr`.
  `fi->fh` holds the real file descriptor. For directories it holds a `DirInfo*` that keeps the
  `DIR*` and resumes from the readdir offset.
- Each operation increments an `n_<op>` atomic counter. These counters are for tests only.

### 2.3 `WalFuseOps`: WAL routing

A path is a WAL segment if `log_seq_number(path)` parses it as 24 hex characters
`TTTTTTTTLLLLLLLLSSSSSSSS` (timeline, log, segment). Those files are handled as follows:

| Op | Behaviour |
|---|---|
| `op_open` | Takes the segment's `WalFuseSegment` from a registry (`weak_ptr`s by segment), or creates it: `WalSegment(cache_, backing_dir, lsn)`, `open()` (mmap), wrapped in a `WalFuseSegment`. The handle gets a heap-allocated `shared_ptr` to it, stored as a **tagged pointer** in `fi->fh`. Sets `direct_io=1` so every read and write reaches the handler without the kernel page cache. |
| `op_write` | `segment->write(buf, size, off)`. Always returns `size`. |
| `op_read` | `memcpy` out of the mmap. |
| `op_fsync` | `segment->fsync()`, which can block (see below). |
| `op_flush` | No-op (logged). |
| `op_release` | Drops the handle's reference; the last one frees the `WalFuseSegment` and its `WalSegment` reference. |
| `op_rename` | Passthrough. When the target is a timeline history file (`%08X.history`, installed by PostgreSQL on promotion), it reads the file and calls `WalTracker::async_timeline(timeline, content)`; raft replicates it as a `TimelineOp`. |
| `op_unlink` | **Never deletes a WAL file.** Renames `X` to `.X` in the backing directory. Followers and lagging WAL senders may still need the segment, so the HA layer owns WAL retention, not PostgreSQL. `WalSegment::open()` falls back to the `.X` name when it opens a segment. |

**Tagged `fh`**: `fh` is a single `uint64_t` that must distinguish "real fd" from
"`WalFuseSegment*`" without a lookup table. A WAL handle has bit 63 set. If the pointer itself
already has bit 63 set, the code sets bit 0 instead. That is safe because the pointer is at
least 2-byte aligned, which is asserted. A real fd never has bit 63 set.

`WalFuseOps` owns one `WalSegmentCache`, so all handles to the same segment share one mapping.

### 2.4 `WalSegment` and `WalTracker` (base layer)

See [base.md](base.md#5-wal-segments-wal_segmenth): `WalSegment` is a shared, ref-counted mmap of
a 16 MiB segment through a `WalSegmentCache` keyed by LSN, and `WalTracker` has three hooks:
`async_publish`, `async_fsync` and `async_timeline`. A raft follower writes received WAL with
`WalSegment::write`; the raft leader copies bytes out of the mapping before sending them, because
PostgreSQL may rewrite them mid-send. Because the cache is keyed by LSN, the node unmounts FUSE
before it replaces segment files (`rebuild_pg_wal`, a restore).

### 2.5 `WalFuseSegment`: parse, publish, and fsync

State for each segment, **shared by every handle open on it**. PostgreSQL backends write the WAL
through handles of their own (each flushes its own commits). With a state per handle, each
handle's walk started where that handle last wrote and published again everything the others had
written since: about 16 times the WAL with 16 clients. Every method takes the segment's mutex
(not held while `fsync` waits).

| Field | Meaning |
|---|---|
| `next_` | Parse cursor. Bytes in `[0, next_)` have been walked by the parser and published. |
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

The caller loops until `consumed == 0`. The result is that **`next_` stops where the bytes start
to look unwritten**: at a page whose `xlp_magic` is zero, or at a record whose `xl_tot_len` is zero.
PostgreSQL pre-zeroes pages ahead of time, so this is where its output ends.

This is a structural walk, not validation. The parser checks only that the magic is non-zero, and
does not check `xlp_pageaddr`, `xlp_tli`, `xl_prev` or the CRC. `XLogWrite` writes whole pages, and
a page can hold a record whose space has been reserved and whose header has been copied but whose
body is still zeros. The parser trusts that `xl_tot_len`, so `next_` (and the published range) can
end inside an incomplete record. PostgreSQL's own reader rejects such a record by its CRC when a
standby replays it.

**`write(buf, size, off)`**:

1. On the first call, `initialize()` parses the existing file contents from offset 0. This sets
   `next_`, `synced_` and `written_` to the end of the valid data already on disk. This matters
   when the segment is reopened after a restart.
2. The part of the write that lies before `next_` is a **rewrite of bytes already walked**.
   PostgreSQL legitimately rewrites the partial last page on each flush. `check_modified` compares
   the bytes. If they differ (the tail of a page was completed), it copies and re-publishes only the
   **smallest range that differs** (from the first differing byte to the last), and pulls `synced_`
   back to its start.
3. The rest of the write is **stored as written**, like any file, and `written_` (the highest
   offset written) moves up.
4. `WalState` then walks the **stored segment** from `next_` up to `written_`, and the newly walked
   range `[begin, next_)` is published through `tracker_.async_publish(map_, begin, len)`.

Walking the stored bytes rather than each write's buffer makes the write boundaries irrelevant: a
page header or an `xl_tot_len` cut by the end of one write is walked once the rest arrives
(`WalState::update` never reads past what is stored). A standby's walreceiver writes at any byte,
and FUSE may split PostgreSQL's writes. Earlier, the walk ran over each write's buffer: a cut page
header made the cursor run past the write and tripped an assertion, and the bytes past the walk
were not stored at all (`wal_segment_any_write_boundaries` covers both).

**`fsync()`**: it always calls
`tracker_.async_fsync(map_, synced_, max(next_, written_) - synced_, cb)`, even when the range is
empty: the tracker may hold writes of this segment published through a handle that has since
closed (the state was then read back from the file, as synced), and the flush is the barrier that
acknowledges them. The range is everything stored,
walked or not (on a standby the bytes past the last complete record are WAL the walreceiver
reports as flushed). It **blocks the FUSE thread on a `std::future`** until the tracker
completes. On success `synced_` moves to the end of the range captured before the wait (writes
that arrive during the wait are synced by the next fsync). On failure, it returns `-errno`,
and PostgreSQL's WAL flush fails, which PANICs the backend. That failure mode
is intended: PostgreSQL never acknowledges a commit that is not durable on a quorum.
`WalSegment::kNoConsensus = ECANCELED` is reserved for "msync succeeded locally, but there is no
quorum".

## 3. Concurrency

- libfuse runs operations on its loop thread. `fuse_loop` is single-threaded in this build. A
  `WalFuseSegment` is shared by the handles open on its segment and guarded by its mutex.
- The mapping cache is guarded by `WalSegmentCache::mtx`. Mappings are shared across handles and
  with the Raft layer (`shared_ptr<WalSegment>`).
- An `fsync` blocks the FUSE loop. While PostgreSQL waits for durability, no other WAL operation
  on that mount can proceed. That is acceptable because the WAL writer issues its fsync
  serially, but it limits group commit to whatever PostgreSQL batches by itself.

## 4. Tools and build notes

- `debug_wal_ops`: an interactive driver for WAL operations
  (`/opt/pg_raft/bin/b2 src/wal_fuse//debug_wal_ops`).
- `wal_fuse_bench`: a FUSE throughput benchmark (`/opt/pg_raft/bin/b2 src/wal_fuse//wal_fuse_bench`).
- `wal_data.cc` / `wal_data_exp.cc`: captured real WAL bytes used as test fixtures.
- `src/base/pch.h` defines `FUSE_USE_VERSION 26` and `_FILE_OFFSET_BITS 64` before it includes
  `<fuse/fuse.h>`. A file that includes `<fuse/fuse.h>` outside the PCH must define them first.

## 5. Testing

| Test | Covers |
|---|---|
| `fuse_test` | Mount/unmount and every passthrough op (rename, xattr, readdir, concurrency). Two cases run real PostgreSQL to confirm WAL lands in the backing directory. |
| `wal_fuse_ops_test` | open/write/fsync/read/release against `WalFuseOps` backed by a `TmpDir`; real WAL written in odd chunks (cutting page headers and `xl_tot_len`) is stored byte for byte and walked as far as when written at once (`wal_segment_any_write_boundaries`); the history-file rename hook. |
| `wal_state_test` | Feeds the parser captured WAL and checks the `(off, size, record_start)` sequence. |
| `wal_shadow_test` | Real PostgreSQL plus pgbench through a `ShadowWalOps` subclass. Checks that published bytes match the on-disk WAL. |

## 6. Open items

1. **`WalState` checks little.** It never reads past the stored bytes, but a corrupt
   `xl_tot_len` or `xlp_rem_len` (for example after a crash in the middle of a page) is not
   range-checked, and CRCs are not checked.
2. **Non-WAL writes are not replicated**, except timeline history files (`op_rename`). Files
   such as `archive_status/`, and the `xlogtemp.*` copy of the old segment that becomes the new
   timeline's first segment on promotion, go straight to the backing directory. `WalSender` reads
   that copied prefix from the parent timeline's segment instead.
3. `op_write` always returns `size`, which hides real I/O errors such as a failed mmap.
4. An `fsync` blocks the single FUSE loop thread until the raft commit (see §3).

The renamed `.X` segments are deleted by the node once they are before raft's first entry's
segment ([backup.md](backup.md#5-wal-retention-and-raft-compaction)).
