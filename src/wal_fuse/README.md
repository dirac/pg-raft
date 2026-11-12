# wal_fuse

FUSE-based WAL interception. A passthrough filesystem is mounted over PostgreSQL's `pg_wal`, so
that every WAL write and fsync goes through pg-raft before it reaches the backing storage.

## How it fits together

```
data_dir/pg_wal/      ← FUSE mount point (left empty by PgInstance::create_raft_wal)
      ↕ FUSE
raft_dir/pg_wal/      ← backing directory with the real segment files
```

PostgreSQL writes WAL to `pg_wal/` as usual. Each write is parsed for record boundaries and
published to a `WalTracker`; each fsync blocks until the `WalTracker` reports the range durable.
The raft node's `WalTracker` reports that only once the range is committed on a quorum.

## Classes

**`Fuse`** — owns the mount: `Fuse(mount_point, unique_ptr<FuseOps>)`. `start(timeout_ms)` mounts
with `-onoappledouble`, runs `fuse_loop` on its own thread, and blocks until the mount is live (the
mount point's `st_dev` differs from its parent's). `stop()` unmounts and joins the thread.

**`FuseOps`** — the passthrough base class. Each operation is forwarded to `backing_dir`.
Subclasses override `op_*` methods to intercept operations. `static make_ops()` returns the
`fuse_operations` table of static trampolines, which dispatch through
`fuse_get_context()->private_data`. Each operation has an `n_<op>` atomic counter for the tests.

**`WalFuseOps`** — `WalFuseOps(WalTracker&, backing_dir)`. It recognizes WAL segments by their
24-hex-character name and owns one `WalSegmentCache`, so every handle to a segment shares one
mapping. For WAL files:
- `op_open` maps the segment (`WalSegment`, see `base`), wraps it in a `WalFuseSegment`, and
  stores a tagged pointer in `fi->fh` (bit 63, or bit 0 if the pointer already has bit 63 set).
  It sets `direct_io`.
- `op_write` / `op_read` / `op_fsync` go to the `WalFuseSegment`; `op_flush` does nothing.
- `op_release` deletes the `WalFuseSegment`.
- `op_unlink` renames `X` to `.X` instead of deleting it, because the HA layer owns WAL retention.

Other files pass through to `FuseOps`.

**`WalFuseSegment`** — the per-handle parse and publish logic:
- The first write parses the existing file to find where its data ends (`next_`, `synced_`).
- `write()` advances the parse cursor with `WalState::update()`. That walks page headers
  (`xlp_rem_len`, the remaining length of a record continued from the previous page) and record
  lengths (`xl_tot_len`), and stops at a page whose `xlp_magic` is zero or a record whose length is
  zero. It does not validate the WAL: no CRC, `xl_prev`, `xlp_pageaddr` or exact magic check.
  Only the bytes it walked over are copied into the mapping and published with
  `WalTracker::async_publish()`. A rewrite of bytes already parsed is compared, and
  re-published if it differs. A write beyond `next_` is ignored, because PostgreSQL writes it again
  later.
- `fsync()` calls `WalTracker::async_fsync(segment, synced_, next_ - synced_, cb)` and blocks the
  FUSE thread on a future. On success `synced_` moves to `next_`; on failure it returns `-errno`.

**`WalSegment`, `WalSegmentCache`, `WalTracker`** live in `src/base/wal_segment.h` (see the `base`
README).

## Additional tools

- `debug_wal_ops` — an interactive WAL operation debugger.
- `wal_fuse_bench` — a FUSE write throughput benchmark.

Build either with `/opt/pg_raft/bin/b2 src/wal_fuse//debug_wal_ops` or `//wal_fuse_bench`.

## Tests

`fuse_test` (two cases need PostgreSQL), `wal_fuse_ops_test`, `wal_state_test` (captured WAL in
`wal_data.cc`), `wal_shadow_test` (PostgreSQL plus pgbench).

## FUSE include note

`src/base/pch.h` defines `FUSE_USE_VERSION 26` and `_FILE_OFFSET_BITS 64` before it includes
`<fuse/fuse.h>`. A file that includes `<fuse/fuse.h>` outside the PCH must define them first.
