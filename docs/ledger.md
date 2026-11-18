# `ledger` — the raft log and the WAL in one file

Status: steps 1–4 of §15 are done, and the part of step 5 that step 4 needs. `Ledger`
(`src/raft/ledger.{h,cc}`, `ledger_test`) is `RaftServer`'s log; LMDB and `RaftLog` are gone;
AppendEntries and InstallSnapshot carry ledger records and files; `raft/wal` is written when an
entry is applied, and checkpoints sync it; `exec()` takes the WAL bytes, which `NodeWalTracker`
passes from PostgreSQL's `pg_wal`, which is never synced; `leader_sync` and `follower_sync` sync
the ledger. Still to come: measuring (step 6).

## 1. Why

With the commit-path syncs on, pg-raft loses 16–24% of its throughput; streaming replication
loses 1–9% at most points ([bench/README.md](../bench/README.md), "Where the sync time goes").
The cause is not the disk.

- **Too many syncs.** Each raft round makes three syncs on the leader, one after another and with
  the raft lock held: an msync of PostgreSQL's `pg_wal`, an msync of raft's copy, and LMDB's
  `fdatasync`. Each follower makes two: an msync of the WAL it received and LMDB's `fdatasync`.
- **Syncing writes made through an mmap is expensive.** It costs 60–70 µs, against 17–47 µs for a
  `pwrite` and `fdatasync` of the same bytes. The msync write-protects the pages and flushes the
  TLB, and the next write to those pages faults.
- **LMDB costs CPU even without syncs.** A write transaction for a ~100-byte entry was 12% of the
  leader's CPU and 20% of a follower's.

The **ledger** is one append-only sequence of records. Each record holds a raft log entry
followed by that entry's WAL bytes. It is written with `pwrite` and synced with one `fdatasync`.
Nothing on the commit path goes through an mmap, and there is one sync per round on each node.

## 2. Layout

```
raft/
  ledger/
    0000000000000001.ledger    file header, records 1 … 81,920 (ledger_file_size, 1 GiB)
    0000000000014001.ledger    file header, records 81,921 … (the last file: appended to)
    .spare.ledger              the next file, already zero-filled
    .install.ledger            an InstallSnapshot being received (§6.6)
  term                         {current term, voted for}
  meta                         {saved state, installing}
  wal/                         the materialized WAL: segments and history files (§9)
```

LMDB is dropped: `term.lmdb` and `logs.lmdb` are gone, along with the dependency (`Jamroot.jam`,
`pch.h`, `external/`). An existing `raft/` directory is not read; clusters are created fresh.

## 3. The record

All integers are little-endian. The header is a fixed 36 bytes:

| Offset | Field | |
|---|---|---|
| 0 | `u32 magic` | `0x5247444C`, the bytes `LDGR` (distinct from `net`'s `0xDEADBEEF`). 0: no record here (the end) |
| 4 | `u16 version` | 1 |
| 6 | `u16 flags` | 0 |
| 8 | `u32 meta_len` | size of the serialized metadata (at most 1 MiB) |
| 12 | `u64 idx` | raft index |
| 20 | `u64 term` | raft term |
| 28 | `u32 prev_crc` | `crc` of the record at `idx - 1` (the chain, below) |
| 32 | `u32 crc` | CRC-32C of bytes `[0, 32)`, the metadata and the WAL bytes |

The header is followed by `meta_len` bytes of metadata, `RawOArchive` of `{time, WalCommand}`, and
then the WAL bytes.

- **The WAL's length is in the serialized `WalCommand`**: `wal_bytes(cmd)` (a `WriteOp`'s or a
  `FlushOp`'s size; none for a tick, `OpenOp`, `CloseOp` or `TimelineOp`). The header has no WAL
  length of its own, so a reader decodes the metadata to find where a record ends. A record ends
  inside its file, or it is rejected before its crc is computed.
- **`idx` and `term` are in the fixed header**, so that the chain checks and the truncation compare
  them without deserializing anything.
- **The checksum is CRC-32C** (`crc32c.h`, using the CPU's instructions). It covers the whole
  record.
- **The chain (`prev_crc`).** A record is created once, by the leader that appends it, and from
  then on it is only copied byte for byte (§10). So for any given `(idx, term)` the record bytes
  are the same on every node, and `prev_crc` links one global chain, much like a hash chain.
  The chain is what lets the readers reject a stale record left on disk after a truncation or a
  torn write (§7): such a record does not chain to the record that now precedes it. A file's
  first record chains to its file header's `prev_crc`.
- There is no padding between records, and no sentinel record: the empty log is index 0 with
  term 0.

### 3.1 The file header

Each file starts with a header, followed by its records:

| Bytes | |
|---|---|
| `u32 magic` | `0x4647444C` (`LDGF`) |
| `u32 size` | of the payload |
| `u32 crc` | CRC-32C of the magic, the size and the payload |
| payload | `RawOArchive` of `FileHeader{first, prev_term, prev_crc, state}` |

`first` is the file's first index, `prev_term` and `prev_crc` are the term and crc of the record
before it (0 for the first file of a fresh ledger), and `state` is the `RaftState` at that record,
of the records only (`entry_state()`: `log_idx`, `timestamp`, `flushed_lsn`, `wal_end`; the node's
own `needs_backup`, `first_idx` and `first_segment` cleared). The ledger keeps the state at its
last record as it appends (`RaftState::apply`), and recomputes it from the file's header after a
truncation; a rotation writes it into the new file's header.

So a file is self-contained: its records, where they continue from, and the state they continue
from. That is the boundary state compaction keeps (§6.5) and what an InstallSnapshot carries
(§6.6): nothing else is needed.

## 4. Ledger files

- **Name.** Each file is named after the index of its first record, as 16 hex digits plus
  `.ledger`, so the names sort numerically. Finding the file that holds `idx` is a lookup in a
  `std::map` from first index to file.
- **Size.** Every file is exactly `ledger_file_size` bytes (a `Config` setting, default 1 GiB: a
  power of 2, larger than 32 MiB, at most 2 GiB; tests use smaller files through raft's `Options`),
  and a record never crosses its end.
  A record that doesn't fit in what is left of the file goes to the next file, and the current one
  gets an **end marker** if at least 8 bytes are left: `u32 0x444E454C` (`LEND`), then the `crc` of
  the file's last record (so a stale marker left by a truncation doesn't count). With fewer than
  8 bytes left, the file is full without a marker. A reader that finds the marker, or fewer than
  8 bytes left, goes on to the next file; one that finds a zero magic has reached the end of the
  log. A batch written with one `pwrite` is split at that boundary. A record larger than a file is
  refused (`EFBIG`); with files over 32 MiB that never happens (a record carries at most a
  segment).
- **Zero-filled ahead of time.** A background thread prepares `.spare.ledger` by writing
  `file_size` bytes of zeros and `fdatasync`ing it. Neither `fallocate` nor `ftruncate` would do:
  on xfs both leave extents that the first write must convert, and then every `fdatasync` also
  commits a metadata journal transaction. With the zeros already written, an append's `fdatasync`
  flushes data only. The zeros also mark the end of the log: a header with magic 0.
- **Rotation.** The old file gets its marker and is `fdatasync`ed (always, even with the syncs
  off), the spare is renamed to `<idx>.ledger`, its header is written, and the directory is
  `fsync`ed. Then the thread prepares the next spare. If the spare is not ready yet, the new file is created with `ftruncate`
  (a warning is logged when spares are on).
- Every file except the last is immutable. The last one is only appended to, except when it is
  truncated (§6.4).

## 5. In memory

A `std::map<tuple<idx, term>, tuple<WalCommand, tuple<offset, size>>>` would cost roughly
100–150 bytes per entry. At ~2,000 entries a second that is about 1 GB an hour. Compaction only
follows the backups (once a day in the bench config), so that would grow to tens of GB. The
index is therefore sparse, and it reads the files for anything old:

| Structure | Contents | Size |
|---|---|---|
| `files_` | first index → `File{fd, header, start, end, written, last, last_crc, full, sparse}` | one per file |
| `File::sparse` | `(idx, offset)` of the first record starting at least 64 KiB after the previous point | ~16K points (256 KB) per GiB |
| `terms_` | `(first_idx, term)` at each term change | one per term |
| base | the index, term, crc and state before the first entry (the first file's header) | |
| `tail_state_` | the state at the last record | |
| `window_` | the entries after the last applied one: `{idx, Entry, crc}` and their WAL bytes (shared) | the unapplied entries |

How each read is answered:

- `term(idx)`: `terms_` (the base term for the entry before the first).
- `last_entry()` / `first_idx()`: the last file / the base.
- `entries(idx, …)`: `window_` when it holds `idx` (apply); otherwise `records()`, decoded.
- `records(idx, …)`: the sparse point at or before `idx`, a scan forward reading headers and
  metadata, then one `pread` per file (at most two files per call).
- Compaction's "first entry whose segment is at or after `keep_from`": a scan of the entries.

## 6. Operations

### 6.1 Leader: `exec(cmd, handler)`

Under the server lock:

1. Assign `idx = last + 1`.
2. Serialize the header, the metadata and the WAL bytes into one buffer, computing the CRC
   (`encode_record`).
3. `pwrite` the buffer at the tail, and add the entry to `window_`.

`exec(cmd, wal, handler)` takes the WAL bytes the command names: `NodeWalTracker` passes them from
PostgreSQL's segment in `pg_wal` (the FUSE layer's mapping). The record gets its own copy (its crc
is of that copy), so PostgreSQL writing on meanwhile doesn't matter. Nothing goes into `raft/wal`
until the entry is applied (§9.1); peers send the records from the ledger files (§10.1).

### 6.2 Syncs

The commit-path syncs are `leader_sync` and `follower_sync` (§11), separate flags.

- **`leader_sync = true`.** A sync thread loops: take the append offset, `fdatasync`, set `durable_idx` to the
    last record below that offset, and wake the commit. The leader counts itself in the quorum at
    `durable_idx` (§10.3), so its sync runs in parallel with the network round instead of before
    it. Rounds that arrive while a sync is in flight are covered by the next one (group commit).
- **`follower_sync = true`.** A follower writes the records with `pwritev2(RWF_DSYNC)` (one system
  call: they are on disk when it returns; a file's first records are followed by an `fdatasync`,
  for its header) and then responds. `synced_idx` is the highest index a sync covered; when it
  doesn't reach the answer's index (records the follower already had, e.g. from its time as the
  leader, skipped and never synced) the follower `fdatasync`s first. Measured: no gain over `pwrite`
  + `fdatasync` (the ~30 µs is the disk write), and heartbeats no longer sync. A follower never
  has more than about one AppendEntries waiting (PostgreSQL's WAL arrives one flush at a time), so
  grouping their syncs wouldn't help.
- **Off.** `durable_idx` is the appended index. A background thread `fdatasync`s the
  last file once a second. A machine crash can lose the last
  second of a node's acknowledged entries; commits survive through replication, as they do today.
- `no_sync` (tests only): nothing is synced.
- A failed `pwrite` or `fdatasync` makes the ledger refuse further writes, and the server exits
  (`system_error`).

### 6.3 Follower: appending a request's records

The request's attached bytes are whole records (§10.1). For each record, the follower checks:

- the magic, the version, the lengths and the CRC;
- `idx` consecutive from `prev_log_idx + 1`;
- `term` not decreasing;
- `prev_crc` equal to the CRC of the record before it. For the first record, that is the
  follower's record at `prev_log_idx` (from memory when the follower is in sync, otherwise read
  from its header). An invalid record fails the request.

A record whose index the follower already holds with the same CRC is skipped. At the first one
whose CRC differs, the follower truncates (§6.4) and appends the rest with one `pwrite` per file,
then answers (written with `RWF_DSYNC` with `follower_sync`, §6.2). The WAL bytes go into its `raft/wal` when the
entries are applied (§9.1).

### 6.4 Truncation

Raft truncates only uncommitted entries: the follower accepts a request only when
`prev_log_idx >= commit_idx`. The ledger:

1. Lowers the file's written offset (§12), then zeros `[offset of the first dropped record, end)`.
2. Deletes every later file.
3. `fdatasync`s the file and `fsync`s the directory.
4. Drops the entries from `window_`, `terms_` and the sparse points, and recomputes the state at
   the new last record from the file's header.

The span to zero is bounded by the uncommitted entries. Strictly, the chain already rejects stale
records, but zeroing them keeps the files easy to read.

### 6.5 Compaction

Every node compacts its own ledger, the leader included (stepping down to compact would be a
PostgreSQL failover), and none takes a backup to do it: the standbys take the backups
([backup.md](backup.md)), and each node's backup thread passes the latest one's segment to
`set_wal_keep_from`. The target first index is the first entry whose segment is at or after it
(`keep_old_logs` entries without one, in the raft tests), never past the last applied entry, then
rounded **down** to the first index of the file that holds it: compaction deletes whole files.
Under the server lock:

1. Write `meta` with the new `first_idx` and `first_segment` (the segment of the first entry with
   one, from the new first file on).
2. Delete the files before it (`Ledger::drop_before`).

The boundary state, the state at the entry before the first one, is the new first file's header:
nothing is computed. A crash between steps 1 and 2 leaves files before `first_idx`; startup
deletes them.

### 6.6 InstallSnapshot

A follower whose next entry the leader has compacted away gets the leader's **first ledger
file**, bytes `[0, written offset)`, in chunks of at most `max_request_bytes`, one request at a
time (`InstallSnapshot{first, offset, size}`, the chunk attached). Nothing else is sent: the file
header holds where the records continue from and the state there, and the records the WAL.

On the follower:

1. Each chunk is written into `.install.ledger` (`Ledger::install_chunk`; offset 0 starts a new
   one, any other offset must follow the previous chunk).
2. After the last one, the file is checked whole (`Ledger::prepare_install`: the header, every
   record's crc, the chain, the bytes ending exactly at the end of a record) and `fdatasync`ed.
3. `meta` is written: the header's state with `needs_backup`, `first_idx` (the file's first
   index) and `first_segment`, and `installing = first`.
4. Every `*.ledger` is deleted, `.install.ledger` is renamed to `<first>.ledger`, the directory is
   `fsync`ed, and the ledger rebuilds its index by scanning it, as at startup
   (`Ledger::finish_install`). `meta` is rewritten without `installing`.
5. The follower answers with its last index; the leader continues with AppendEntries after it.
   The installed records' WAL goes into `raft/wal` as they are applied (§9.1), from the header's
   state on; the checkpoint state is the header's too.

The WAL before the file's records, and the history files of the timelines before them, come from
the latest backup, which the node then restores (`needs_backup`; the restore copies the backup's
segments and history files into `raft/wal` where missing). Compaction keeps the ledger from the
file that holds the latest backup's segment, so the backup and the file meet.

Startup finishes step 4 when `meta` names an install and `.install.ledger` exists. It deletes
`.install.ledger` when `meta` names none. A crash during an install no longer leaves a log that
doesn't match its state (what `raft.md`'s open item 3 was).

The leader reads the file without the ledger's lock, below its written offset. If a compaction
deletes it meanwhile, the next chunk finds it gone and the InstallSnapshot starts over with the new
first file. A failed chunk ends the InstallSnapshot; the next one starts from offset 0.

## 7. Startup

1. Read `term` and `meta`. Finish or discard an install (§6.6). Delete the files before
   `first_idx`'s, and a half-written spare (`.spare.tmp`); a complete `.spare.ledger` is kept.
2. Each file's header is read: the first file's gives the base (the index, term, crc and state
   before the first entry). A later file's must continue the one before it (the index, the crc,
   the term, and the state its records lead to). A last file without a valid header (a rotation
   cut short by a crash) is deleted.
3. **Every file but the last** was `fdatasync`ed at rotation, so only its record headers and
   metadata are read, to rebuild `files_`, the sparse points, `terms_` and the state.
4. **The last file** is read in full, checking every CRC and the chain. The log ends at the first
   record that fails:
   - magic 0;
   - a bad CRC;
   - an index out of sequence;
   - a decreasing term;
   - a broken chain.

   Anything after that record is a torn write or a stale record. It is never read, because a
   later append overwrites it from that point and the chain rejects what is left beyond. The
   end is logged when it is not a zero header. `verify()` reads each file only below its written
   offset, so it never sees them.
5. `verify()` (on demand, and in the tests) checks every header, CRC and the chain across all
   files.

Reading a 1 GiB file takes about half a second.

## 8. `term` and `meta`

Two small files, each written as a whole: write `<name>.tmp`, `fdatasync` it, rename it over
`<name>`, and `fsync` the directory. Each holds a version and a CRC-32C of its contents.

- `term`: `{current_term, voted_for}`, written on every term change and vote. It is always synced,
  whatever the syncs say (except under `no_sync`).
- `meta`: `{RaftState state, optional<RaftIndex> installing}`, written at compaction, on
  `needs_backup` changes and by an install (with step 4, by checkpoints too, §9.2). The boundary
  state is not in it: it is the first ledger file's header.

## 9. The materialized WAL: `raft/wal`

`raft/wal` keeps its form: 16 MiB segment files plus history files. `WalSender`, `rebuild_pg_wal`,
the node's restore, `wal_gc` and the tests' `check_consistency` read it as before. What changed is
who writes it and when: only `apply_entries()`, on every node, the leader included.

### 9.1 Written on apply

`apply_entries()` writes each entry's WAL bytes (from `window_`, or read from the ledger after a
restart) into its segment with `pwrite`, before it applies the state and calls the observer (which
advances the WAL sender over them). Raft keeps the last segments' file descriptors open (and those
written since the last checkpoint), and a missing segment is created at 16 MiB. The writes are
never synced on the commit path: the ledger is the durable copy, and `raft/wal` is derived from
it.

Since only **committed** entries are materialized, `raft/wal` no longer holds uncommitted bytes
(a follower used to write them on receipt). A later record over the same range wins, as
PostgreSQL's rewrites of its partial last page require, because the entries are applied in
order.

### 9.2 Checkpoints

A checkpoint makes `raft/wal` durable up to an applied index and records that index:

1. Under the server lock, take the applied state `S` and the segments written since the last
   checkpoint.
2. Without it, `fdatasync` them (applying goes on, into the next checkpoint).
3. Under the lock again, `S` becomes the checkpoint state and `meta` is written.

A thread runs it every `checkpoint_secs` (10 s), and `close()` once more. Every other `meta` write
(compaction, `set_needs_backup`, `restored_backup`, an install) writes the checkpoint state's entry
part (`log_idx`, `timestamp`, `flushed_lsn`, `wal_end`) with the node's own fields as they are, so
`meta` never runs ahead of `raft/wal`. An install makes the file header's state the checkpoint
state and voids a checkpoint taken before it.

On startup, raft takes `meta`'s state as today: `applied_idx = commit_idx = S.log_idx`. As
entries commit, it applies `(S.log_idx, …]` again. That rewrites their WAL bytes into
`raft/wal` with the same contents, so a crash that lost unsynced segment writes is repaired. The
observer sees those entries again, as it already does after a restart.

**Rule:** compaction never goes past the last checkpoint (its target is at most the checkpoint's
index + 1). Every entry that might need re-applying stays in the ledger. A checkpoint compacts again
when a `keep_from` is set, since it may go further now.

### 9.3 Later: a view instead of a copy

Each WAL byte is written twice: to the ledger, and (off the commit path, to the page cache) to
`raft/wal`. A later step can replace `raft/wal`'s segments with an interval map per segment,
from an LSN range to the `(file, offset)` of the latest applied record covering it, and give
`WalSender` a reader for it. That would also require changes to `rebuild_pg_wal` and the restore.

## 10. Protocol

### 10.1 AppendEntries carries records

`AppendEntries{prev_log_term, prev_log_idx, commit_idx, n}`: the attached bytes are `n` whole
records, copied byte for byte from the leader's ledger with one `pread` per file (`Ledger::records`).
A request spans at most 2 files, which matches `net`'s two attached buffers. The batch is limited
by `max_entries` and by a byte limit (`max_request_bytes`, 8 MiB). `RaftRequest::data`
(`MemBlock`) and the "at most 2 segments" trim are gone. The term of the entry before the first
one is known (the first file's header), so AppendEntries can continue right after it.

The records' CRCs give the WAL bytes an end-to-end check, from the leader's buffer to the
follower's file. The frame checksums stay off.

### 10.2 InstallSnapshot carries the first file

`InstallSnapshot{first, offset, size}` with a chunk of the leader's first ledger file attached
(§6.6); `InstallSnapshotResponse{idx, success, done}`. The peer connection sends the chunks one
at a time, and no AppendEntries while one is in flight. The segments, the boundary state and the
history file it carried before are gone.

### 10.3 The leader in the quorum

`MatchIndexes` counted the peers only, with quorum `n/2`, because the leader's copy was
implicit. The leader's copy now becomes durable asynchronously, so the leader is counted as a
member at its ledger's `durable_idx` (quorum `n/2 + 1` of all `n` nodes), updated with every
response and by the ledger's sync thread (`on_durable`). With `leader_sync` off, `durable_idx` is the
last appended index, and the result is the same as before.

## 11. Node and configuration

- **`NodeWalTracker`.** `async_publish` only extends the segment's pending range; `exec` gets the
  pending range's bytes from the `pg_wal` segment's mapping; nothing is copied into `raft/wal`
  (raft writes it on apply). `async_fsync` never msyncs `pg_wal`: in leader mode it appends the
  `FlushOp` and completes on commit, in follower mode it completes at once.
- **`pg_wal` is a cache of the WAL.** Raft keeps the committed WAL durably: the ledger, and
  `raft/wal` derived from it. A standby whose `pg_wal` lost pages in a machine crash finds an
  invalid record there and streams the rest from the WAL sender; a deposed primary's `pg_wal` is
  rebuilt from `raft/wal` (`rebuild_pg_wal`) before it restarts as a standby, and so is a node's
  after an unclean end of its previous run ([node.md §3.4](node.md#34-startup-and-postgresql-exits)).
- **Config.** `leader_sync` and `follower_sync` are the ledger's syncs now (§6.2): they used to
  msync `pg_wal` and raft's copy, and `log_sync` synced LMDB (gone). `RAFT_SYNC=1` in
  `bench/multi` sets both.
  `ledger_file_size` sets the files' size (§4).
- Raft's `Options`: `ledger_file_size`, `max_request_bytes` (8 MiB), `checkpoint_secs` (10),
  `leader_sync`, `follower_sync`.
- `rebuild_pg_wal`: same algorithm; `raft/wal` holds committed WAL only now.

## 12. Concurrency

- **One writer.** Append, truncation, rotation, compaction and install run under the server lock
  and the ledger's exclusive lock.
- **Readers don't hold the lock while they read.** Each file has a **written offset**
  (`std::atomic<unsigned>`), which the writer publishes (release) after writing the bytes before
  it. A reader takes the ledger's shared lock only to find where to read (a file, a sparse point,
  the file's last index and written offset), then reads without it, never past that written
  offset (a read that reaches it comes back short, which a scan takes for the end of the log). A truncation lowers the written offset before zeroing,
  and it only drops uncommitted records, which no reader outside the writer reads (apply reads
  committed ones, from `window_`; only a leader sends, and it never truncates).
- **File lifetime.** A `File` is held by `shared_ptr`, so deleting a file while a send reads it is
  safe: the descriptor stays valid until the last reader drops it.
- **Threads.** The sync thread (§6.2), the spare thread (§4) and the server's checkpoint thread
  (§9.2) each have their own `std::thread`. They are woken through a condition variable and
  joined on close. The sync thread's callback (`RaftServer::on_durable`) takes the server's usage
  lock with `try_to_lock`, since `close()` holds it exclusively while it stops the thread.

## 13. Expected effect

With `leader_sync` and `follower_sync` on, per round:

| | Today | Ledger |
|---|---|---|
| Leader | 2 msyncs (~125 µs) + LMDB `fdatasync`, before the round | 1 `fdatasync` (17–47 µs), during the round |
| Follower | msync (~70 µs) + LMDB `fdatasync` (~30 µs) | 1 `fdatasync` (17–47 µs) |
| Added to a commit | ~0.24 ms | ~0.05 ms (the follower's sync) |

With the syncs off, the LMDB write transaction (12% of the leader's CPU, 20% of a follower's) and
the follower's mmap write and faults go away. The apply-time `pwrite` into `raft/wal` replaces
them.

These are estimates from the raw costs in the bench README. They will be measured with
`bench/multi`, with the syncs on and off.

## 14. Testing

- **`ledger_test`** (replaced `raft_log_test`):
  - round trips of records, including ticks and `TimelineOp`s (every byte change rejected);
  - appends across files, with and without the end marker, with and without a spare;
  - `entries` and `term` across files and sparse points;
  - a follower's appends of the leader's records (skips, conflicts truncate);
  - truncation in the middle of a file and across files;
  - every file header's state against the records before it, through a truncation;
  - compaction by whole files, and the base it leaves;
  - the chain; startup after a torn tail, after garbage past the end, and with stale records left
    after a truncation;
  - InstallSnapshot in chunks, a chunk out of order, and the install's crash points;
  - the sync and spare threads; each `pwrite`, `fdatasync` and `rename` failing in turn;
  - the `term` file's rules, and the checked `meta` file.
- **The raft ensemble tests** stay as they were, with 4 MiB ledger files and 1 MiB requests (so an
  InstallSnapshot takes several chunks). `raft_ensemble_keep_from_test` expects compaction by whole
  files: to the start of the file holding the kept segment's first entry.
- **`node_wal_tracker_test`, `node_test`, `node_backup_test`**: unchanged expectations; the node
  tests use 64 MiB ledger files (`Config`'s smallest). `node_recovery_test` uses 4 MiB files
  (`NodeOptions::ledger_file_size_for_test`, which bypasses `Config`'s minimum) so that its WAL
  spans several files: every node compacts by whole files, and the wiped follower gets the
  leader's first file as its InstallSnapshot, then restores the latest backup. It allows the first
  segment kept to be the one before `keep_from`.
- **A crash test**: `kill -9` a follower and the leader mid-run (`raft_ensemble_leader_stopped_test`
  does the leader), then restart them and check that `raft/wal` is re-materialized from the last
  checkpoint.

## 15. Steps

1. Done: `src/raft/ledger.{h,cc}` (the record format, file headers, files, index, append,
   truncate, lock-free reads, compaction, install, startup), the `term` and `meta` files and
   `ledger_test`.
2. Done: `RaftServer` on the ledger; `RaftLog` and LMDB removed; the leader counted in the quorum
   at `durable_idx`.
3. Done: AppendEntries carries records, InstallSnapshot the first ledger file; `MemBlock` and
   `write_blocks` removed; `ledger_file_size` in `Config`.
4. Done: `raft/wal` written on apply, with checkpoints; `exec()` takes the WAL bytes
   (`NodeWalTracker` passes them from `pg_wal` and no longer copies them into `raft/wal`).
   `raft_ensemble_rematerialize_test`: a follower that loses its `raft/wal` and its `meta`
   applies its ledger again and has every byte back.
5. Done: `NodeWalTracker`'s msyncs removed (`pg_wal` is a cache of the WAL, §11);
   `leader_sync` / `follower_sync` sync the ledger, separately per role.
6. Benchmark with `bench/multi`, syncs on and off, 3 and 5 nodes.

## 16. Open items

1. (Done: a node rebuilds `pg_wal` from `raft/wal` on every start after an unclean end of its
   previous run, [node.md §3.4](node.md#34-startup-and-postgresql-exits).)
2. **The view instead of the copy** (§9.3).
3. **Writes.** `O_DIRECT` or io_uring for the ledger's appends and syncs, if the `fdatasync`
   remains the commit's largest cost.
4. **The sparse index's spacing** (64 KiB) and the file size (1 GiB) are guesses; tune them after
   measuring.
5. **Reads still take a shared lock briefly**, to find where to read (no I/O under it). Making the
   file list an immutable snapshot swapped by the writer would remove it.
