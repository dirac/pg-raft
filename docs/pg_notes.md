# PostgreSQL notes for pg-raft

Notes collected from claude.ai conversations held between May and November 2026 while building
pg-raft. They cover the WAL format, how PostgreSQL writes WAL, the wire and physical replication
protocols, standby behavior, promotion and timelines, plus the OS, FUSE and Asio details that came
up along the way.

**How to read this document**

- The content is organized by topic, not by conversation. §13 lists every source conversation.
- The chats contained several factual mistakes. Where a statement here differs from what a chat
  said, it is marked **⚠ Correction** and also collected in §11. Corrections come from the
  PostgreSQL 17 source layout and from this repo's code, and they name the function or header to
  check.
- **pg-raft:** callouts link a fact to code in this repo.
- The share links in `x.links` could not be matched to conversations (share IDs are not
  conversation IDs). This document was built from the account export (`conversations.json`),
  keeping only PostgreSQL- and pg-raft-related chats.

---

## Contents

1. [WAL on disk](#1-wal-on-disk)
2. [How PostgreSQL writes WAL (what the FUSE layer sees)](#2-how-postgresql-writes-wal-what-the-fuse-layer-sees)
3. [Partial writes and end-of-WAL detection](#3-partial-writes-and-end-of-wal-detection)
4. [Reading WAL: xlogreader and tools](#4-reading-wal-xlogreader-and-tools)
5. [Frontend/backend wire protocol](#5-frontendbackend-wire-protocol)
6. [Physical replication protocol](#6-physical-replication-protocol)
7. [Standby behavior](#7-standby-behavior)
8. [Promotion and timelines](#8-promotion-and-timelines)
9. [Process, FUSE and OS notes](#9-process-fuse-and-os-notes)
10. [Logical replication (for reference)](#10-logical-replication-for-reference)
11. [Corrections to the source conversations](#11-corrections-to-the-source-conversations)
12. [Action items for pg-raft](#12-action-items-for-pg-raft)
13. [Source conversations](#13-source-conversations)

---

## 1. WAL on disk

### 1.1 Segments and file names

- WAL is a single logical byte stream addressed by a 64-bit **LSN** (`XLogRecPtr`). It is stored
  as fixed-size **segment files** in `$PGDATA/pg_wal/`. The default segment size is 16 MiB; it is
  set at `initdb --wal-segsize`, must be a power of two, and ranges from 1 MiB to 1 GiB.
- Each segment is divided into **8 KiB WAL pages** (`XLOG_BLCKSZ`).
- File name: `TTTTTTTTXXXXXXXXYYYYYYYY` (24 hex characters):
  - `T`: timeline ID
  - With `segno = lsn / wal_segment_size` and `segs_per_id = 0x100000000 / wal_segment_size`
    (256 for 16 MiB): `X = segno / segs_per_id` and `Y = segno % segs_per_id`.
  - For 16 MiB segments that simplifies to `X = lsn >> 32` and `Y = (lsn >> 24) & 0xFF`.
- An LSN is displayed as `X/Y`, the high and low 32 bits in hex (e.g. `0/1A3F200`).
- Offset within a segment: `lsn % wal_segment_size`. Offset within a page: `lsn % 8192`.

> **pg-raft:** `WalSender` derives segment names from the LSN as `%08X%08X%08X`. `WalFuseOps`
> recognizes WAL files by their 24-hex-character name. Other files in `pg_wal` (`*.history`,
> `xlogtemp.*`, `*.partial`, `archive_status/`, `summaries/`) must fall through to passthrough.
> See §2.4 and §8.4.

### 1.2 Page header

Each 8 KiB page starts with `XLogPageHeaderData` (`src/include/access/xlog_internal.h`):

| Field | Type | Notes |
|---|---|---|
| `xlp_magic` | uint16 | `XLOG_PAGE_MAGIC`. **PG 17 = `0xD116`** (PG 16 = `0xD113`, PG 15 = `0xD110`). Changes with every major version that alters the WAL format. |
| `xlp_info` | uint16 | Flag bits (below) |
| `xlp_tli` | uint32 | Timeline of the first record on the page |
| `xlp_pageaddr` | uint64 | LSN of the page start |
| `xlp_rem_len` | uint32 | Bytes of a continued record that remain, counting data bytes on this and later pages but no headers |

Size: the struct is 20 bytes, and `SizeOfXLogShortPHD = MAXALIGN(20) = 24`.

The **first page of every segment** uses `XLogLongPageHeaderData`, which adds:

| Field | Type |
|---|---|
| `xlp_sysid` | uint64 (system identifier, also returned by `IDENTIFY_SYSTEM`) |
| `xlp_seg_size` | uint32 |
| `xlp_xlog_blcksz` | uint32 |

`SizeOfXLogLongPHD = 40`.

`xlp_info` flags:

| Flag | Value | Meaning |
|---|---|---|
| `XLP_FIRST_IS_CONTRECORD` | `0x0001` | Page starts with the continuation of a record from the previous page |
| `XLP_LONG_HEADER` | `0x0002` | Long header (first page of segment) |
| `XLP_BKP_REMOVABLE` | `0x0004` | Backup blocks on this page are optional (archive compression) |
| `XLP_FIRST_IS_OVERWRITE_CONTRECORD` | `0x0008` | The continuation that should be here was abandoned; an `OVERWRITE_CONTRECORD` record follows (§8.5) |

> ⚠ **Correction:** the chats gave the magic as `0xD10D` and later `0x7741`, gave the long header
> as "+8 bytes", and showed `xl_info: 0x0004` as the continuation flag. The correct values are
> `0xD116` for PG 17, +16 bytes (40 in total), and `0x0001` for `XLP_FIRST_IS_CONTRECORD`.

### 1.3 Record header

`XLogRecord` (`src/include/access/xlogrecord.h`), 24 bytes:

| Offset | Field | Type | Notes |
|---|---|---|---|
| 0 | `xl_tot_len` | uint32 | Total record length including this header |
| 4 | `xl_xid` | TransactionId | 0 if none |
| 8 | `xl_prev` | XLogRecPtr | Start LSN of the previous record (back-chain, validated by the reader) |
| 16 | `xl_info` | uint8 | **High 4 bits:** rmgr-specific record type (`XLR_RMGR_INFO_MASK = 0xF0`). **Low bits:** generic flags `XLR_SPECIAL_REL_UPDATE (0x01)` and `XLR_CHECK_CONSISTENCY (0x02)` |
| 17 | `xl_rmid` | uint8 | Resource manager |
| 18 | — | 2 bytes padding | |
| 20 | `xl_crc` | pg_crc32c | CRC-32C over the record body, then the header up to `xl_crc` |

Layout rules:

- Records start at **MAXALIGN (8-byte)** boundaries. The padding between records is zero.
- A record **can span pages and segments**. Each continuation page carries
  `XLP_FIRST_IS_CONTRECORD` and `xlp_rem_len`.
- **The 24-byte header itself can straddle a page boundary.** `xlogreader.c` handles this case:
  it reads `xl_tot_len` first, then assembles the rest of the header.
- Size limits: `xl_tot_len` is a uint32, but PG 16+ caps records at `XLogRecordMaxSize`
  (≈1020 MB; check `xlogrecord.h`). In practice, record size is bounded by the number of block
  references: `XLR_MAX_BLOCK_ID = 32`, so block IDs 0–32 (33 blocks), each with at most an 8 KiB
  image.

> ⚠ **Correction:** one chat said the *low* 4 bits of `xl_info` are rmgr-specific and mentioned
> `XLR_HAS_BACKUP_BLOCK`. That has it backwards (high 4 bits are rmgr-specific), and the flag
> predates 9.5.

### 1.4 Record body (PG 9.5+ format)

```
XLogRecord (24 B)
XLogRecordBlockHeader ×N        -- one per referenced block, in order
   [XLogRecordBlockImageHeader] -- if BKPBLOCK_HAS_IMAGE
   [XLogRecordBlockCompressHeader] -- if image has a hole AND is compressed
   [RelFileLocator (12 B)]      -- omitted if BKPBLOCK_SAME_REL
   BlockNumber (4 B)
[XLR_BLOCK_ID_ORIGIN 253 + RepOriginId]
[XLR_BLOCK_ID_TOPLEVEL_XID 252 + TransactionId]
[XLR_BLOCK_ID_DATA_SHORT 255 + uint8 len | XLR_BLOCK_ID_DATA_LONG 254 + uint32 len]
-- then the payloads, in header order:
block 0 image bytes, block 0 data, block 1 image, block 1 data, ...
main data
```

`XLogRecordBlockHeader` (4 bytes before the optional parts): `id` (uint8), `fork_flags` (uint8)
and `data_length` (uint16).

- `fork_flags & 0x0F` is the fork number: `MAIN 0`, `FSM 1`, `VISIBILITYMAP 2`, `INIT 3`.
- `fork_flags & 0xF0` holds the flags: `BKPBLOCK_HAS_IMAGE 0x10`, `BKPBLOCK_HAS_DATA 0x20`,
  `BKPBLOCK_WILL_INIT 0x40` and `BKPBLOCK_SAME_REL 0x80`.

`XLogRecordBlockImageHeader` (5 bytes): `length` (uint16), `hole_offset` (uint16) and `bimg_info`
(uint8). The `bimg_info` bits are `BKPIMAGE_HAS_HOLE 0x01`, `BKPIMAGE_APPLY 0x02`,
`BKPIMAGE_COMPRESS_PGLZ 0x04`, `BKPIMAGE_COMPRESS_LZ4 0x08` and `BKPIMAGE_COMPRESS_ZSTD 0x10`
(PG 15+). The "hole" is the free space between `pd_lower` and `pd_upper` on the data page, which is
left out of the image.

### 1.5 Resource managers

From `src/include/access/rmgrlist.h`:

| ID | Name | ID | Name |
|---|---|---|---|
| 0 | XLOG (checkpoint, switch, FPI, end-of-recovery, overwrite-contrecord) | 11 | Btree |
| 1 | Transaction (commit/abort/prepare) | 12 | Hash |
| 2 | Storage (smgr create/truncate) | 13 | Gin |
| 3 | CLOG | 14 | Gist |
| 4 | Database | 15 | Sequence |
| 5 | Tablespace | 16 | SPGist |
| 6 | MultiXact | 17 | BRIN |
| 7 | RelMap | 18 | CommitTs |
| 8 | Standby (running xacts, locks) | 19 | ReplicationOrigin |
| 9 | Heap2 (prune, vacuum, freeze, visible, multi-insert) | 20 | Generic |
| 10 | Heap (insert/update/delete/HOT/lock) | 21 | LogicalMessage |

> ⚠ **Correction:** the "WAL format" chat listed Heap = 9 and Btree = 10. The "physical
> replication" chat had the correct table (Heap2 = 9, Heap = 10, Btree = 11) but left out
> Standby = 8.

### 1.6 Full-page images (FPIs)

- The first modification of a data page after a checkpoint's redo point logs the **entire page**
  in the record (when `full_page_writes = on`). This protects against torn data pages: replay
  restores the whole image and sets `pd_lsn`.
- Later changes to the same page before the next checkpoint log only deltas.
- Images can be compressed with `wal_compression = pglz|lz4|zstd`.
- The data-page header (`PageHeaderData`: `pd_lsn`, `pd_checksum`, `pd_flags`, `pd_lower`,
  `pd_upper`, `pd_special`, `pd_pagesize_version`, `pd_prune_xid`) reaches WAL only inside an FPI.
  Ordinary records carry deltas, and the standby's page header changes as a side effect of replay.

### 1.7 Checkpoints and `pg_control`

- A checkpoint record holds the redo pointer, the timeline, the next XID/OID/multixact, the oldest
  XID, and a timestamp.
- `pg_control` (`global/pg_control`) stores the location of the latest checkpoint and the cluster
  state (`in production`, `shut down`, `shut down in recovery`, `in archive recovery`, ...).
  Recovery starts from the checkpoint's redo pointer.

> **pg-raft:** `PgInstance::verify_backup` branches on the `pg_controldata` state:
> `shut down` → `pg_checksums --check`, `in production` → `pg_verifybackup`.

---

## 2. How PostgreSQL writes WAL (what the FUSE layer sees)

### 2.1 Write path

- Backends insert records into the shared `wal_buffers` using **WAL insertion locks**
  (`NUM_XLOGINSERT_LOCKS = 8`). Several backends can copy into the buffers at the same time.
- **`XLogWrite()`** (`src/backend/access/transam/xlog.c`) moves buffer pages to the kernel with
  `write()`/`pg_pwrite()`. It runs **only while holding `WALWriteLock`**, so at most one process
  writes WAL to disk at any moment, even though many processes may call it.
- `XLogWrite` writes **whole 8 KiB pages**. The current partly filled page is written, then
  **written again at the same offset** later as more records land on it. Writes are append-mostly
  with **page-sized rewrites of the tail page**; they are not strictly append-only.
- When a segment fills up, `XLogWrite` **fsyncs that segment immediately** before moving on.
- Callers of `XLogWrite`/`XLogFlush`:
  - committing backends: `RecordTransactionCommit` → `XLogFlush`, unless
    `synchronous_commit = off`
  - the **walwriter**, every `wal_writer_delay` (200 ms default) or after
    `wal_writer_flush_after` bytes
  - the checkpointer
  - backends that need buffer space (`AdvanceXLInsertBuffer` evicting a dirty WAL buffer)
  - `XLogBackgroundFlush`

> ⚠ **Correction:** the "WAL sequential file operations" chat said FUSE would see "occasional
> concurrent writes" from multiple backends. Writes to WAL files are serialized by
> `WALWriteLock`. What the FUSE layer *does* see is several PIDs and fds over time, rewrites of
> the tail page, and fsyncs issued by different processes.

> **pg-raft:** `WalFuseSegment::write()` lowers `synced_` to the write offset
> (`synced_ = std::min(synced_, off)`, `wal_fuse_segment.cc:112`). A rewrite of the tail page is
> therefore re-synced on the next fsync. `WalState` must accept the same page bytes being rewritten
> with more records appended.

### 2.2 fsync path

`XLogFlush(lsn)` calls `XLogWrite`, then `issue_xlog_fsync()`, then advances `LogwrtResult.Flush`.
The syscall depends on `wal_sync_method`:

| `wal_sync_method` | Mechanism |
|---|---|
| `fsync` | `fsync()` after write |
| `fdatasync` | `fdatasync()` (Linux default) |
| `open_sync` | file opened with `O_SYNC`; no separate fsync call |
| `open_datasync` | file opened with `O_DSYNC` |
| `fsync_writethrough` | macOS: `fcntl(F_FULLFSYNC)`; Windows: `_commit` |

PostgreSQL opens WAL with `open()`, not stdio, so it never calls `fflush`.

> **pg-raft:** `PgInstance::write_config` forces `wal_sync_method=fsync`. Two reasons:
> - FUSE intercepts `fsync`, but not `F_FULLFSYNC`.
> - `open_*sync` would give the FUSE layer no fsync callback at all.
>
> Keep this override.

### 2.3 `synchronous_commit`

| Value | Commit waits for |
|---|---|
| `on` (default) | local flush, plus standby **flush** if `synchronous_standby_names` is set |
| `remote_apply` | local flush + standby replay |
| `remote_write` | local flush + standby write (OS cache) |
| `local` | local flush only |
| `off` | nothing; the walwriter flushes within ~3 × `wal_writer_delay` |

With `off`, a crash can lose the most recent commits but cannot corrupt data.

### 2.4 Segment creation, recycling and removal

- **Creation** (`XLogFileInit` → `XLogFileInitInternal`):
  1. Write a new file named **`xlogtemp.<pid>`** in `pg_wal`.
  2. Fill it with zeros when `wal_init_zero = on` (the default).
  3. fsync it.
  4. `durable_rename` it to the real segment name (`InstallXLogFileSegment`), then fsync the
     directory.
- **Preallocation:** the checkpointer creates segments ahead of need (`PreallocXlogFiles`).
- **Recycling** (`wal_recycle = on`, the default): old segments are **renamed** to future segment
  names instead of being deleted. A recycled file holds **stale WAL** from an old position. Its
  page headers have an old `xlp_pageaddr`, which is how readers detect the end of valid WAL.
- **Removal:** `unlink` of segments older than what checkpoints, `wal_keep_size` and replication
  slots require.
- **Expected file operations under `pg_wal`:** `open`, `create`, `write`, `fsync`, `rename`,
  `unlink`, `ftruncate`, directory fsync, `stat`, `readdir`. Also opens of `*.history`,
  `archive_status/*.ready|.done`, and `summaries/` (PG 17 WAL summarizer, if enabled).

> **pg-raft:** with `wal_recycle=off`, FUSE never sees rename-recycling, but it **still sees
> `xlogtemp.<pid>` → segment renames for every new segment**. Rename keeps the inode, fd and mmap
> valid (§9.4). Note the identity problem, though: a write to `xlogtemp.*` is not a WAL-file
> write by name, and the file becomes one only after the rename.
> `WalFuseSegment` "stops at the first stale or pre-zeroed page", which matches how PostgreSQL's
> own reader finds end of WAL (§3).

### 2.5 Shutdown

- **Primary, clean shutdown** (smart/fast): the checkpointer writes a **shutdown checkpoint
  record** (`XLOG_CHECKPOINT_SHUTDOWN`) to WAL, flushes it, and marks `pg_control` as
  `shut down`. Even an idle primary writes this one small record, so its LSN advances.
- **Immediate shutdown** (`SIGQUIT`): no checkpoint and no WAL write. Crash recovery runs at the
  next start.
- **Standby, clean shutdown:** generates **no WAL**. It does perform a **shutdown restartpoint**
  (flushes dirty buffers, updates `pg_control` to `shut down in recovery`).

> ⚠ **Correction:** the 2026-05-27 chat said a clean shutdown "doesn't really write to WAL". The
> 2026-06-09 chat is right: it writes a shutdown checkpoint record. The 2026-06-09 chat also said
> a standby "performs no checkpoint". It does: a restartpoint, which writes no WAL.

> **pg-raft:** a clean primary shutdown generates WAL that the FUSE layer and `WalSender` must
> carry. The standby must receive the shutdown checkpoint record to reach the same final LSN.

---

## 3. Partial writes and end-of-WAL detection

- **Torn data pages** are real: storage only promises sector atomicity (512 B or 4 KiB), and an
  8 KiB page spans several sectors. PostgreSQL protects data pages with FPIs (§1.6). InnoDB uses a
  doublewrite buffer for the same purpose. Copy-on-write filesystems (ZFS, Btrfs) and drives with
  power-loss protection reduce the risk but are not assumed.
- **Torn WAL** is detected, not prevented:
  - every record carries a **CRC-32C** that covers the header and the whole body
  - the reader validates each page header: `xlp_magic`, `xlp_pageaddr` equal to the expected
    address, `xlp_tli`, and `xlp_rem_len` consistency for continuations
  - it validates each record header: `xl_tot_len` ≥ 24, a known `xl_rmid`, and `xl_prev` equal to
    the previous record's start
  - a zeroed `xl_tot_len`, a stale `xlp_pageaddr` (recycled segment), or a CRC mismatch means
    **end of valid WAL**, and recovery stops there
- WAL pages have **no page-level checksum**, only per-record CRCs.
- A commit is acknowledged only after `XLogFlush` returns. A torn tail can therefore only contain
  unacknowledged transactions, which is why discarding it is safe.
- **fsyncgate (2018):** on Linux, a failed `fsync()` could drop dirty pages and a later `fsync()`
  would report success. Since then PostgreSQL **PANICs on fsync failure**
  (`data_sync_retry = off`).

> **pg-raft:** if `WalTracker::async_fsync` calls its completion with an error, the fsync fails and
> PostgreSQL will PANIC. That is the right outcome; never report success for data that is not
> durable.

**LMDB comparison** (2026-06-01 chat):

- LMDB uses a copy-on-write B+tree with two meta pages. A commit writes the new pages, syncs, then
  writes the meta page with a higher txnid. A crash before the meta write leaves the new pages
  unreachable and harmless.
- Data pages have no checksums.
- `MDB_NOSYNC` and `MDB_NOMETASYNC` give up durability, and with `MDB_NOSYNC` the meta page can
  reach disk before the data pages it points to.
- ⚠ The chat said LMDB has a **CRC on the meta page**. As far as I know, LMDB validates meta pages
  by magic, version and txnid only. Check `mdb_env_read_header` in `mdb.c` before relying on this.

---

## 4. Reading WAL: xlogreader and tools

### 4.1 Source files worth reading (in order)

1. `src/include/access/xlogrecord.h`: record structs
2. `src/include/access/xlog_internal.h`: page headers, `XLByteToSeg`, `XLogFileName`
3. `src/backend/access/transam/xlogreader.c`: the generic reader, compiled for both frontend and
   backend
4. `src/bin/pg_waldump/pg_waldump.c`: a complete example program using the reader
5. `src/backend/access/rmgrdesc/*desc.c`: per-rmgr record descriptions; `*_xlog.c` files hold the
   redo routines (e.g. `heapam_xlog.c`, `nbtxlog.c`)
6. `src/backend/replication/logical/decode.c` and `reorderbuffer.c`: logical decoding

### 4.2 `XLogDecodeNextRecord(state, nonblocking)`

This static function is the engine behind `XLogReadRecord()`/`XLogNextRecord()` (blocking) and
`XLogReadAhead()` (non-blocking, used by recovery prefetch). It returns `XLREAD_SUCCESS`,
`XLREAD_FAIL` or `XLREAD_WOULDBLOCK`.

Steps:

1. Take the target LSN from `NextRecPtr`. On the first call, after `XLogBeginRead`, this is the
   start LSN; it may also be found by `XLogFindNextRecord`.
2. `ReadPageInternal()` calls the user-supplied `page_read` callback and validates the page header
   with `XLogReaderValidatePageHeader()`.
3. Read `xl_tot_len`. If the header straddles the page, gather the rest of the header from the next
   page. Run `ValidXLogRecordHeader()`, which checks `xl_prev`, `rmid` and length.
4. Make room for the decoded record in the decode ring buffer (`XLogReadRecordAlloc`), or fall back
   to a one-off allocation for oversized records. In non-blocking mode a full ring buffer returns
   `WOULDBLOCK`.
5. If the record spans pages, copy the fragments into `readRecordBuf` (grown by
   `allocate_recordbuf`), checking that each continuation page has `XLP_FIRST_IS_CONTRECORD` and a
   matching `xlp_rem_len`.
6. `ValidXLogRecord()` checks the CRC over the whole record.
7. `DecodeXLogRecord()` fills in the `DecodedXLogRecord` (block refs, images, main data).
8. Queue the result and advance `NextRecPtr`/`EndRecPtr`.
9. A record that is cut off at the end of available WAL sets `abortedRecPtr`/`missingContrecPtr`.
   Recovery uses these to write `OVERWRITE_CONTRECORD` (§8.5).

Errors are reported through `report_invalid_record()` into `errormsg_buf`, because the file is
also compiled into frontend tools that cannot use `ereport`.

### 4.3 Tools

```bash
pg_waldump -p $PGDATA/pg_wal -s 0/1000000 -e 0/2000000   # decode a range
pg_waldump --stats=record -p $PGDATA/pg_wal -s ...        # per-rmgr stats
```

```sql
CREATE EXTENSION pg_walinspect;                -- PG 15+
SELECT * FROM pg_get_wal_records_info('0/1000000', '0/1001000');
SELECT * FROM pg_get_wal_stats('0/1000000', '0/2000000');
SELECT pg_current_wal_lsn(), pg_walfile_name(pg_current_wal_lsn());
```

> **pg-raft:** `src/wal_fuse/wal_data_exp.cc` holds `pg_waldump` output used as expected values
> for `wal_state_test`.

---

## 5. Frontend/backend wire protocol

### 5.1 Framing and startup

- Every message after startup is `Byte1 type | Int32 length (counts itself, not the type byte) |
  payload`. All integers are big-endian.
- Messages sent **before** startup have **no type byte**: `Int32 length | Int32 code | ...`.
  - `StartupMessage`: code `196608` (protocol 3.0), followed by `key\0value\0...\0`. Physical
    replication sends `replication=true` here; logical replication sends `replication=database`.
  - `SSLRequest` code `80877103`: the server answers a single byte, `S` (proceed with TLS) or `N`.
  - `GSSENCRequest` code `80877104`.
  - `CancelRequest` code `80877102`, carrying the PID and secret key, sent on a **new**
    connection.
- After startup:
  1. Authentication: `R` with a subtype: `0` Ok, `3` cleartext, `5` MD5, `10` SASL,
     `11` SASLContinue, `12` SASLFinal. SCRAM-SHA-256 follows RFC 5802.
  2. `ParameterStatus` (`S`) messages: `server_version`, `client_encoding`, `TimeZone`,
     `integer_datetimes`, ...
  3. `BackendKeyData` (`K`).
  4. `ReadyForQuery` (`Z`) with a transaction status byte: `I` idle, `T` in transaction,
     `E` failed transaction.
- Protocol 3.0 has been stable since PG 7.4. PG 18 adds 3.2 (longer cancel keys);
  `NegotiateProtocolVersion` (`v`) handles version mismatches.

> **pg-raft:** in its Startup phase, `WalSender` declines SSL (`N`), sends AuthenticationOk and
> ParameterStatus, then ReadyForQuery.

### 5.2 Message type bytes

Frontend → backend:

| Byte | Message | Byte | Message |
|---|---|---|---|
| `B` | Bind | `Q` | Query (simple) |
| `C` | Close | `S` | Sync |
| `D` | Describe | `X` | Terminate |
| `E` | Execute | `c` | CopyDone |
| `F` | FunctionCall | `d` | CopyData |
| `H` | Flush | `f` | CopyFail |
| `P` | Parse | `p` | Password / SASL response |

Backend → frontend:

| Byte | Message | Byte | Message |
|---|---|---|---|
| `1` | ParseComplete | `N` | NoticeResponse |
| `2` | BindComplete | `R` | Authentication* |
| `3` | CloseComplete | `S` | ParameterStatus |
| `A` | NotificationResponse | `T` | RowDescription |
| `C` | CommandComplete | `V` | FunctionCallResponse |
| `D` | DataRow | `W` | CopyBothResponse |
| `E` | ErrorResponse | `Z` | ReadyForQuery |
| `G` | CopyInResponse | `c` / `d` | CopyDone / CopyData |
| `H` | CopyOutResponse | `n` | NoData |
| `I` | EmptyQueryResponse | `s` | PortalSuspended |
| `K` | BackendKeyData | `t` | ParameterDescription |
| | | `v` | NegotiateProtocolVersion |

`C`, `D`, `E`, `H` and `S` mean different messages depending on direction.

### 5.3 Simple query (`Q`)

- Request: `'Q' | len | query\0`. The string may contain **several `;`-separated statements**.
- The backend handles it in `exec_simple_query()` (`postgres.c`). The whole string is parsed into a
  list of raw parse trees. Each statement is analyzed, rewritten, **planned from scratch** and run
  in an unnamed portal with no row limit.
- Replies for each statement:
  - row-returning statement: `T`, `D`×N, `C` (e.g. `SELECT 5`)
  - other statements: `C` only (e.g. `INSERT 0 1`)
  - empty string: `I`
- **Implicit transaction:** when no transaction block is open, the whole multi-statement string runs
  as one transaction. An explicit `BEGIN`/`COMMIT` inside the string takes over.
- **On error:** `E` is sent, the remaining statements are skipped, and the transaction is aborted.
- There is always exactly one `Z` at the end.
- Results are always text format. `PortalSuspended` never occurs.

> **pg-raft:** `PgClient` uses the extended protocol. `WalSender` handles simple `Q` messages in
> its Command phase (`IDENTIFY_SYSTEM`, `SHOW`, `SET`, simple `SELECT`).

### 5.4 Extended query protocol (prepared statements)

Object model:

- **Prepared statement:** a `CachedPlanSource`; parsed and analyzed, with `$n` placeholders.
- **Portal:** a statement bound to parameter values and result formats; holds executor state.
- Each has one **unnamed** slot (`""`) and any number of named ones.
  - The unnamed statement is replaced by the next unnamed Parse, or by any `Q`.
  - Named statements last for the session until Close, `DEALLOCATE` or `DISCARD ALL`. Parsing an
    existing name is an error.
  - Portals are dropped at transaction end.

Messages:

| Message | Contents | Reply |
|---|---|---|
| Parse `P` | name, query (exactly one statement), Int16 n, Int32[n] param OIDs (0 = infer) | `1` |
| Bind `B` | portal, stmt, Int16 n + Int16[] param formats (0 = all text, 1 = applies to all, N = per param), Int16 n + {Int32 len (-1 = NULL), bytes}, Int16 n + Int16[] result formats | `2` |
| Describe `D` | `'S'`/`'P'` + name | for a statement: `t` then `T` or `n` (format codes 0); for a portal: `T` or `n` |
| Execute `E` | portal, Int32 max_rows (0 = all) | `D`*, then `C` / `s` / `I`. **Never sends a RowDescription.** |
| Close `C` | `'S'`/`'P'` + name | `3` (closing a missing name is not an error) |
| Sync `S` | — | `Z` |
| Flush `H` | — | forces buffered output out |

**Sync semantics:**

1. Without `BEGIN`, everything between two Syncs is **one implicit transaction**, committed at the
   Sync.
2. **On error:** the backend sends `E`, **discards everything up to the next Sync**, then sends `Z`.
   The status byte is `I` if the transaction was implicit (rolled back) and `E` if it was explicit.
   This discard rule is what makes pipelining safe.
3. Replies may not be flushed until a Sync or Flush arrives.

**Parameter types:**

- The statement has as many parameters as the highest `$n` used or the number of OIDs sent,
  whichever is larger. OID `0` or `705` (`unknown`) means the server infers the type.
- Inference happens when an unknown Param is first coerced: `variable_coerce_param_hook` fixes its
  type. Examples:
  - `col = $1` → the type of `col`
  - `$1::date` with `$1` still untyped → `$1` **becomes** `date`; this is a declaration, not a
    run-time cast
  - top-level `SELECT $1` → `text` (PG 10+)
- `exec_parse_message()` rejects any parameter still unknown after analysis with
  `could not determine data type of parameter $n`. Examples: `$1 IS NULL`, or a `$2` gap that is
  never referenced.
- **Bind never resolves types.** It only converts values with `typinput`/`typreceive`, and
  conversion errors are reported there.
- The first coercion fixes a parameter's type. Later `::T` on the same parameter is a real cast,
  e.g. `SELECT $1::date, $1::int` → `cannot cast type date to integer`.
- SQL-level `PREPARE name(int, text) AS ...` takes **types only, no names**. Parameters not listed
  are inferred the same way.

**Planning:**

- Parse does parse, analyze and rewrite. Planning happens at **Bind**, in `GetCachedPlan()`.
- With `plan_cache_mode = auto`, the first 5 executions get custom plans. After that, a generic plan
  is used if its estimated cost is ≤ the average custom-plan cost (the average includes a planning
  overhead term).
- DDL, `ANALYZE` or a `search_path` change invalidates a cached statement. If the result type
  changes, revalidation fails with `cached plan must not change result type`.

**Drivers** (from the 2026-11-11 chat): libpq `PQexecParams` sends unnamed
Parse/Bind/Describe/Execute/Sync in one batch. pgJDBC switches to named statements at
`prepareThreshold=5`. pgx defaults to a Parse+Describe statement cache. asyncpg always uses named
statements and binary format. psycopg 3 promotes at `prepare_threshold=5`. node-postgres sends OID 0
and text values. Named statements break under transaction-mode poolers unless PgBouncer ≥ 1.21
with `max_prepared_statements` is used.

**Built-in type OIDs** are fixed (`pg_type.dat`, all < 16384): bool 16, int8 20, int4 23, text 25,
float8 701, date 1082, timestamptz 1184, numeric 1700, uuid 2950, jsonb 3802. Array types have their
own OIDs (`typarray`; `_int4` = 1007). User and extension types get OIDs per database.

### 5.5 COPY sub-protocol

```
COPY FROM STDIN:  Q → G (CopyInResponse) ; F: d* then c (or f) ; B: C, Z
COPY TO STDOUT:   Q → H (CopyOutResponse) ; B: d* c C Z
Replication:      START_REPLICATION → W (CopyBothResponse) ; d in both directions
```

- `CopyIn`/`Out`/`BothResponse` payload: `Int8 overall format (0 text / 1 binary) | Int16 ncols |
  Int16[ncols] formats`. Replication sends format 0 with 0 columns.
- `CopyData` has no size limit beyond the Int32 length. **One CopyData frame carries exactly one
  replication message.**
- Binary COPY file format:
  - signature `PGCOPY\n\377\r\n\0`, Int32 flags, Int32 header extension length
  - each tuple: Int16 field count, then for each field Int32 length (−1 = NULL) and the bytes
  - trailer: Int16 −1

---

## 6. Physical replication protocol

### 6.1 Connection and commands

The standby connects with `replication=true` in its startup message. That backend is a
**walsender**, which accepts replication commands. In physical mode it also accepts a few simple
SQL commands (`SHOW`, `SET`), and nothing from the extended query protocol.

| Command | Purpose / reply |
|---|---|
| `IDENTIFY_SYSTEM` | One row: `systemid` (text), `timeline` (int4), `xlogpos` (text `X/Y`), `dbname` (text, NULL for physical) |
| `TIMELINE_HISTORY tli` | One row: `filename` (text), `content` (bytea/text): the `%08X.history` file |
| `START_REPLICATION [SLOT s] [PHYSICAL] X/Y [TIMELINE tli]` | Enters CopyBoth streaming |
| `READ_REPLICATION_SLOT s` | (PG 15+) slot type, restart LSN, timeline |
| `CREATE_REPLICATION_SLOT s PHYSICAL [RESERVE_WAL] [TEMPORARY]` / `DROP_REPLICATION_SLOT s` | Slot management |
| `BASE_BACKUP ...` | Used by `pg_basebackup` |
| `SHOW name` | e.g. `SHOW wal_segment_size`, which `pg_basebackup`/`pg_receivewal` ask for |

A replication connection cannot run normal SQL. Monitoring queries need a separate ordinary
connection.

### 6.2 Streaming messages (inside CopyData `d`)

| Msg | Dir | Layout | Payload size |
|---|---|---|---|
| `w` XLogData | P→S | `'w'`, Int64 dataStart, Int64 walEnd, Int64 sendTime, bytes | 25 + N |
| `k` PrimaryKeepalive | P→S | `'k'`, Int64 walEnd, Int64 sendTime, Byte replyRequested | 18 |
| `r` StandbyStatusUpdate | S→P | `'r'`, Int64 write, Int64 flush, Int64 apply, Int64 sendTime, Byte replyRequested | **34** |
| `h` HotStandbyFeedback | S→P | `'h'`, Int64 sendTime, Int32 xmin, Int32 xminEpoch, Int32 catalogXmin, Int32 catalogXminEpoch | 25 |

- Timestamps are **microseconds since 2000-01-01**.
- In XLogData, `walEnd` is the **sender's current end of WAL** (`SendRqstPtr`). It is *not* the end
  of this message.
- WAL bytes in a message = `CopyData length − 4 − 25`.

> ⚠ **Correction:** one chat said StandbyStatusUpdate is 18 bytes "like keepalive". It is 34.
> `WalSender` reads `body[33]` as the reply flag, which agrees.

### 6.3 What the payload contains

- The payload is **raw WAL bytes starting at `dataStart`**, exactly as they appear in the segment
  file. That includes **WAL page headers** (short and long) wherever the range crosses a page start.
  It is a byte stream, not a record stream.
- Message boundaries are independent of record boundaries:
  - one XLogData can hold many records
  - one record can span many XLogData messages
  - a message can start mid-record, or at a page header when it starts on a page boundary
- The standby's walreceiver writes the bytes into its own segment files at the given LSN without
  parsing them. The startup process later reads complete records from those files.
- **How the walsender chooses chunk sizes** (`XLogSendPhysical`):
  - it sends from `sentPtr` up to the **flushed** end of WAL (`GetFlushRecPtr`); a physical
    walsender **never sends WAL the primary hasn't flushed**
  - each message is capped at `MAX_SEND_SIZE = XLOG_BLCKSZ × 16 = 128 KiB`
  - when the cap applies, the end is rounded down to a page boundary; otherwise the message ends
    exactly at the flush pointer, which may be mid-page

> ⚠ **Correction:** the chats said the send size is "8 KB" and that "each page becomes one
> CopyData frame". The cap is 128 KiB. A chat also answered "are page headers sent?" by talking
> about *data*-page headers. WAL page headers are always part of the physical stream.

> **pg-raft:** `WalSender` reads the segment files with `pread` and follows the same rule. Send only
> up to the FUSE-observed **fsynced** LSN (`advance(flush_lsn)`), never up to merely written
> bytes. Otherwise a standby could receive WAL that the primary later loses in a crash.

### 6.4 Leaving COPY mode

- **End of a historical timeline:** this is the main *normal* reason in physical replication. A
  standby can request `START_REPLICATION ... TIMELINE n` where `n` is not the server's latest
  timeline. The walsender then:
  1. streams up to the timeline's switch point
  2. sends **CopyDone**
  3. waits for the standby's CopyDone
  4. sends a **one-row result set `(next_tli int8, next_tli_startpos text)`**, then
     CommandComplete and ReadyForQuery

  The standby then issues `TIMELINE_HISTORY` and `START_REPLICATION` again on the next timeline.
- **The standby sends CopyDone** when its walreceiver is told to stop: promotion, shutdown,
  reaching a recovery target, or seeing the end of a timeline. The walsender answers with its own
  CopyDone.
- **Errors:**
  - the requested WAL has been removed (`requested WAL segment ... has already been removed`)
  - the slot was invalidated
  - `wal_sender_timeout` expired (no reply received)
  - `pg_terminate_backend`, server shutdown, or a network failure

  Most real disconnects are abrupt, not clean CopyDone handshakes.
- The standby **reconnects on its own**: the startup process relaunches a walreceiver after
  `wal_retrieve_retry_interval` (5 s default), forever, until it is promoted or shut down.

> **pg-raft:** `WalSender` closes the socket when it receives CopyDone or Terminate, and
> `TIMELINE_HISTORY` returns an empty result. That is enough while there is only timeline 1. Once
> promotion and timeline switches are supported (§8), it needs the end-of-timeline CopyDone and
> result-set handshake and real history-file contents.

### 6.5 Monitoring catch-up

- On the primary, `pg_stat_replication` shows `sent_lsn`, `write_lsn`, `flush_lsn`, `replay_lsn`,
  `write_lag`, `flush_lag`, `replay_lag` and `sync_state`.
  - Fully caught up: `replay_lsn = pg_current_wal_lsn()`.
- On the standby:
  - `pg_last_wal_receive_lsn()`: the walreceiver's flushed position; can be mid-record
  - `pg_last_wal_replay_lsn()`: the end of the last replayed record
  - `pg_stat_wal_receiver`: walreceiver status and timestamps
  - `pg_wal_lsn_diff(a, b)`: difference between two LSNs in bytes

> **pg-raft:** the integration tests compare the standby's replay LSN with the primary's LSN and
> compare row counts, using `PgClient`.

---

## 7. Standby behavior

- **When WAL is applied:**
  - the walreceiver writes received WAL to `pg_wal` (`XLogWalRcvWrite`), fsyncs it
    (`XLogWalRcvFlush`), and wakes the startup process
  - the startup process replays it almost immediately, unless `recovery_min_apply_delay` is set
  - status replies go out every `wal_receiver_status_interval` (10 s) or when the primary asks
- **Partially received records:**
  - the startup process **never applies part of a record**
  - it waits in `XLogReadRecord` → `WaitForWALToBecomeAvailable` until all bytes of the record
    arrive, across pages and segment files
  - meanwhile `receive_lsn` can point into the middle of a record while `replay_lsn` stays at the
    end of the last complete one
- **Standby restarts with an unreachable primary:** it replays local WAL, then retries the
  connection forever, serving read-only queries if `hot_standby = on`. It **never promotes
  itself**; that is the job of an external manager such as Patroni, repmgr or pg_auto_failover.
- **Shutting down a standby and deleting its `pg_wal`:**
  - it restarts from the checkpoint/restartpoint recorded in `pg_control` and re-fetches WAL from
    there
  - sources: streaming from the primary, `restore_command`, or a slot that kept the WAL
  - it fails with `requested WAL segment has already been removed` only if the primary no longer
    has that WAL; the fix is a fresh base backup
  - note that the restart point is the **redo pointer of the last restartpoint**, not the last
    replayed LSN, so the primary must still have WAL from that earlier point

  > **pg-raft:** this is exactly what `standby_wal_reset_test` exercises. `WalSender` must serve
  > `START_REPLICATION` from an older LSN than the one it last sent.

- **Replication slots:** a physical slot pins WAL on the primary (`restart_lsn`) while the standby
  is down. With `max_slot_wal_keep_size` the slot becomes `wal_status = lost`. Slots do **not**
  carry over to a promoted standby, except failover slots for logical replication in PG 17.

---

## 8. Promotion and timelines

### 8.1 Triggering promotion

| Method | Notes |
|---|---|
| `pg_ctl promote -D $PGDATA` | Writes **`$PGDATA/promote`**, then sends **SIGUSR1** to the postmaster |
| `SELECT pg_promote(wait => true, wait_seconds => 60)` | PG 12+. Superuser, or a role granted EXECUTE on the function. Returns false on timeout |
| `promote_trigger_file` | **Removed in PG 16** (`trigger_file` in `recovery.conf` before PG 12) |

**SIGUSR1 alone does not promote.** The postmaster's SIGUSR1 handler promotes only if
`CheckPromoteSignal()` finds the `promote` file. It then signals the startup process with SIGUSR2.
SIGUSR1 is the postmaster's general "check your signal flags" interrupt.

> ⚠ **Correction:** the 2026-05-28 chat said "`pg_ctl promote` is just a wrapper that sends
> SIGUSR1". It isn't: without the file, nothing happens.
>
> **pg-raft action:** `PgInstance::promote()` (`src/process/pg_instance.cc:161`) only sends
> `kill(pid, SIGUSR1)` and sets `role_ = kPrimary`. It needs to create `data_dir/promote` first,
> or call `pg_ctl promote` or `SELECT pg_promote()`. It should also confirm promotion, for example
> by polling `pg_is_in_recovery()` until it returns false, rather than setting the role right away.

> ⚠ **Correction:** a chat claimed that deleting `standby.signal` from a running standby causes it
> to promote. The file is read only at startup, so deleting it has no effect until the next
> restart.

### 8.2 What happens during promotion

1. The startup process sees the promote request, tells the walreceiver to stop (which sends
   CopyDone), and replays the WAL already received.
2. **The end of WAL is the end of the last valid record** (see §8.5 for partial records).
3. A new timeline is chosen as `max(existing history) + 1`, and `0000000N.history` is written to
   `pg_wal`.
4. **The first segment of the new timeline is copied from the old timeline's last segment** up to
   the switch point (`XLogInitNewTimeline`). The old timeline's last segment is renamed to
   **`<name>.partial`** (since 9.5) so it isn't archived as if complete.
5. **`standby.signal` and `recovery.signal` are removed.** This is in the end-of-recovery code:
   `xlogrecovery.c`/`xlog.c` in PG 15+, and `StartupXLOG()` in `xlog.c` before that. Search for
   `STANDBY_SIGNAL_FILE`, defined in `access/xlog.h`. Removing them is what makes promotion
   survive a restart; if they stayed, the next start would be a standby again.
6. **Fast promotion** (the default since 9.3) writes an **`XLOG_END_OF_RECOVERY`** record rather
   than waiting for a full checkpoint. The checkpoint is requested to run in the background.
7. The server accepts writes and its WAL carries the new timeline ID.

> ⚠ **Correction:** a chat said "a new checkpoint is written" before the server opens for writes.
> With fast promotion, an end-of-recovery record is written and the checkpoint follows
> asynchronously.

> **pg-raft:** on promotion, the FUSE layer will see a new `.history` file, a newly created segment
> for the new timeline (its name starts with the new TLI), a rename to `.partial`, and an
> end-of-recovery record. `WalFuseOps` needs to treat `.partial` and `.history` as non-WAL
> passthrough files, or handle them explicitly.

### 8.3 History files

- Named `%08X.history` after the **new** timeline, e.g. `00000002.history`. Timeline 1 has none.
- Each line is `parentTLI <tab> switchpoint LSN <tab> reason`. The file is cumulative:

  ```
  1	0/5000000	no recovery target specified
  2	0/8000000	no recovery target specified
  ```

  Read this as: TLI 1 ended at 0/5000000, then TLI 2 ended at 0/8000000. This file belongs to
  TLI 3.
- The reason text comes from `getRecoveryStopReason()`. Possible values: `no recovery target
  specified`, `before|after transaction N`, `before|after <timestamp>`, `at restore point "name"`,
  `before|after LSN "X/Y"`, `reached consistency` (for `recovery_target = 'immediate'`).
- Users of history files:
  - standbys with `recovery_target_timeline = 'latest'` (the default since PG 12), to find where
    to switch timelines
  - walsenders, to decide which timeline's WAL to send for a requested position
  - `pg_rewind`, to find the fork point
  - the `TIMELINE_HISTORY` replication command

> ⚠ **Correction:** the reason strings `target time reached`, `target LSN reached` and so on, from
> the 2026-05-28 chat, are not PostgreSQL's real strings.

**What creates a new timeline:** the end of archive recovery, meaning promotion or PITR reaching
its target and then promoting. **What doesn't:** a standby restart, a reconnect, crash recovery,
cascading changes, or `pg_rewind` (which rewinds a server so it can join an existing timeline).

**PITR:** restore a base backup, set `restore_command`, and set one of `recovery_target_time`,
`_lsn`, `_xid`, `_name` or `recovery_target = 'immediate'`, plus `recovery_target_inclusive`.
`recovery_target_action` is `pause` (the default), `promote` or `shutdown`.

### 8.4 Files in `pg_wal` besides segments

| File | Created by |
|---|---|
| `0000000N.history` | promotion or end of PITR |
| `<segment>.partial` | the last segment of the old timeline at promotion |
| `xlogtemp.<pid>` | segment creation (§2.4) |
| `<segment>.backup` | backup history files (archive mode) |
| `archive_status/<seg>.ready` / `.done` | archiver |
| `summaries/*.summary` | PG 17 WAL summarizer (`summarize_wal = on`) |

### 8.5 Promoting while a record is only partly received

- The standby's WAL ends inside record B. Recovery stops at the end of the last complete record A,
  and B's partial bytes are **never applied**.
- B's transaction was never acknowledged on the primary: the walsender only sends flushed WAL, but
  a commit could still be waiting in `SyncRepWaitForLSN`. So the outcome is the same as for any
  failover: work the client wasn't told was committed may be lost.
- **`OVERWRITE_CONTRECORD`** (added in PG 15 and back-patched to all supported branches in the
  2021 minor releases): when WAL ends inside a record that continues across a page, recovery
  writes an `XLOG_OVERWRITE_CONTRECORD` record at the point where the continuation should have
  been. It also sets `XLP_FIRST_IS_OVERWRITE_CONTRECORD` on that page. This prevents a later
  reader, or a standby that received more bytes, from stitching the stale tail onto a new record.
  How this interacts with a timeline switch is subtle; read the `abortedRecPtr` /
  `missingContrecPtr` handling in `xlogrecovery.c` and `xlog.c` before relying on it.
- After promotion, the old primary has diverged (TLI 1 continues past A on it). It must be fenced,
  then rewound with `pg_rewind --target-pgdata=... --source-server=...` or re-cloned.

> **pg-raft:** this is the central safety question for a Raft-backed WAL. Decide whether a new
> leader exposes WAL only up to the **Raft-committed** LSN, and whether that point always falls on
> a record boundary. The `complete` flag returned by `WalState::update()` (a full XLogRecord was
> reached) is the natural place to enforce this.

---

## 9. Process, FUSE and OS notes

### 9.1 fork/exec process manager (2025-12-30 chat)

- The design: `spawn()` (fork + exec), `isRunning()` (`waitpid(WNOHANG)`), `wait(timeout)`,
  `terminate()` (SIGTERM), `kill()` (SIGKILL), and a destructor that reaps the child so no zombies
  are left.
- The status queries it proposed (memory, CPU, child PIDs) read `/proc/<pid>/{stat,status,cmdline}`.
  **That is Linux-only.** On macOS, use `libproc` instead:
  - `proc_pidinfo(PROC_PIDTASKINFO)` for RSS and CPU time
  - `proc_listchildpids()` for child PIDs
  - `proc_pidpath()` for the executable path

> **pg-raft:** `src/process/Process` implements the start/stop/kill/wait/try_wait part. Any future
> process introspection there must use libproc on macOS.

### 9.2 macFUSE / libfuse

- macOS uses **macFUSE** (`brew install --cask macfuse`). The kernel extension must be approved
  in System Settings, and Apple Silicon needs reduced security mode.
- Headers are under `/usr/local/include/fuse`.
- Use `FUSE_USE_VERSION 26`. Code written against the libfuse 2 API builds on both macOS and Linux
  (`libfuse2`). libfuse 3 (`FUSE_USE_VERSION 31`) changes signatures, e.g. `filler` gains a flags
  argument.
- Unmount with `umount`/`diskutil unmount` on macOS and `fusermount -u` on Linux. Linux non-root
  mounts need `user_allow_other` in `/etc/fuse.conf` for `-o allow_other`.
- Useful mount options: `-f` (foreground), `-d` (debug), `-ovolname=`, `-olocal`.

> **pg-raft:** `src/base/pch.h` defines `FUSE_USE_VERSION 26` and `_FILE_OFFSET_BITS 64`. `Fuse`
> runs the loop in its own thread and detects the mount by watching `st_dev` change.

### 9.3 Which file operations FUSE sees under `pg_wal`

See §2.1 and §2.4. In short:

- one writer at a time (`WALWriteLock`), but different PIDs and fds over time
- page-granular writes with tail-page rewrites
- fsync from the walwriter, the checkpointer and backends
- a segment fsync whenever a segment fills
- `xlogtemp.<pid>` create, zero-fill, fsync, rename
- unlinks of old segments
- `.history`, `.partial`, `archive_status` and `summaries` files

**Track the write offset per file handle and log any non-sequential write that isn't a rewrite of
the tail page.**

### 9.4 fd and mmap after rename

- A rename changes only the directory entry. The inode is the same, so **open fds and mmaps stay
  valid and coherent**, including writes through `MAP_SHARED`. This holds in the same directory or
  across directories on one filesystem.
- After unlink, the inode lives on until the last fd or mapping goes away.
- A cross-filesystem `mv` is a copy plus delete: the fd and mmap keep the old inode, and the new
  path is a different file.

> **pg-raft:** `WalSegmentCache` keys shared mappings by LSN, not by path. `WalFuseOps::op_unlink`
> hides a segment as `.X` rather than deleting it, and `WalSegment::open` falls back to `.X`. So the `xlogtemp` →
> segment rename (and a rename to `.partial`) does not invalidate a live mapping. One thing to
> check: the backing-dir file for `xlogtemp.<pid>` becomes the segment file through the rename that
> FUSE passes through.

### 9.5 Boost.Asio

- **`async_connect` → "Operation canceled (89)":** 89 is `ECANCELED` on macOS (125 on Linux), i.e.
  `asio::error::operation_aborted`. The cause is almost always local cancellation, not the network:
  - a timeout timer firing `socket.cancel()` after the connect already finished (cancel the timer
    in the connect handler, and in the timer handler `if (ec) return;`)
  - the socket or its owning object being destroyed or closed (capture `shared_from_this()`)
  - `io_context::stop()` or destruction
  - The chat also blamed "a second op of the same type on kqueue cancels the first". That claim is
    unverified; don't rely on it.
- **SSL server and client** (2026-05-31 chat):
  - server: `ssl::context` with a cert chain and private key, `ssl::stream<tcp::socket>`,
    `async_handshake(server)`
  - client: `set_default_verify_paths()`, `set_verify_mode(verify_peer)`,
    `SSL_set_tlsext_host_name` for SNI, `async_shutdown` for close_notify
  - for testing: `openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt -days 365
    -nodes -subj "/CN=localhost"`, and load it on the client with `load_verify_file`

> **pg-raft:** `src/net/Connection` uses `std::variant<tcp_socket, ssl_socket>`, and
> `async_connect_test` covers connect behavior.

### 9.6 `initdb`

- Creates the data directory layout: `base/`, `global/`, `pg_wal/`, `pg_xact/`, ...
- Creates the system catalogs and **three databases**: `template1`, `template0`, `postgres`.
- Writes `postgresql.conf`, `pg_hba.conf` and `pg_ident.conf`.
- Fixes encoding and locale, and data checksums (`-k`; on by default in PG 18).
- Creates a superuser **role** named after the OS user (or `-U`). It does **not** create a database
  with that name. That's why a bare `psql` fails with "database venkat does not exist"; use
  `psql -d postgres`.

> **pg-raft:** `PgInstance::init()` runs `initdb`. `create_raft_wal()` then moves the initial
> segments into `raft_dir/pg_wal`, leaving `pg_wal` empty for the FUSE mount.

---

## 10. Logical replication (for reference)

pg-raft doesn't use logical replication. This summary is kept because the chats covered it at
length.

- Logical replication uses the same CopyBoth transport, started with
  `START_REPLICATION SLOT s LOGICAL X/Y (proto_version '4', publication_names 'p', ...)`.
- Each XLogData carries **one decoded message from the output plugin**. These do not map 1:1 to WAL
  records: a commit is reassembled in the ReorderBuffer first. Partial records are never visible to
  the consumer.
- pgoutput messages:
  - `B` begin, `C` commit, `R` relation, `Y` type, `O` origin
  - `I` / `U` / `D` / `T`: insert, update, delete, truncate
  - `M` message
  - streaming (`proto_version` 2+): `S`, `E`, `c`, `A`
  - two-phase (3+): `b`, `P`, `K`, `r`
  - `proto_version` 4 (PG 16) adds parallel apply
  - TupleData column kinds: `n` null, `u` unchanged TOAST, `t` text, `b` binary
- The slot's `confirmed_flush_lsn` advances only with the **flush** LSN in StandbyStatusUpdate. If
  the consumer reports write/apply but never flush, WAL piles up on the server.
- The chat's asyncpg example (`STANDBY_STATUS_UPDATE` as a SQL command) is **not real**. asyncpg
  has no replication-stream API, so use psycopg's replication support or raw protocol code.

---

## 11. Corrections to the source conversations

| # | Chat said | Correct | Where to check |
|---|---|---|---|
| 1 | PG 17 `xlp_magic` is `0xD10D` / `0x7741` | `0xD116` | `xlog_internal.h` `XLOG_PAGE_MAGIC`; this repo's `wal_fuse_ops_test.cc` |
| 2 | Long page header adds 8 bytes | adds 16 (`sysid` 8, `seg_size` 4, `xlog_blcksz` 4); total 40 | `xlog_internal.h` |
| 3 | `XLP_FIRST_IS_CONTRECORD` = `0x0004` | `0x0001` (`0x0004` = `XLP_BKP_REMOVABLE`) | `xlog_internal.h` |
| 4 | RMGR Heap = 9, Btree = 10 | Heap2 = 9, Heap = 10, Btree = 11; Standby = 8 | `rmgrlist.h` |
| 5 | `xl_info` low 4 bits are rmgr-specific; `XLR_HAS_BACKUP_BLOCK` | high 4 bits are rmgr-specific; that flag predates 9.5 | `xlogrecord.h` |
| 6 | Concurrent WAL writes reach the file system | `XLogWrite` runs under `WALWriteLock`, one writer at a time; tail page is rewritten | `xlog.c` |
| 7 | Walsender sends 8 KB, one page per CopyData | `MAX_SEND_SIZE` = 128 KiB; ends at the flush ptr or rounds down to a page | `walsender.c` |
| 8 | StandbyStatusUpdate is 18 bytes | 34 bytes | protocol docs; `wal_sender.cc` reads `body[33]` |
| 9 | Physical stream sends no page headers (answered about data pages) | WAL page headers are always in the byte stream | — |
| 10 | Clean shutdown writes nothing meaningful to WAL | writes a shutdown checkpoint record | `xlog.c` `CreateCheckPoint` |
| 11 | Standby does no checkpoint at shutdown | does a shutdown restartpoint (no WAL) | `CreateRestartPoint` |
| 12 | `pg_ctl promote` = just SIGUSR1 | writes the `promote` file, then SIGUSR1; SIGUSR1 alone does nothing | `pg_ctl.c`, `postmaster.c` `CheckPromoteSignal` |
| 13 | Deleting `standby.signal` on a running standby promotes it | only read at startup | `xlogrecovery.c` |
| 14 | Promotion writes a checkpoint before opening | fast promotion writes `END_OF_RECOVERY`; checkpoint runs asynchronously | `xlog.c` |
| 15 | History reasons like "target time reached" | strings from `getRecoveryStopReason()` (e.g. `after transaction N`) | `xlogrecovery.c` |
| 16 | Partial record at promotion is "truncated" | recovery ends at the last valid record; PG 15+ (and back-patched) may write `OVERWRITE_CONTRECORD`; old last segment → `.partial` | `xlogrecovery.c`, `xlog.c` |
| 17 | Sender exits COPY mode only on error or shutdown | also normally at the end of a historical timeline (CopyDone + next-TLI result set) | protocol docs, "Streaming Replication Protocol" |
| 18 | `pg_promote` role exists | no such role; grant EXECUTE on the function | `system_functions.sql` |
| 19 | LMDB meta page has a CRC | probably not; check `mdb_env_read_header` | `mdb.c` |
| 20 | Process status via `/proc` | Linux-only; use `libproc` on macOS | — |
| 21 | `PREPARE ... AS SELECT $1,$2,$3` fails for `$3` | the chat corrected itself: top-level `$3` resolves to `text` on PG 10+ | `parse_target.c` |

---

## 12. Action items for pg-raft

Status as of 2026-11 (open ones are also in [`todo.md`](todo.md)):

1. **Done.** `PgInstance::promote()` creates `data_dir/promote` before SIGUSR1. `role_` is still
   set before the promotion finishes ([process.md](process.md#5-open-items) item 1); the node waits
   for the new timeline instead.
2. **Done.** Timeline support in `WalSender`: `TIMELINE_HISTORY`, `START_REPLICATION ... TIMELINE
   n`, and the end-of-timeline handshake (`timeline_switch_test`).
3. **Done for history files** (`op_rename` → `TimelineOp`). Other non-segment files
   (`*.partial`, `xlogtemp.*`, `archive_status/`, `summaries/`) pass through untouched; the
   `xlogtemp.*` copy that starts a new timeline is read from the parent timeline's segment.
4. **Done.** `WalSender` advances only to the committed `flushed_lsn`.
5. **Done.** A new leader's standby receives everything up to `wal_end` before promotion;
   PostgreSQL replays to the last complete record, discards an incomplete one at the end and
   starts the new timeline there ([node.md](node.md#32-promotion-became_leader)). Demotion
   rebuilds `pg/pg_wal` up to the committed end.
6. **Done.** `write_config` sets `wal_sync_method=fsync`.
7. **Done.** `node_test` checks that the standbys converge on the final LSN.

---

## 13. Source conversations

These are taken from the claude.ai export (`conversations.json`), identified by creation date and
the first 8 hex characters of the conversation ID. The share links in `x.links` use different IDs
and could not be matched one-to-one.

| Date | ID | Title | Sections |
|---|---|---|---|
| 2025-12-30 | 2f7d06c5 | Fork-exec child process manager class | §9.1 |
| 2026-05-19 | e6b2f1f0 | Building a user space filesystem on Mac with libfuse | §9.2 |
| 2026-05-21 | 57c86d4d | PostgreSQL physical replication protocol | §1.5, §1.6, §6 |
| 2026-05-22 | 5b94b311 | PostgreSQL initdb command explained | §9.6 |
| 2026-05-22 | b4822a73 | PostgreSQL WAL format explained | §1 |
| 2026-05-22 | fc5d2c7c | PostgreSQL WAL reader | §4 |
| 2026-05-23 | 03859c66 | Understanding pg_promote function | §7, §8.1 |
| 2026-05-23 | 90529c57 | PostgreSQL WAL file flushing and syncing | §2.2, §2.3 |
| 2026-05-26 | 497da8f5 | PostgreSQL WAL sequential file operations | §2.1, §9.3 |
| 2026-05-27 | 15caa50f | Postgres WAL behavior during shutdown | §2.5 |
| 2026-05-27 | 3b8ea0ac | Standby WAL local application timing | §7 |
| 2026-05-27 | 6aad998e | PostgreSQL COPY protocol | §5.5, §6.2, §6.3, §10 |
| 2026-05-28 | 4f9df357 | PostgreSQL wire protocol overview | §5.1, §5.2 |
| 2026-05-28 | 6ab92ec0 | Standby to primary promotion process | §8 |
| 2026-05-28 | b8cf5769 | PostgreSQL replication copy mode exit conditions | §6.4, §8.3 |
| 2026-05-28 | c18f0018 | XLogDecodeNextRecord function explanation | §4.2 |
| 2026-05-31 | 1b2b82fb | Boost ASIO SSL server and client example | §9.5 |
| 2026-06-01 | ab3c02c8 | Partial writes in database files | §3 |
| 2026-06-04 | efd1937c | Checking standby replication lag in PostgreSQL | §6.5, §7, §8.5 |
| 2026-06-09 | 3e1842ef | PostgreSQL shutdown and WAL writes | §2.5, §7 |
| 2026-06-12 | 9dd7537f | File descriptor and mmap validity after rename | §9.4 |
| 2026-08-26 | 074e52c4 | Resolving boost::asio::async_connect operation canceled error | §9.5 |
| 2026-09-21 | dd8b5caf | PostgreSQL protocol explanation | §5 |
| 2026-09-21 | 72b8c34f | PostgreSQL protocol Q message flow | §5.3 |
| 2026-11-11 | 911da919 | PostgreSQL prepared statements protocol | §5.4 |
