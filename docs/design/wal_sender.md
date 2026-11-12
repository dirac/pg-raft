# `wal_sender` — PostgreSQL physical-replication server

Source: `src/wal_sender/` — `wal_sender.{h,cc}`, `wal_sender_server.{h,cc}`.

## 1. Purpose

On each Raft follower, the WAL bytes that pg-raft has replicated sit in local segment files. The
local PostgreSQL standby has to replay them. Rather than reinventing recovery, pg-raft **acts as
a primary towards the standby**. It implements just enough of the streaming replication
protocol for an unmodified PostgreSQL standby to connect with `primary_conninfo` and stream WAL.

The key property is that **the sender never streams past the LSN that pg-raft allows**.
`advance(lsn)` is the only way to move the stream forward, and the Raft layer calls it with the
*committed* `WalCommand::flushed_lsn`. A standby therefore never applies WAL that could later be
truncated by a leader change.

## 2. Components

### 2.1 `WalSenderServer`

This is a **Unix-domain-socket** acceptor. Standbys connect locally.

- `start()` looks for a free `socket_dir/.s.PGSQL.<port>`. If the file already exists, it tries
  the next port number until one is free. It then binds and listens with a backlog of 16. The
  standby uses `primary_conninfo = 'host=<socket_dir> port=<bound_port()>'`.
- For each connection it creates a `WalSender`, calls `start()` on it, and hands it to
  `on_sender`. The owner keeps it so it can call `advance()` and `shutdown()` later.
- `shutdown()` closes the acceptor and removes the socket file. Running senders are not
  affected.
- The constructor takes the **identity of the stream**: `system_id` (from `pg_controldata` of
  the cluster), `timeline`, the initial `flush_lsn`, the WAL directory, and the segment size.

### 2.2 `WalSender`: one replication connection

Every handler runs on `strand_`. The public `start`, `advance`, and `shutdown` calls post onto
it, so they are safe from any thread.

#### Phase 1: Startup
- It reads `int32 len` and the body.
- `SSLRequest (80877103)` gets the reply `'N'`, and the sender reads the startup message again.
- `CancelRequest (80877102)` closes the connection.
- For a StartupMessage, it replies with `AuthenticationOk`, `ParameterStatus` for
  `server_version=17.0`, `integer_datetimes=on`, and `server_encoding=UTF8`, then
  `BackendKeyData` and `ReadyForQuery('I')`. **No authentication** is done, because the socket
  is local and protected by filesystem permissions.

#### Phase 2: Command (simple-query `'Q'` only)
| Query (case-insensitive substring/prefix) | Reply |
|---|---|
| `IDENTIFY_SYSTEM` | One row: `systemid`, `timeline`, `xlogpos=end_lsn_`, `dbname=NULL` |
| `TIMELINE_HISTORY` | Empty row set. Single-timeline only, see the open items. |
| `SHOW wal_segment_size` | The configured segment size. Any other `SHOW` returns an empty string. |
| `SET …` | `SET` |
| `SELECT …` | One empty text column |
| `START_REPLICATION [SLOT s] [PHYSICAL] X/Y [TIMELINE t]` | Parses the first `hex/hex` token as `start_lsn`, sets `sent_lsn_ = start_lsn`, sends `CopyBothResponse`, and enters Streaming |
| `'X'` Terminate | close |
| any other | `ErrorResponse` + `ReadyForQuery` |

#### Phase 3: Streaming (CopyBoth)
- **Outbound WAL**: `do_advance(new_flush)` raises `flush_lsn_` and `end_lsn_`. Then, while
  `sent_lsn_ < flush_lsn_`:
  1. Work out the segment and file name `%08X%08X%08X` (tli, seg>>32, seg&0xFFFFFFFF).
  2. `pread` from `sent_lsn_` up to `min(segment end, flush_lsn_)`.
  3. Send `XLogData('w', start=chunk_lsn, end=end_lsn_, now)` with that payload.
  4. Advance `sent_lsn_`.

  A missing file or a short read stops the loop. The next `advance()` call retries.
- **Keepalive**: every 5 s, a `PrimaryKeepalive('k', end_lsn_, now, reply=0)` is sent.
- **Inbound** (`CopyData 'd'`):
  - `'r'` StandbyStatusUpdate is decoded into `StandbyStatus{written, flushed, applied}` and
    passed to `status_cb_`. If the reply-requested bit is set, an immediate keepalive is sent.
  - `'h'` HotStandbyFeedback is ignored.
  - `CopyDone` and `Terminate` close the connection.

#### Write path
`enqueue_send(Bytes)` pushes onto `send_queue_`. `drain_sends()` keeps exactly one
`async_write` in flight, so messages go out in order on the strand.

#### Timestamps and LSNs
PostgreSQL timestamps are microseconds since 2000-01-01 (`now_pg_ts`). LSNs are `uint64` and are
written to the wire as two big-endian 32-bit halves. Text LSNs use the form `X/Y`.

## 3. Role in pg-raft

```
RaftServer (follower)                 WalSender ──XLogData──▶ local standby postgres
  on commit of entry e:                   ▲
    sender->advance(e.cmd.flushed_lsn) ───┘
  StatusCb(standby flushed/applied) ──▶ Status.standby_applied_lsn in RaftResponse
```

On the leader, the same server can feed any standby that is attached directly.

## 4. Testing

| Test | Scenario |
|---|---|
| `wal_sender_test` | A real primary plus a standby that replicates through `WalSenderServer`. A loop polls `pg_current_wal_flush_lsn()` and calls `advance()`. pgbench runs, and the test checks that the LSNs and row counts converge. |
| `pg_wal_switch_test` | Same setup, plus four concurrent `pg_switch_wal()` calls during pgbench. Checks that streaming crosses forced segment boundaries. |
| `standby_wal_reset_test` | The standby is stopped, its `pg_wal` wiped, and it is restarted after more pgbench. Checks that the sender re-streams from the standby's requested LSN. |

## 5. Known issues and open items

1. **`StatusCb` is never wired by the server.** `WalSenderServer::do_accept` passes `{}`. The
   constructor and `create()` need to accept a callback, or the `SenderCb` needs to set it.
2. **Unlinked segments.** `WalFuseOps::op_unlink` renames `X` to `.X`, but `do_advance` opens only
   `X`. A standby that falls behind PostgreSQL's checkpoint-driven removal stalls silently. It
   should fall back to `.X`, as `WalSegment::open` does, or read through the shared
   `WalSegment` cache. That would also avoid an open/pread/close on every advance.
3. **Message size.** A single `XLogData` can carry up to 16 MiB, and each `advance` allocates a
   new `Bytes`. The chunk size should be capped (PostgreSQL uses 128 KiB) to bound memory and
   latency.
4. **Timelines.** `TIMELINE_HISTORY` returns nothing, and the requested `TIMELINE` in
   `START_REPLICATION` is ignored. Following a timeline switch after a failover needs a real
   history file and an end-of-timeline `CopyDone` + result set.
5. Command matching is by substring. A `SELECT` whose text contains `IDENTIFY_SYSTEM` would be
   routed to the wrong handler. It should match on the first token.
6. `server_version` is hardcoded to `17.0`. It should come from `Config::pg_major_version`.
7. Some errors close the socket without notifying the owner, which still holds the
   `shared_ptr<WalSender>`. A close callback is needed so the owner can drop it.
