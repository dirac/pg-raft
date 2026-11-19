# `wal_sender` — PostgreSQL physical-replication server

Source: `src/wal_sender/` — `wal_sender.{h,cc}`, `wal_sender_server.{h,cc}`.

## 1. Purpose

On each Raft follower, the WAL bytes that pg-raft has replicated sit in local segment files. The
local PostgreSQL standby has to replay them. Rather than reinventing recovery, pg-raft **acts as
a primary towards the standby**. It implements just enough of the streaming replication
protocol for an unmodified PostgreSQL standby to connect with `primary_conninfo` and stream WAL.

The key property is that **the sender never streams past the LSN that pg-raft allows**.
`advance(LogSeqNumber)` is the only way to move the stream forward, and the node calls it with the
*committed* `RaftState::flushed_lsn` as entries are applied (and with `wal_end` when it promotes,
[node.md](node.md#32-promotion-became_leader)). A standby therefore never applies WAL that could
later be truncated by a leader change.

## 2. Components

### 2.1 `WalSenderServer`

This is a **Unix-domain-socket** acceptor. Standbys connect locally.

```cpp
auto server = WalSenderServer::create(
    socket_dir, port, wal_dir, system_id, timeline, initial_flush_lsn,
    [](shared_ptr<WalSender> s) { /* keep s, call s->advance() later */ },
    [](StandbyStatus const& st) { /* the standby's written/flushed/applied LSNs */ });
if (!server->start()) { ... }
// standby: primary_conninfo = 'host=<socket_dir> port=<server->bound_port()>'
server->shutdown();
```

- `start()` looks for a free `socket_dir/.s.PGSQL.<port>`. If the file already exists, it tries
  the next port number until one is free. It then binds and listens with a backlog of 16. The
  standby uses `primary_conninfo = 'host=<socket_dir> port=<bound_port()>'`.
- For each connection it creates a `WalSender`, calls `start()` on it, and hands it to
  `on_sender`. The owner keeps it so it can call `advance()` and `shutdown()` later.
- `bound_port()` and `socket_path()` report what was bound.
- Every sender reports the standby's `StandbyStatusUpdate`s to the `status_cb` passed to
  `create()`.
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
  `server_version=<major>.0` (`WalSender::set_pg_major_version`, from `Config::pg_major_version`), `integer_datetimes=on`, and `server_encoding=UTF8`, then
  `BackendKeyData` and `ReadyForQuery('I')`. **No authentication** is done, because the socket
  is local and protected by filesystem permissions.

#### Phase 2: Command (simple-query `'Q'` only)
| Query (case-insensitive substring/prefix) | Reply |
|---|---|
| `IDENTIFY_SYSTEM` | One row: `systemid`, the latest `timeline`, `xlogpos=end_lsn_`, `dbname=NULL` |
| `TIMELINE_HISTORY n` | One row: `filename` (`%08X.history`) and `content` (the file, from `wal_dir`), or an error if there is no such file |
| `SHOW wal_segment_size` | The configured segment size. Any other `SHOW` returns an empty string. |
| `SET …` | `SET` |
| `SELECT …` | One empty text column |
| `START_REPLICATION [SLOT s] [PHYSICAL] X/Y [TIMELINE t]` | Parses the first `hex/hex` token as `start_lsn` and `TIMELINE t` (default: the latest). An unknown `t`, or a start past `t`'s switch point, is an error. Otherwise sets `sent_lsn_ = start_lsn`, sends `CopyBothResponse`, and enters Streaming on `t` |
| `'X'` Terminate | close |
| any other | `ErrorResponse` + `ReadyForQuery` |

#### Phase 3: Streaming (CopyBoth)
- **Outbound WAL**: `advance(LogSeqNumber)` raises `flush_` (the committed WAL, on its timeline)
  and `end_lsn_`, then `send_wal()` streams the timeline `t` up to the limit: the committed WAL
  (if `flush_` is on an earlier timeline than `t`, only up to where that timeline ended) and
  `t`'s switch point. For each chunk:
  1. Find the timeline whose range holds the position: `t`, or for bytes before `t`'s start point
     an ancestor (see below). Work out its segment file `%08X%08X%08X` (that timeline, seg>>32,
     seg&0xFFFFFFFF).
  2. `pread` up to `min(segment end, limit, that timeline's end)`.
  3. Send `XLogData('w', start=chunk_lsn, end=end_lsn_, now)` with that payload.
  4. Advance `sent_lsn_`.

  A missing file or a short read stops the loop. The next `advance()` call retries.
- **Timelines**: the sender starts on its `timeline` and learns later ones from the history files
  (`%08X.history`) in `wal_dir`, which raft writes when it applies a `TimelineOp`. It checks for
  new ones on `IDENTIFY_SYSTEM`, `START_REPLICATION`, and when `advance()` brings a later
  timeline. The latest history file gives every timeline's range (it is cumulative).
- **End of a timeline**: when `t` is historic and `sent_lsn_` reaches its switch point, the
  sender sends CopyDone and waits for the standby's CopyDone, then answers with the result set
  `(next_tli, next_tli_startpos)`, CommandComplete, CommandComplete `START_STREAMING` and
  ReadyForQuery, and goes back to the Command phase. The standby then fetches the next
  timeline's history and starts streaming it, which PostgreSQL's walreceiver does by itself.
- **The new timeline's first segment**: a standby that moves to timeline `t+1` restarts at the
  start of the segment that holds the switch point. On promotion PostgreSQL fills the bytes before
  the switch point by copying the old segment through an `xlogtemp` file, which the FUSE layer
  doesn't replicate, so in `raft/wal` they are zeros. The sender reads them from the parent
  timeline's segment, which holds the same bytes.
- **Keepalive**: every 5 s, a `PrimaryKeepalive('k', end_lsn_, now, reply=0)` is sent.
- **Inbound** (`CopyData 'd'`):
  - `'r'` StandbyStatusUpdate is decoded into `StandbyStatus{written, flushed, applied}` and
    passed to `status_cb_`. If the reply-requested bit is set, an immediate keepalive is sent.
  - `'h'` HotStandbyFeedback is ignored.
  - `CopyDone` from the standby (it ends the stream) is answered with CopyDone,
    CommandComplete `START_STREAMING` and ReadyForQuery; at the end of a historic timeline, with
    the next timeline (above). `Terminate` closes the connection.
- **Keepalive** messages are only sent while streaming.

#### Write path
`enqueue_send(Bytes)` pushes onto `send_queue_`. `drain_sends()` keeps exactly one
`async_write` in flight, so messages go out in order on the strand.

#### Timestamps and LSNs
PostgreSQL timestamps are microseconds since 2000-01-01 (`now_pg_ts`). LSNs are `uint64` and are
written to the wire as two big-endian 32-bit halves. Text LSNs use the form `X/Y`.

## 3. Role in pg-raft

Every node runs one `WalSenderServer` (`Node`), on `config.socket_dir` and port `pg_port + 20000`
(so that it can't take another node's PostgreSQL socket in the same directory), reading raft's
`wal_dir` (`raft/wal`). Its standby's `primary_conninfo` points at it.

```
RaftObserver::applied(state)          WalSender ──XLogData──▶ local standby postgres
  sender->advance(state.flushed_lsn) ───▲
became_leader: advance(state.wal_end) ──┘
StatusCb(standby flushed) ──▶ the promotion waits until the standby has flushed wal_end
```

## 4. Testing

| Test | Scenario |
|---|---|
| `wal_sender_test` | A real primary plus a standby that replicates through `WalSenderServer`. A loop polls `pg_current_wal_flush_lsn()` and calls `advance()`. pgbench runs, and the test checks that the LSNs and row counts converge. |
| `pg_wal_switch_test` | Same setup, plus four `pg_switch_wal()` calls, 150 ms apart, while pgbench runs. Checks that streaming crosses forced segment boundaries. |
| `standby_wal_reset_test` | The standby is stopped, its `pg_wal` wiped, and it is restarted after more pgbench. Checks that the sender re-streams from the standby's requested LSN. |
| `timeline_switch_test` | A failover: another standby is promoted to timeline 2 (through `PgInstance::promote()`) after the primary stops; the standby behind `WalSender`, reading a directory that mirrors the WAL as `raft/wal` would, follows onto timeline 2 and converges. |

## 5. Open items

1. **Unlinked segments.** `do_advance` opens `X` only, never `.X` (what `WalFuseOps::op_unlink`
   renames a removed segment to). In a node the sender reads `raft/wal`, which has no `.X` files,
   so this only matters for a sender reading a live `pg_wal` (the tests). Reading through the
   shared `WalSegment` cache would also avoid an open/pread/close on every advance.
2. **Message size.** A single `XLogData` can carry up to 16 MiB, and each `advance` allocates a
   new `Bytes`. The chunk size should be capped (PostgreSQL uses 128 KiB) to bound memory and
   latency.
3. (Done: `server_version` comes from `Config::pg_major_version`, `WalSender::set_pg_major_version`.)
4. Some errors close the socket without notifying the owner, which still holds the
   `shared_ptr<WalSender>`. A close callback is needed so the owner can drop it (the node keeps
   weak pointers, so they just expire).
5. Command matching: `IDENTIFY_SYSTEM`, `START_REPLICATION` and `TIMELINE_HISTORY` match on the
   start of the command; anything else that is not `SET`, `SHOW` or `SELECT` is an error.
6. The end-of-timeline reply (the result set, then a second CommandComplete) follows PostgreSQL's
   walreceiver code; it works with PostgreSQL 17, but it was not compared byte for byte with
   PostgreSQL's own walsender.
