# wal_sender

An Asio server that speaks PostgreSQL's physical streaming replication protocol. A standby connects
to `WalSenderServer` exactly as it would connect to a primary's built-in walsender. The stream
never goes past the LSN that pg-raft allows; the raft node advances it to the committed
`flushed_lsn`.

## WalSenderServer

A **Unix-domain-socket** accept loop:

```cpp
auto server = WalSenderServer::create(
    socket_dir, port, wal_dir, system_id, timeline, initial_flush_lsn,
    [](shared_ptr<WalSender> s) { /* keep s, call s->advance() later */ });
if (!server->start()) { ... }
// standby: primary_conninfo = 'host=<socket_dir> port=<server->bound_port()>'
server->shutdown();
```

- `start()` binds `socket_dir/.s.PGSQL.<port>`. If that file exists it tries the next port, and so
  on. It returns `false` on error. `bound_port()` and `socket_path()` report what was bound.
- Each accepted connection gets a `WalSender`, which is started and passed to the callback.
- `shutdown()` closes the acceptor and removes the socket file. Running senders are not affected.

## WalSender

Handles one replication connection. Everything runs on a strand; `start()`, `advance()` and
`shutdown()` can be called from any thread.

**Protocol phases:**
1. **Startup** — declines `SSLRequest` (`'N'`), closes on `CancelRequest`, and answers the startup
   message with AuthenticationOk (no authentication: the socket is local),
   `server_version = 17.0`, `integer_datetimes`, `server_encoding`, BackendKeyData and
   ReadyForQuery.
2. **Command** (simple query) — `IDENTIFY_SYSTEM` (the latest timeline), `TIMELINE_HISTORY n`
   (the history file from `wal_dir`), `SHOW` (only `wal_segment_size` has a value), `SET`,
   `SELECT` (one empty column), and `START_REPLICATION … X/Y [TIMELINE t]`, which starts
   streaming timeline `t` from `X/Y`.
3. **Streaming** (CopyBoth) — `XLogData` messages up to the flush LSN, a keepalive every 5 s, and
   `StandbyStatusUpdate` handling (a keepalive in reply when one is requested).

**WAL delivery.** `advance(LogSeqNumber flush_lsn)` raises the committed WAL (with its timeline),
then reads the segments from `wal_dir` with `open`/`pread` and sends them up to it.

**Timelines.** Later timelines are learned from the history files (`%08X.history`) in `wal_dir`,
which raft writes when it applies a `TimelineOp`. A historic timeline is streamed up to its
switch point; then the sender sends CopyDone and, after the standby's, the next timeline
(`next_tli`, `next_tli_startpos`), and the standby continues on it. Bytes before a timeline's start
point are read from the parent timeline's segment. Segment file names are
`%08X%08X%08X` (timeline, segment high word, segment low word). A missing segment stops the
reading until the next `advance()`.

`StatusCb` receives `StandbyStatus { written_lsn, flushed_lsn, applied_lsn }`. Note that
`WalSenderServer` currently creates its senders without one.

## Tests

All three need PostgreSQL and skip if it is absent:
- `wal_sender_test` — a primary and a standby through `WalSenderServer`, with pgbench.
- `pg_wal_switch_test` — concurrent `pg_switch_wal()` calls during pgbench.
- `standby_wal_reset_test` — the standby's WAL is wiped while it is down, and is streamed again.
- `timeline_switch_test` — a failover: another standby is promoted to timeline 2, and the standby
  behind `WalSender` follows it onto timeline 2.
