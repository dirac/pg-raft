# wal_sender

Asio-based server that speaks the PostgreSQL physical streaming replication protocol. A standby connects to `WalSenderServer` exactly as it would connect to a primary's built-in `walsender` process.

## WalSender

Handles one replication connection. Reads WAL directly from a `wal_dir` on disk and streams it to the connected standby.

**Protocol phases:**
1. **Startup** — handles the PostgreSQL startup packet, SSL decline, auth-ok, parameter status messages
2. **Command** — responds to `IDENTIFY_SYSTEM`, `START_REPLICATION`, `TIMELINE_HISTORY`, `SHOW`, `SET`, and simple `SELECT` queries
3. **Streaming** — after `START_REPLICATION`, enters `CopyBoth` mode and streams `XLogData` messages; sends periodic keepalives every 5 seconds; handles `StandbyStatusUpdate` replies from the standby

**WAL delivery:** Call `advance(flush_lsn)` from any thread to notify the sender that WAL is available up to `flush_lsn`. The call is posted onto the sender's strand, which then reads from `wal_dir` using `pread` and enqueues `XLogData` messages. WAL segment filenames are derived from the LSN: `%08X%08X%08X` (timeline, segment-high-word, segment-low-word).

An optional `StatusCb` is called each time a `StandbyStatusUpdate` arrives from the standby, delivering `StandbyStatus { written_lsn, flushed_lsn, applied_lsn }`.

All I/O runs on an `asio::io_context::strand` — no dedicated thread per connection.

## WalSenderServer

TCP accept loop. On each accepted connection, creates a `WalSender` and calls the user-supplied `SenderCb` with it.

```cpp
auto server = WalSenderServer::create(
    port, wal_dir, system_id, timeline, initial_flush_lsn,
    [](shared_ptr<WalSender> s) { /* store s, call s->advance() later */ });
server->start();
// ...
server->shutdown();  // cancels acceptor; existing senders keep running
```

Pass `port = 0` to let the OS pick an ephemeral port; read it back with `bound_port()`.

## Typical usage pattern

```
WalSenderServer (accepts connection)
       │
       └─▶ WalSender (one per standby)
                 │
       caller calls advance(lsn) ──▶ WalSender reads wal_dir, sends XLogData
                 │
       standby sends StandbyStatusUpdate ──▶ StatusCb fires
```

The caller is responsible for polling or being notified of new WAL (e.g. via a FUSE `fsync` hook or a `pg_current_wal_flush_lsn()` query) and calling `advance()` accordingly.
