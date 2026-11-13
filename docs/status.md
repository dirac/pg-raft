# `status` — Minimal PostgreSQL wire-protocol client

Source: `src/status/` — `pg_client.{h,cc}`.

## 1. Purpose

pg-raft needs to read a few facts from each local PostgreSQL instance, and it needs to do so
cheaply and often:

- `pg_is_in_recovery()`
- `pg_current_wal_flush_lsn()` on the primary
- `pg_last_wal_replay_lsn()` on a standby

These values feed `RaftServer::set_pg_status()`, which reports them in the client channel's
`StatusResponse` (the proxy's admin interface shows them), and the primary's flush LSN becomes
the `flushed_lsn` of the leader's raft entries. The tests also use `PgClient` to check row counts
and LSN convergence.

`PgClient` speaks the PostgreSQL v3 frontend/backend protocol directly over a TCP socket. This
avoids libpq, keeps everything on the shared Asio io_context, and makes queries non-blocking.

## 2. Interface

```cpp
auto c  = PgClient::connect(port).value();        // sync: TCP + startup + wait for ReadyForQuery
auto st = c->prepare("SELECT ...").value();       // sync: Parse/Describe/Sync
c->async_exec(st, [](Result<Rows> r) { ... });    // async: Bind/Execute/Sync
c->close();                                       // sync: Terminate + close
```

`Row = vector<optional<string>>`, where NULL is `nullopt`. `Rows = vector<Row>`. Every column is
returned in **text format**.

## 3. Protocol handling

### Connect (synchronous)
1. TCP-connect to `127.0.0.1:port` with `TCP_NODELAY`.
2. Send the StartupMessage: protocol `3.0`, `user=$USER` (or `postgres` if unset), and
   `database=postgres`.
3. Drain messages until `ReadyForQuery ('Z')`:
   - `AuthenticationOk` (`'R'` with code 0) is accepted. Any other `'R'` fails with
     `permission_denied`. **Only `trust` authentication is supported**, which matches the default
     loopback HBA rule from `PgInstance::write_hba`.
   - `ErrorResponse ('E')` fails with `protocol_error`. The `M` field is logged.
   - `ParameterStatus` and `BackendKeyData` are ignored.

### Prepare (synchronous)
The client sends `Parse(name=N, sql, 0 param types)`, `Describe('S', N)`, and `Sync`. `N` is a
per-client counter. It reads until `'Z'`, ignores `1/t/T/n`, and returns
`invalid_argument` on `'E'`. The returned `PgStmt` is the server-side statement name, so later
executions skip parsing.

### Execute (asynchronous)
Requests are queued in `queue_` under `mu_`. `running_` keeps exactly one exec cycle in flight
per connection.

```
enqueue ─(first item)─▶ post(strand_, start_exec)
start_exec : pop req; build Bind(unnamed portal, stmt, 0 params, text results)
             + Execute(unlimited rows) + Sync  →  do_send
do_send    : async_write  →  do_read_hdr
do_read_hdr: read 5 bytes (type + len) → do_read_body → on_msg
on_msg     : '2','C',default → keep reading
             'D' → parse DataRow into Row, append to rows_
             'E' → log, set exec_failed_, keep reading (drain to 'Z')
             'Z' → finish_exec(exec_failed_ ? io_error : rows_)
finish_exec: invoke cb; if the queue is non-empty, start_exec the next request
```

The design always drains to `ReadyForQuery`, even after an error. Because the extended protocol
ends every request with `Sync`, the connection is always back in a known state before the next
request starts. Each `DataRow` is bounds-checked, and a malformed message fails the exec with
`protocol_error`.

## 4. Threading

- All async I/O and the exec state (`exec_cb_`, `rows_`, `hdr_`, `body_`) are confined to
  `strand_`.
- `queue_` and `running_` are protected by `mu_`, so `async_exec` can be called from any thread.
- Callbacks run on the strand. A callback may call `async_exec` again.
- `connect` and `prepare` are blocking and use the same socket and header buffer. **Do not call
  `prepare` while an `async_exec` is in flight** (see open item 1). `close` runs on the strand
  (below) and may be called at any time.

## 5. Use in pg-raft

The node's status poller (`Node::poll`, every 500 ms) prepares this statement once per
connection, and reconnects when an exec fails or takes more than 2 s:

```sql
SELECT pg_is_in_recovery()::text,
       CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_flush_lsn()::text END,
       CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_walfile_name(pg_current_wal_flush_lsn()) END,
       CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()::text END
```

On the primary, the flush LSN and the timeline (the first 8 hex digits of the WAL file name) go to
`NodeWalTracker::set_flushed_lsn` (the `flushed_lsn` of the leader's entries, the "safe to stream"
bound of the followers' WAL senders) and to `set_pg_status`; on a standby, the replay LSN goes to
`set_pg_status`. The node's promotion also uses one-off queries (`pg_is_in_recovery()`, the
current WAL file name).

## 6. Testing

`pg_client_test` starts a real PostgreSQL. It covers connecting, preparing and executing simple
queries, NULL handling, error queries, and many queued `async_exec` calls completing in order.

## 7. Open items

1. **Sync and async calls share state.** `prepare()` reads into `hdr_`, which the async read
   chain also uses. Preparing while an exec is running corrupts both. Possible fixes are to
   prepare everything up front (the current usage), or to route `prepare` through the async
   queue.
2. Only trust authentication works. SCRAM would be needed for non-loopback use.
3. Only loopback connections are supported (the address is hardcoded to `127.0.0.1`).
4. No query parameters can be passed (Bind always sends 0 parameters).
5. Every value is returned as text: callers parse LSNs and numbers themselves.

`close()` runs on the strand (it used to close the socket from the caller's thread while an exec's
read was pending, a data race in asio): an exec in flight is cancelled and its callback gets an
error, and Terminate is only sent when nothing is in flight.
