# status

A PostgreSQL client for querying a running instance. It speaks the extended query protocol over a
raw TCP socket, without libpq.

## PgClient

```cpp
auto client = PgClient::connect(port).value();        // sync: TCP + startup + ReadyForQuery
auto stmt   = client->prepare("SELECT ...").value();  // sync: Parse/Describe/Sync
client->async_exec(stmt, [](Result<Rows> rows) { ... });
client->close();
```

- `connect(port)` connects to `127.0.0.1:port` as `$USER` (or `postgres`) to the `postgres`
  database. Only `trust` authentication works, which matches the loopback rule that
  `PgInstance::write_hba` adds.
- `prepare(sql)` returns a `PgStmt` (the name of a server-side prepared statement), so later
  executions skip the parsing.
- `async_exec(stmt, cb)` runs Bind/Execute/Sync and delivers `Result<Rows>`, with every column as
  text. Calls queue up and run one at a time in submission order. An error response fails the
  call, but the client still reads up to ReadyForQuery, so the connection stays usable.
- `close()` sends Terminate and closes the socket.

**Result types** (also used elsewhere):
```cpp
using Row  = vector<std::optional<string>>;  // nulls are std::nullopt
using Rows = vector<Row>;
struct PgStmt { string name; };
```

## Design notes

The async exec chain runs on a strand. The queue of requests is protected by a mutex, so
`async_exec` can be called from any thread, and the callbacks run on the strand. `connect`,
`prepare` and `close` are synchronous and share the socket and read buffers with the exec chain,
so don't call `prepare()` while an `async_exec` is in flight. Prepare the statements first.

Uses: the tests query LSNs and row counts with it. The raft node is meant to poll
`pg_is_in_recovery()`, `pg_current_wal_flush_lsn()` and `pg_last_wal_replay_lsn()` with it, for
`RaftServer::set_pg_status()`.

## Tests

`pg_client_test` (needs PostgreSQL; skips if it is absent).
