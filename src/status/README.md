# status

PostgreSQL client for querying a running instance using the extended query protocol over a raw TCP socket — no libpq dependency.

## PgClient

`PgClient` speaks just enough of the PostgreSQL frontend/backend wire protocol to prepare and execute queries asynchronously.

**Connecting and preparing:**
```cpp
auto client = PgClient::connect(port).value();   // synchronous TCP connect + auth
auto stmt   = client->prepare("SELECT ...").value();  // Parse/Describe/Sync round-trip
```

`prepare` is synchronous and returns a `PgStmt` handle (just a named statement string). Reuse the handle for repeated executions — no re-parsing overhead.

**Executing:**
```cpp
client->async_exec(stmt, [](Result<Rows> rows) {
    // rows is vector<vector<optional<string>>>
});
```

Multiple `async_exec` calls queue up and execute one at a time in submission order. The callback fires on the Asio io_context thread. `close()` tears down the socket.

**Result types** (defined in this header, used across modules):
```cpp
using Row  = vector<std::optional<string>>;  // nulls are std::nullopt
using Rows = vector<Row>;
struct PgStmt { string name; };
```

## Design notes

The connection uses a single `asio::io_context::strand` for all async I/O, with a `std::mutex`-protected queue for incoming `async_exec` requests posted from other threads. The send path (startup, prepare) is synchronous to simplify setup; only `async_exec` is fully async. This client is used in tests to query LSNs and row counts without needing an external `psql` binary.
