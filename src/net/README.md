# net

Async networking layer built on Boost.Asio with optional TLS. Provides framed message passing, file streaming, and connection lifecycle management.

## Connection

Async framed-message connection over TCP or TLS (`socket_type = std::variant<tcp_socket, ssl_socket>`). Messages are framed with a 4-word header.

Instantiate via `Connection::create<DerivedType>(...)` — this calls `start()` automatically. Never construct directly. Override `on_shutdown()` in your subclass to clean up when the connection closes.

Four I/O operations, all callback-based and safe to call from any thread (an internal strand serializes them):
- `read_data()` / `write_data()` — send `Data` objects serialized via Boost.Serialization
- `read_file()` / `write_file()` — stream raw file content directly from/to disk without buffering the whole file in memory

Status exchange (heartbeat messages) and a read timeout are managed automatically. The connection tears itself down if the peer goes silent.

## Server

Abstract TCP accept loop. Subclass and implement:
- `start()` — called once after the acceptor is bound
- `new_connection(socket)` — called for each accepted socket; create and return a `Connection` subclass
- `on_shutdown()` — cleanup

Instantiate via `Server::create<DerivedType>(...)`. Call `shutdown()` to cancel the acceptor; existing connections are not affected.

## Data

Pool-backed scatter/gather I/O buffer with `kPageSize = 8192`. Implements `std::basic_streambuf` so it works directly with `std::ostream`/`std::istream`. Serialize objects into a `Data` with `to_data<T>(obj)` and deserialize with `from_data<T>(data)` (Boost.Serialization binary format by default).

## Other

**`HostAddress`** — Plain `{string address, uint16_t port}` with Boost.Serialization support and `to_string()`.

**`async_connect` / `connect`** — Establish outbound TCP or TLS connections. `connect` is synchronous; `async_connect` takes a completion callback.
