# net

Async framed messaging on Boost.Asio, between raft servers and between clients (the proxy) and
raft servers. See `docs/net.md` for the full design.

## Sockets (`socket.h`)

`socket_type = std::variant<tcp_socket, ssl_socket>`. The wrappers lock a mutex around every
socket call, because reads and writes are started from different threads. `tcp_socket` also
counts its reads (`counter()`), which the connection timeout uses. The `ssl_socket` alternative
is kept so that TLS can be enabled later; nothing creates one yet.

## Connection

The base class of every framed connection. Instantiate a subclass through
`Connection::create<DerivedType>(...)`, which constructs it and calls `start()` (once). Override
`on_shutdown()` to release resources when the connection closes.

**Wire format.** Every frame starts with six little-endian 32-bit words: magic `0xDEADBEEF`, a
frame counter, then words that depend on the kind of frame:

| Kind | Header | Body |
|---|---|---|
| data | data size, attached size, xxhash32 of each | the `Data`, then the attached bytes |
| file | size (high bit set) | chunks of `[xxhash32][up to 64 KiB]` |
| status | request, or response with a status byte | none |

A checksum mismatch closes the connection with `EBADMSG`. A bad magic number or frame counter
stops the reading and closes it.

**Operations.** All of these can be called from any thread, and the callbacks are always posted to
the io_context:

- `write_data(Data, {buf0, buf1}, handler)` / `write_data(Data, handler)` — send a message,
  optionally followed by up to two attached buffers, which the peer receives as one. The caller
  keeps the buffers alive until the handler runs.
- `read_data_and_buff(handler)` → `Result<tuple<Data, Bytes>>`; `read_data(handler)` for messages
  without attached bytes.
- `write_file(path, handler)` / `read_file(handler)` — stream a file from disk to a temp file on
  the other side, without holding all of it in memory.
- `request_peer_status()`; `status()` / `set_peer_status()` carry a one-byte status on heartbeats.

**Liveness.** With a timeout `t`, a status request goes out every `clamp(t/10, 100 ms, 1 s)`. The
connection closes with `ETIMEDOUT` when the socket read counter hasn't moved for `t`.

**Shutdown** (`shutdown()` / `shutdown(ec)`) is idempotent: it cancels the timers, fails the
pending handlers with `connection_aborted`, closes the socket (with a TLS shutdown and a 5 s
limit for TLS), then calls `on_shutdown()`.

## Server

An abstract TCP acceptor. Subclass it and implement `start()` (call `do_accept()`),
`new_connection(peer, socket)` and `on_shutdown()`. Instantiate it with
`Server::create<DerivedType>(...)`.

**Handshake.** The server sends one byte, which is 1 if TLS follows (always 0 for now). It then
reads the client's 64-byte NUL-terminated identity (for example `Raft:2`) and passes it to
`new_connection()` as `peer`. The handshake must complete within 10 s.

Accept errors caused by a single connection keep the accept loop going; resource errors (no file
descriptors or memory) retry after 100 ms; any other error shuts the server down. `shutdown()`
cancels the acceptor, and existing connections keep running.

## Data

A pooled buffer of 8 KiB pages (`kPageSize`), which is also a `std::basic_streambuf`.
`to_data<T>(obj)` serializes with Boost.Serialization (binary by default), and
`from_data<T>(data)` deserializes into a `Result<T>` (`EBADMSG` for a malformed message).

## Other

- **`HostAddress`** — a plain `{string address, uint16_t port}` with serialization support and
  `to_string()`.
- **`connect(io, identity, host)` / `async_connect(...)`** — resolve the host, connect, read the
  server's TLS byte (fails with `ENOTSUP` if it is set), send the identity (at most 63
  characters), and return the socket.

## Tests

`data_test`, `connection_test` (messages with one and with two attached buffers, status,
heartbeats, files, the timeout), `async_connect_test`.
