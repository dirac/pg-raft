# `net` — Framed async messaging over TCP

Source: `src/net/` — `connection.{h,cc}`, `socket.h`, `server.{h,cc}`,
`async_connect.{h,cc}`, `data.{h,cc}`, `host_address.h`. The checksum is
`src/base/xxhash.{h,cc}`.

## 1. Purpose

`net` is the transport for every pg-raft node-to-node and client-to-node channel. Raft peers,
Raft followers, and admin clients all use it. It provides:

- Delivery of **typed messages** (Boost.Serialization objects), in order, over a long-lived
  connection. Each message is checksummed.
- **Attached buffers**: raw bytes sent right after a message without copying them into the
  `Data`. Raft uses this to send WAL bytes alongside its requests.
- **Bulk file transfer** that streams from disk to disk, with a checksum on every chunk. It is
  used for snapshots and backups.
- **Liveness**: heartbeats ("status requests"), and closing a connection when nothing has been
  read from it for the timeout.
- A one-byte **peer status** piggybacked on heartbeats. Raft uses it to tell clients whether a
  node is the leader without a separate request.

The layer does not decide what is inside a message. Subclasses read and write `Data` and
interpret it.

## 2. Components

### 2.1 `HostAddress`
`{string address; uint16_t port}`. It is serializable and ordered, and has `to_string()` and a
`std::formatter`.

### 2.2 `Data`: pooled message buffer

```
struct Data : std::basic_streambuf<char>, vector<array<char, 8192>*>
```

- Its storage is a list of 8 KiB pages from `MemoryPool<kPageSize>`. A `Data` can grow without
  reallocating or copying.
- **Serialization**: `to_data<T>(obj, binary=true)` writes a format byte (`'b'` binary or `'t'`
  text). A Boost archive then writes into the streambuf (`overflow()` adds pages), and `pack()`
  records the size.
- **Deserialization**: `from_data<T>(data)` reads the content through an `Istreambuf` over the
  pages and returns `Result<T>`. A bad format byte or an archive exception becomes `EBADMSG`.
- **Socket I/O**: `const_buffer()` / `mutable_buffer()` return one asio buffer per page, for
  gather writes and scatter reads.

Attached bytes are **not** part of `Data`. They travel as a separate section of the frame (see
§2.4).

### 2.3 `socket.h`: socket wrappers

`socket_type = std::variant<tcp_socket, ssl_socket>`. These are thin wrappers around the asio
TCP socket and TLS stream:

- They **lock a mutex around every socket call**. The read chain and the write chain start
  operations from different threads, so the calls have to be serialized.
- `tcp_socket` **counts `async_read_some` calls** (`counter()`). The connection timeout uses
  this count. A long message being received keeps starting reads, so it is not mistaken for a
  silent peer.

The `ssl_socket` alternative is kept so that TLS can be enabled later. Nothing creates one yet
(see §6).

### 2.4 `Connection`

The base class of every framed connection. Instantiate a subclass through
`Connection::create<DerivedType>(...)`, which constructs it and calls `start()` (once); override
`on_shutdown()` to release resources when the connection closes.

#### Wire format

Every frame starts with a header of **six little-endian 32-bit words**. The header is read in one
call.

| Word | Meaning |
|---|---|
| `w0` | Magic `0xDEADBEEF` |
| `w1` | Frame counter: 1, 2, 3, … per direction. The receiver checks for an exact `+1`. |
| `w2` | The top two bits are the frame kind |
| `w3`–`w5` | Depend on the kind |

| Kind (`w2 >> 30`) | Header | Body |
|---|---|---|
| `0b0x` **data** | `w2` = data size (< 2³¹), `w3` = attached size, `w4` = xxhash32(data), `w5` = xxhash32(attached), or 0 when not computed | data bytes, then the attached bytes |
| `0b10` **file** | `w2` = `0x80000000 \| size>>32`, `w3` = `size & 0xFFFFFFFF` | chunks of `[xxhash32 of the chunk][up to 64 KiB]` |
| `0b11` **status** | `w2 & 0xFF` = 0 for a request, or `s+1` for a response carrying peer status `s` (0–254) | none |

Data frames are checksummed only when `Connection::set_frame_checksums(true)` (config
`frame_checksums`, off by default: TCP checksums the segments already, and xxhash32 of every frame
was a noticeable part of a commit); a sender with them off writes 0, and a receiver verifies a
non-zero checksum only, so peers with different settings interoperate. File chunks are always
checksummed. The receiver drops the connection with `EBADMSG` on a checksum mismatch. On a bad magic number
or frame counter it **stops reading**, because the sizes in that header can't be trusted, and
shuts down.

#### Read side

There is one read loop per connection: `do_read_header` → `process_header` → (data | file |
status) → `do_read_header`. All reads are composed `async_read`s of an exact size.

- **Data**: one scatter read into a `Data` of `w2` bytes plus a `Bytes` of `w3` bytes. Both
  checksums are verified. The pair is pushed onto `read_data_q_`.
- **File**: read in 64 KiB chunks into a new `tmp_file()`. Each chunk's checksum is verified
  before the chunk is written. The file path is pushed onto `read_file_q_`. On error the file is
  deleted.
- **Status**: a request queues a status response. A response clears
  `outstanding_status_request_` and calls the virtual `set_peer_status(s)`.

Consumers call:

- `read_data_and_buff(handler)` for `Result<tuple<Data, Bytes>>`.
- `read_data(handler)` for messages that have no attached bytes.
- `read_file(handler)`.

**Reader thread** (`threaded_reader()`, TCP only; both ends of raft's leader-to-follower
connections): instead of the asio loop, a thread of the connection's own reads frames from a dup of
the socket (`recvmsg` with `MSG_DONTWAIT`, data and status frames; a file frame closes the
connection), sleeping in `ppoll` until data arrives (asio's sockets are non-blocking, and a dup
shares the flag), and hands each frame to its handler on that thread: no hand-off to an io thread,
and no second read for the body. Writes stay on the io_context: they are queued, so a slow peer
never blocks the thread that sends (raft sends with its lock held). The thread holds the connection
and ends when the socket is shut down.

Each call takes a queued item or parks the handler until one arrives. Handlers are **posted**,
never invoked inline, so a handler can safely call `read_*` again. The queue elements are wrapped
in non-template structs, which avoids a libc++ `std::expected` recursion under C++26.

#### Write side

`write_data(Data, {buf0, buf1}, handler)` / `write_data(Data, handler)` and
`write_file(path, handler)` add an entry to a typed queue and a token to `write_queue_`, all under
`write_mtx_`. If the connection is closed, they post `connection_aborted` and return. Only the
first token in the queue starts `do_write`, which runs on the strand. Each completion dequeues the
entry, posts the user's handler, and continues with the next token.

- **Data**: the header, the `Data` pages, and the attached buffers (up to two) go out in **one
  gather `async_write`**. The caller must keep the attached buffers alive until the handler runs.
  `RaftPeerConnection` copies the WAL bytes out of the segment mmap into a `shared_ptr<Bytes>`
  and captures it in the handler (see [raft.md](raft.md#22-wal-payload-memblock)).
- **File**: open it, `fstat` it for the size, write the header, then send checksummed 64 KiB
  chunks.
- **Status**: a status message is inserted at position 1 of `write_queue_`. It goes out right
  after the frame currently being written, so heartbeats never wait behind a long queue. At most
  one status request is in flight.
- `write_test_delay()` puts a 5 s pause into the write queue. It exists for the timeout test.

At most one read and one write are outstanding at any time. `outstanding_read_req_` and
`outstanding_write_req_` enforce this, and a violation triggers `breakpoint()`.

#### Liveness

For `Connection(peer, socket, timeout)` with `timeout > 0`, `timeout_and_times(timeout)` gives a
check period of `clamp(timeout/10, 100 ms, 1 s)` and a count of `max(10, timeout/period)`. On
every period the connection:

1. Sends a status request, so a healthy peer always has something to send back.
2. Compares the socket read counter with its value at the last check. If it hasn't changed for
   the full count, the connection shuts down with `ETIMEDOUT`.

`RaftPeerConnection` uses `Options::timeout_secs` (10 s). Follower-side connections pass no
timeout and rely on the leader's heartbeats.

#### Shutdown

`shutdown(ec)` / `shutdown()` is idempotent, guarded by `closed_`. It:

1. Cancels the timeout timer.
2. Fails every pending read and write handler with `connection_aborted`.
3. Logs the reason.
4. Posts the socket close to the strand:
   - TCP: `shutdown(both)` + `close`.
   - TLS: `async_shutdown`, with a 5 s timer that force-closes the socket if the peer doesn't
     respond.
5. Calls `on_shutdown()`, which subclasses override to release their resources.

Nothing sleeps or busy-waits on an io thread. The destructor logs a warning if `shutdown()` was
never called.

### 2.5 `Server`

This is an abstract TCP acceptor. `create<T>(...)` constructs the server and calls `start()`,
and the subclass calls `do_accept()` from `start()`. The acceptor is wrapped in a mutex because
it is accessed from different threads.

**Handshake** (`HandshakeConnection`, must finish within 10 s):

1. The server sends **one byte**: `1` if TLS follows. It is always `0` for now.
2. The server reads the client's **64-byte NUL-terminated identity**, for example `"Raft:2"`.
3. It calls `new_connection(identity, socket)`, which the subclass uses to pick a connection
   type.

**Accept errors**:

- Transient errors (`connection_aborted`, `connection_reset`, `interrupted`, `try_again`): keep
  accepting.
- Resource errors (out of file descriptors, buffers, or memory): retry after 100 ms
  (`accept_later`).
- Anything else: shut the server down.

`shutdown()` cancels and closes the acceptor and calls `on_shutdown()`. Existing connections keep
running.

### 2.6 `async_connect` / `connect`

The client half of the handshake works in these steps:

1. Resolve the address, unless it is a literal IP.
2. `async_connect` to the endpoint list.
3. Read the server's TLS byte. A non-zero byte fails with `ENOTSUP`, because TLS isn't supported
   yet.
4. Write the 64-byte identity (truncated to 63 characters).
5. Return the socket as a `tcp_socket`.

`connect()` is a blocking wrapper that uses a promise.

## 3. Threading model

- Socket calls are serialized by the `socket.h` wrappers.
- Write initiation (`do_write`) runs on the connection's strand. TLS completions are also bound
  to the strand.
- The public API (`read_*`, `write_*`, `shutdown`, `request_peer_status`) can be called from any
  thread. Queue state is protected by `ReadQ::mtx` and `write_mtx_`. The timer has its own mutex
  so `async_wait` and `cancel` can be called from different threads.
- User callbacks always run on an io_context thread through `asio::post`.

## 4. Testing

| Test | Covers |
|---|---|
| `data_test` | Text and binary round-trips across page boundaries. Takes about 50 s, close to its 60 s limit, and times out under ASAN. |
| `connection_test` | Messages in each direction and order, messages with one and with **two** attached buffers, status request/response, background heartbeats, file transfer, the read-silence **timeout** (using `write_test_delay`), and the `timeout_and_times` values |
| `async_connect_test` | Connect and handshake |
| `base/xxhash_test` | Known xxHash32 vectors, and that splitting the input across buffers gives the same hash |

All of these pass in a normal build, under AddressSanitizer (except `data_test`'s time limit),
and under ThreadSanitizer.

## 5. Changes from the previous version

- The header was 3–4 host-order words. It is now 6 little-endian words, so the wire format is
  incompatible with the old version.
- Data frames are checksummed. Attached bytes are a separate section with its own size, instead
  of following an `#END` marker inside `Data`.
- File transfer uses per-chunk xxhash32 instead of a whole-file SHA-256, and `sha256.{h,cc}` was
  removed.
- The timeout uses the socket read counter instead of last-activity timestamps, and there is no
  longer a separate status timer.
- Shutdown cancels the timers and always closes the socket. It no longer sleeps on io threads.
- Bugs fixed: `write_data`/`write_file` called the handler twice on a closed connection, and the
  identity `strncpy` used `max` instead of `min`, which could overflow its buffer.
- `Server` gained the handshake timeout, the accept-error handling, and the TLS byte.

## 6. Design notes

| Feature | Current state | Why |
|---|---|---|
| TLS | The TLS byte is always 0; `ssl_socket` is kept in the variant | `base` has no certificate support yet |
| Peer identity | The handshake identity is passed through as `peer` | Raft dispatches on the identity itself (`Raft:<id>`) |
| Attached buffers | Up to two (`inplace_vector<const_buffer, 2>`) | Raft sends WAL from two segments in one message |
| Callbacks | `std::function` | Apple clang 17's libc++ lacks `std::move_only_function` |
| `from_data` | Returns `Result<T>` (`EBADMSG` on a bad message) | pg-raft's error convention |

## 7. Open items

1. **TLS**: needs certificate support in `base` (certificates, key pairs and an SSL context). The handshake and `Connection` already handle both
   alternatives of `socket_type`.
2. `Server::acceptor_cancel` still sleeps in 10 ms steps while waiting for the accept loop to
   finish.
