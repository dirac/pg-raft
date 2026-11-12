# `proxy` — Client entry point (design)

Source: `src/proxy/` is **empty**, and the module is not in the build. This document is the
agreed design.

## 1. Purpose

The proxy gives clients fixed addresses for the PostgreSQL servers:

- **`proxy_pg_port`** (default 5432) always reaches the **PostgreSQL running on the Raft
  primary (leader) server**: `servers[i].hostname : servers[i].pg_port`, where `i` is the node
  Raft reports as leader. Clients connect there for normal use and follow failovers without
  reconfiguration.
- **`proxy_pg_port + 1 + i`** reaches **`servers[i]`** directly, whatever its role (5433 →
  server 0, 5434 → server 1, …). This is for administration, monitoring, and reading from a
  specific standby on purpose.

| Proxy port | Target |
|---|---|
| `proxy_pg_port` | `servers[leader].hostname : servers[leader].pg_port`, the PostgreSQL on the Raft leader node |
| `proxy_pg_port + 1` | `servers[0].hostname : servers[0].pg_port` |
| `proxy_pg_port + 2` | `servers[1].hostname : servers[1].pg_port` |
| … | … |

## 2. Design decisions

| Question | Decision |
|---|---|
| Forwarding | A plain TCP proxy along the lines of [Arash Partow's Boost.Asio TCP proxy](https://www.partow.net/programming/tcpproxy/index.html). It is **not** built on `src/net`, because it forwards PostgreSQL's own protocol byte for byte and has no frames of its own. |
| Leader discovery | `RaftServerConnection` (`src/net`) to every node's `ha_port`. `net` is used only for this control channel. |
| TLS | **Passed through to PostgreSQL.** The proxy never parses or terminates TLS: the `SSLRequest`, the handshake, and the encrypted stream are all forwarded as opaque bytes, and PostgreSQL holds the certificates. |
| Read-only routing | **No.** The primary port always goes to the primary. Reading from a standby on purpose uses its per-server port. |
| Proxy HA | **Not required.** One proxy process. If HA is ever needed, it will come from the deployment (Kubernetes, for example). |

## 3. Forwarding (Partow-style bridge)

Each listening port has an acceptor. Each accepted client gets a **bridge** object that owns two
sockets:

```
client ──▶ downstream socket ─┐                 ┌─▶ upstream socket ──▶ PostgreSQL
                              └──── bridge ─────┘
```

1. **Accept** the client on the downstream socket.
2. **Pick the target.** For a per-server port the target is fixed. For the primary port it is the
   PostgreSQL on the current Raft leader node (§4).
3. **Connect upstream** asynchronously to `hostname:pg_port`. If the connection fails, close
   the client socket.
4. **Pump both directions**, as in Partow's design:
   - downstream `async_read_some` into a fixed buffer → `async_write` the bytes upstream →
     read again;
   - upstream `async_read_some` into a second buffer → `async_write` the bytes downstream →
     read again.

   Each direction has at most one read or write outstanding, so the buffers need no copying or
   queueing. A slow side applies back-pressure naturally.
5. **Close**: an error or EOF on either side closes both sockets, once (Partow guards this with a
   mutex; an atomic flag works too). The bridge is held by `shared_ptr`, captured in its
   handlers, so it is freed when the last handler finishes.

The proxy never looks at the bytes it forwards. That is what makes TLS pass-through work, and it
keeps the proxy independent of the PostgreSQL protocol version.

Implementation notes:

- Run on the shared `io_context` with `IoThreads`, one strand per bridge.
- Set `TCP_NODELAY` on both sockets. PostgreSQL traffic is latency-sensitive request/response.
- Enable TCP keepalive on both sockets so half-dead peers are noticed.
- Track the live bridges per target (a weak list), so they can be closed when the primary
  changes (§4).

## 4. Following the primary

The primary port routes each **new** connection to whichever server is primary at accept time.

- **No primary** (during an election or a promotion): hold the accepted client for up to a
  failover window (for example 30 s) and retry, then close the client socket. Because the proxy
  doesn't speak the protocol, it does not send a PostgreSQL `ErrorResponse`. The client sees a
  closed connection and reconnects.
- **Primary changes**: close every bridge on the primary port that points to the old primary.
  Their transactions are lost anyway, because the old primary is demoted. Clients reconnect and
  land on the new primary. Bridges on per-server ports are not touched.

### 4.1 Leader discovery

The proxy keeps one `RaftServerConnection` to each node at `servers[i].hostname :
servers[i].ha_port`. These connect with a non-Raft identity (for example `"proxy"`), so each node
serves the proxy with a `RaftClientConnection`. Each connection is created with a short timeout,
so heartbeats are frequent: one every `clamp(timeout/10, 100 ms, 1 s)`.

- Every heartbeat response carries the node's status byte, computed by
  `RaftClientConnection::status()`:
  - `0x3`: leader and serving.
  - `0x1`: leader, but not serving yet. It is still committing inherited entries, or promoting.
  - `0x0`: not the leader.
- `RaftServerConnection::set_peer_status` stores the byte. `is_leader()` and `is_serving()` read
  it, and both return false once the connection is closed.
- **The primary is the one node whose connection reports `is_serving()`.** The proxy connects to
  the PostgreSQL on that node, at its `hostname:pg_port`. If no node reports serving, or if
  more than one does during a transition, there is no primary, and the proxy holds new
  connections as described above.
- When accepting on the primary port, the proxy uses the cached state if it is no older than one
  heartbeat. Otherwise it calls `async_leadership_check()` on the candidate connection to force a
  fresh status round-trip.
- When a node's connection drops, the proxy reconnects with `async_connect` on a retry interval,
  like `PeerConnectionMgr::reconnect`. If the dropped node was the primary, the proxy treats it as
  a primary change: it closes the bridges to that node and waits for a new node to report `0x3`.
- When the serving node changes, the proxy closes the primary-port bridges to the old node.

Raft decides who the leader is, so this channel is authoritative. Polling PostgreSQL's
`pg_is_in_recovery()` instead could briefly see two primaries during a failover.

## 5. Configuration

The proxy reads `Config` (`config.toml`):

- `proxy_pg_port`: the primary port. The per-server ports are `proxy_pg_port + 1 + i`.
- `servers[i].hostname`, `servers[i].pg_port`: the targets.
- `proxy_http_port`: reserved for an admin interface. Nothing listens on it yet (§6.2).

The ports `proxy_pg_port .. proxy_pg_port + servers.size()` must not collide with a PostgreSQL
listening on the same host. In particular, a server co-located with the proxy cannot use 5432
when the proxy does.

## 6. Open items

1. **Raft dependency.** `RaftServerConnection` and `RaftClientConnection` are in `src/raft`,
   which is not built yet. The proxy needs at least the `raft_client` library
   (`raft_server_connection.cc`, already a separate target in `raft/Jamfile.jam`) to build, and
   the node side (`RaftClientConnection::status()`) to be live.
2. **Admin interface** on `proxy_http_port` (status, current primary). Not designed yet.
3. **Failover window and timeouts**: the hold time when there is no primary, and the upstream
   connect timeout.
