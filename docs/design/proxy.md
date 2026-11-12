# `proxy` — Client entry point (proposed)

Source: `src/proxy/` is **empty**, and the module is not in the build. This document is a
proposed design. It is drawn from the hooks that already exist in the other modules: the
`Config` fields, the `RaftServerConnection` leadership status, and `PgInstance`. Treat it as a
starting point, not a description of existing code.

## 1. Purpose

Clients need one stable address that always reaches the current PostgreSQL primary, which is the
Raft leader, and that fails over without client reconfiguration. The proxy:

- Listens on `Config::proxy_pg_port` (default 5432) for PostgreSQL client connections and
  forwards each one to the leader's `servers[i].pg_port`.
- Listens on `Config::proxy_http_port` (default 8080) for a few admin commands: status, leader,
  step-down, and backup.

## 2. Leader discovery

The proxy needs no extra protocol for this. It keeps one `RaftServerConnection` to each
`servers[i].ha_port`. The connection uses the client identity in the `net` handshake, so the node
creates a `RaftClientConnection` for it. It also sets a short `timeout`, so heartbeats run often.

- Each heartbeat response carries the node's status byte, which
  `RaftClientConnection::status()` computes:
  - `0x3`: the node is the leader and serving.
  - `0x1`: the node is the leader but not yet serving. It is still committing inherited entries,
    or it is promoting.
  - `0x0`: the node is not the leader.
- `set_peer_status` updates `is_leader()` and `is_serving()` on each connection. The proxy's
  current target is the only node reporting `0x3`. If none does, there is no target.
- `async_leadership_check()` forces an immediate status round-trip before routing a new
  connection, if the cached state is older than one heartbeat.

## 3. Connection forwarding

```
client ──TCP──▶ proxy:5432 ──TCP──▶ leader:pg_port
          (byte-for-byte splice after the startup phase)
```

1. Accept the client. Read the first packet. If it is an `SSLRequest`, either reply `'N'` or
   pass TLS through (to be decided).
2. Pick a target: the node that is serving. If there is none, wait up to a configurable
   failover window (for example 30 s) for one to appear, then reply with an `ErrorResponse`
   carrying SQLSTATE `57P03` (cannot_connect_now).
3. Connect to the target and replay the startup packet. Then copy bytes in both directions until
   either side closes. Authentication happens end to end between the client and PostgreSQL.
4. When leadership changes, close the forwarded connections to the old leader. Their
   transactions are lost anyway, and clients reconnect to the new target.

Implement this on the shared io_context with one object per session and two `async_read_some` →
`async_write` pumps. Use a buffer pool, for example `MemoryPool<kPageSize>` from `net::Data`.

## 4. Admin HTTP interface (suggested)

| Endpoint | Action |
|---|---|
| `GET /status` | `node_status()` from every node: leader, applied_idx, primary_flushed_lsn, standby_applied_lsn |
| `GET /leader` | Current target node |
| `POST /stepdown` | Ask the leader to step down (needs a client request type in `client_types.h`) |
| `POST /backup` | Trigger and stream a backup (`SnapshotRequest`) |

## 5. Open questions

1. TLS: terminate at the proxy, or pass through to PostgreSQL?
2. Read-only routing: should read-only sessions go to standbys whose `standby_applied_lsn` is
   close enough to the primary?
3. Proxy HA: should every node run a proxy, with clients given all nodes, or should one proxy
   run behind a VIP?
