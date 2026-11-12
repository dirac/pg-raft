# pg-raft — Component Design

This directory has one design document per component under `src/`. Each one describes how the
component works as the code stands today (the code is the source of truth), why it is built that
way, and the known gaps.

| Component | Doc | State |
|---|---|---|
| `net` | [net.md](net.md) | Implemented, tested |
| `process` | [process.md](process.md) | Implemented, tested |
| `status` | [status.md](status.md) | Implemented, tested |
| `wal_fuse` | [wal_fuse.md](wal_fuse.md) | Implemented, tested |
| `wal_sender` | [wal_sender.md](wal_sender.md) | Implemented, tested |
| `raft` | [raft.md](raft.md) | Partially implemented; not in the top-level build |
| `proxy` | [proxy.md](proxy.md) | Designed; not started (empty directory) |

`base` is shared infrastructure (logger, `Result<T>`, `Int<T,L>`, `Config`, `IoThreads`,
`WalSegment`), and `CLAUDE.md` describes it. This set covers `WalSegment` because `wal_fuse` and
`raft` both depend on it. See [wal_fuse.md](wal_fuse.md#walsegment-base-layer).

## System picture

pg-raft makes PostgreSQL highly available by making **the WAL stream itself the replicated
log**:

1. On every node, PostgreSQL's `pg_wal` directory is a FUSE mount (`wal_fuse`). Each WAL
   `write()` and `fsync()` is intercepted.
2. On the leader, intercepted writes become Raft log entries (`WalCommand`: open, write, flush,
   close, or timeline). The WAL bytes travel with the entries, appended to the Raft message
   (`raft`).
3. On the leader, a PostgreSQL `fsync()` blocks until the flushed range is durable on a quorum.
   This happens through `WalTracker::async_fsync`. Synchronous commit then means *quorum*
   commit.
4. On followers, the received WAL bytes are written into the local WAL segment files. A local
   PostgreSQL standby reads them through the PostgreSQL replication protocol, served by a
   `WalSenderServer` (`wal_sender`) that is advanced only to the *committed* LSN.
5. `process` runs the PostgreSQL instances (initdb, start/stop, promote, backup/restore).
   `status` queries them (LSNs, recovery state) without libpq.
6. `proxy` (planned) is the client entry point. It is a plain TCP proxy, not built on `net`.
   Port `proxy_pg_port` forwards to the current leader, and `proxy_pg_port + 1 + i` forwards to
   `servers[i]`. TLS passes through to PostgreSQL. The proxy learns the leader from the nodes
   over `RaftServerConnection`.

```
            client ──▶ proxy ──▶ leader postgres
                                     │ write/fsync WAL
                                     ▼
                              FUSE (wal_fuse) ── WalTracker ──▶ RaftServer (leader)
                                                                   │ AppendEntries
                                                                   │  + WAL bytes (MemBlocks)
                              ┌────────────────────────────────────┴───────────┐
                              ▼                                                ▼
                    RaftFollowerConnection (node B)                 RaftFollowerConnection (node C)
                      write+msync WalSegment                          write+msync WalSegment
                              │ commit_idx advances                            │
                              ▼                                                ▼
                    WalSender.advance(flushed_lsn)                   WalSender.advance(flushed_lsn)
                              │ PG replication protocol                        │
                              ▼                                                ▼
                       standby postgres B                               standby postgres C
```

## Module dependencies

```
base  ◀── net ◀── raft
  ▲        ▲        │
  │        └────────┤ (raft uses net::Connection / Server / async_connect)
  ├── process       │
  ├── status        │
  ├── wal_fuse ◀────┘ (raft implements WalTracker; shares WalSegment)
  └── wal_sender
```

`base` depends on nothing in this repo. Only `raft` pulls the layers together, and it is the only
module that is not yet built (see [raft.md](raft.md#build-status)).

## Cross-cutting conventions

- **Errors**: functions return `Result<T>` (`std::expected<T, std::error_code>`). `elog()`
  logs an error and returns it. Unrecoverable storage errors in raft call `system_error()`,
  which exits the process.
- **Async model**: one shared `boost::asio::io_context`, run by `IoThreads`. Each
  connection-like object serializes its I/O through its own strand. Objects are owned by
  `shared_ptr`, and each completion handler captures `self()` to keep its object alive.
- **Creation**: networking objects are built through `create<T>(...)` factories that call
  `start()` after construction, so `shared_from_this()` is valid when the first I/O is posted.
- **Typed integers**: `RaftTerm`, `RaftIndex`, and `RaftNodeId` are distinct types, so they
  cannot be mixed by accident.
