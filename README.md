# pg-raft

High availability for PostgreSQL, by replicating the **WAL stream itself** through Raft.

pg-raft runs one node process next to each PostgreSQL server. The node intercepts every WAL
write and fsync of its PostgreSQL with a FUSE filesystem mounted over `pg_wal`. On the primary,
those writes become Raft log entries, and **an fsync returns only once its WAL is durable on a
majority**. So `synchronous_commit` becomes quorum commit, with no change to PostgreSQL. On
failure, the node with the most complete log wins the election, and its standby replays every
acknowledged byte before it is promoted.

> **Status:** experimental. Every component is implemented and covered by integration tests
> against real PostgreSQL 17, but pg-raft has not run in production. It is developed and tested
> on macOS (Apple clang 17, macFUSE). Known gaps are listed in [docs/todo.md](docs/todo.md).

## How it works

```
            client ──▶ proxy ──▶ leader's PostgreSQL (primary)
                                     │ write / fsync WAL
                                     ▼
                              FUSE over pg_wal ──▶ Raft leader
                                                     │ AppendEntries + WAL bytes
                              ┌──────────────────────┴──────────────────────┐
                              ▼                                             ▼
                        Raft follower                                 Raft follower
                        raft/wal segments                             raft/wal segments
                              │ committed WAL only                          │
                              ▼                                             ▼
                        WAL sender ──▶ standby                        WAL sender ──▶ standby
```

- **The WAL is the replicated log.** The log does not hold SQL statements. It holds the WAL
  operations the primary performs: write bytes `[off, off+size)` of a segment, flush, and
  install a new timeline. The WAL bytes travel with the entries.
- **Standbys replay committed WAL only.** Each node serves its local standby through a built-in
  implementation of PostgreSQL's physical replication protocol (a Unix-domain socket). It
  advances only to the committed LSN.
- **Failover.** A new leader's standby first receives everything in its Raft WAL, then it is
  promoted. The new timeline is itself a Raft entry, so every node learns it. The old primary is
  killed as soon as it loses leadership. Its `pg_wal` is then rebuilt from the committed WAL, and
  it restarts as a standby, with no `pg_rewind`.
- **Backups and recovery.** Standbys take `pg_basebackup`s on staggered deadlines into a shared
  backup store. Raft compaction keeps the WAL from the latest backup on. A new or wiped node
  restores the latest backup and catches up through Raft.
- **Clients connect to a proxy.** The proxy follows the Raft leader, holds connections during a
  failover, and has a small read-only HTTP admin interface.

The design is described in [docs/README.md](docs/README.md), with one document per component.

## Repository layout

| Directory | What |
|---|---|
| `src/base` | logging, `Result<T>`, config, WAL segment mmaps ([docs](docs/base.md)) |
| `src/net` | framed async messaging on Boost.Asio ([docs](docs/net.md)) |
| `src/raft` | Raft over the WAL stream, LMDB log ([docs](docs/raft.md)) |
| `src/wal_fuse` | FUSE interception of `pg_wal` ([docs](docs/wal_fuse.md)) |
| `src/wal_sender` | PostgreSQL physical replication server ([docs](docs/wal_sender.md)) |
| `src/process` | PostgreSQL process control, backup and restore ([docs](docs/process.md)) |
| `src/status` | minimal PostgreSQL client ([docs](docs/status.md)) |
| `src/backup` | the backup store ([docs](docs/backup.md)) |
| `src/node` | `pg_raft_node`, `pg_raft_initdb` ([docs](docs/node.md)) |
| `src/proxy` | `pg_raft_proxy` ([docs](docs/proxy.md)) |
| `external` | scripts that build the dependencies into `/opt/pg_raft` |

## Building

Requirements:

- A C++23 compiler (Apple clang 17).
- macFUSE, with libfuse under `/usr/local`.

Everything else is built from source into `/opt/pg_raft` by the scripts in `external/`:
Boost (with b2), LMDB, toml11, cxxopts, OpenSSL, ICU and PostgreSQL 17.

```bash
sudo mkdir -p /opt/pg_raft && sudo chown $USER /opt/pg_raft
./external/install.sh            # the dependencies, once

/opt/pg_raft/bin/b2              # build everything (from the repository root)
/opt/pg_raft/bin/b2 test         # build and run the tests
/opt/pg_raft/bin/b2 src/raft//raft_ensemble_sanity_test   # one test
```

pg-raft builds with Boost.Build (`b2`), not CMake. The binaries are written to
`src/<module>/bin/<toolset>/debug/...`. The integration tests start real PostgreSQL instances and
FUSE mounts, and skip themselves when PostgreSQL is not installed. The node tests take about two
minutes each, so run them one at a time.

## Running a cluster

1. Write a `config.toml` that every node and the proxy share:

   ```toml
   dir          = "/var/lib/pg_raft"     # each node's directory: <dir>/node<N>
   backup_store = "/shared/pg_raft/backups"
   socket_dir   = "/tmp"

   proxy_pg_port   = 5432
   proxy_http_port = 8080

   [[server]]
   hostname = "db1"
   pg_port  = 5442
   ha_port  = 7001

   [[server]]
   hostname = "db2"
   pg_port  = 5442
   ha_port  = 7001

   [[server]]
   hostname = "db3"
   pg_port  = 5442
   ha_port  = 7001

   [pg_config]                          # written into postgresql.conf
   max_connections = 100
   ```

   The proxy listens on `proxy_pg_port` (the primary) and `proxy_pg_port + 1 + i` (server `i`),
   so keep those ports clear of `pg_port` on the proxy's host.

   `backup_store` must be a directory that every node can reach: local for a single-machine
   test, or a shared filesystem. Other settings are `backup_interval_secs` (default 6 h),
   `backup_wal_size` (default 4 GiB) and `backup_pending_timeout_secs` (default 120 s).
   [docs/base.md](docs/base.md) lists them all.

2. Create the cluster once. `pg_raft_initdb` runs `initdb` and writes the first backup into the
   store:

   ```bash
   pg_raft_initdb --config config.toml
   ```

3. Start a node on each server. A node restores the latest backup, mounts FUSE, starts
   PostgreSQL as a standby and joins the Raft group. The elected leader promotes its PostgreSQL:

   ```bash
   pg_raft_node --config config.toml --id 0     # 1 and 2 on the other servers
   ```

4. Start the proxy and connect through it:

   ```bash
   pg_raft_proxy --config config.toml
   psql -h proxy-host -p 5432 postgres          # the primary
   psql -h proxy-host -p 5433 postgres          # server 0, whatever its role (5434: server 1, ...)
   curl http://proxy-host:8080/status.json      # or /status.html, /primary, /health
   ```

## Limitations

- The cluster's membership is fixed by `servers` in `config.toml`.
- No TLS between nodes. Client TLS passes through the proxy to PostgreSQL.
- Backups go to a directory; there is no S3 store.
- A PostgreSQL that crashes on its own is not restarted yet, and a failed promotion does not
  always step down. See [docs/todo.md](docs/todo.md).

## License

MIT. See [LICENSE](LICENSE).
