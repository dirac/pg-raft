# Benchmark: PostgreSQL streaming replication vs pg-raft

Scripts that set up two 3-node PostgreSQL HA clusters on one machine, with the same PostgreSQL
settings, and run the same pgbench workload against each.

| | `std/` | `raft/` |
|---|---|---|
| Nodes | primary `pg0` (5532) + standbys `s1` (5533), `s2` (5534) | 3 pg-raft nodes (PostgreSQL on 5441–5443, raft on 7001–7003) |
| Commit | quorum sync replication: `synchronous_standby_names = 'ANY 1 (s1, s2)'` | quorum commit through raft (2 of 3) |
| Clients | the primary, port 5532 | the proxy, port 5432 (`--direct`: the primary's own port) |
| Backups | none | pg-raft's backup store (`backup_store` in `raft/config.toml`) |
| Data | `std/data/pg{0,1,2}` | `raft/data/node{0,1,2}` (`dir` in `raft/config.toml`) |

Both setups use what pg-raft forces on its PostgreSQL (`wal_sync_method = fsync`,
`wal_recycle = off`), data checksums, and the shared settings in `raft/config.toml`'s
`[pg_config]` and `std/postgresql.std.conf` (keep the two the same). Every result records the
settings actually in effect.

## Setup

The scripts keep their data, logs and results next to themselves, so copy this directory to where
the clusters should live (a fast local disk), then point `raft/config.toml` there:

```bash
cp -r bench /data/testing && cd /data/testing
# raft/config.toml: `dir` (the nodes' data) and `backup_store` (a directory every node can reach,
# e.g. an NFS mount); the ports if they clash with something.
PG_RAFT_REPO=/path/to/pg-raft ./raft/install_bins.sh   # release build of pg-raft into raft/bin
```

Requirements: pg-raft's dependencies and PostgreSQL 17 in `/opt/pg_raft` (see the top-level
README), FUSE, `curl` and `python3`. `env.sh` holds the shared settings: `PG_BIN`, the pgbench
defaults, `RESULTS_DIR`.

## A comparison run

Run one setup at a time, on a fresh cluster each time:

```bash
# pg-raft
./raft/reset.sh --yes          # delete the old cluster and its backups (skip the first time)
./raft/init.sh                 # initdb + the cluster's first backup, into backup_store
./raft/start.sh                # 3 nodes + proxy; waits for the primary and every node
./raft/bench.sh                # pgbench through the proxy (--direct: to the primary itself)
./raft/stop.sh

# streaming replication
./std/reset.sh --yes           # skip the first time
./std/init.sh                  # primary + 2 standbys from base backups, started; waits for quorum
./std/bench.sh
./std/stop.sh
```

pgbench takes its parameters from the environment (defaults in `env.sh`: scale 100, 16 clients,
4 threads, 300 s):

```bash
SCALE=100 CLIENTS=16 THREADS=4 DURATION=300 ./raft/bench.sh
PGBENCH_ARGS="-N" ./std/bench.sh --no-init      # simple-update, reusing the loaded tables
```

Each run writes `results/<setup>-<time>/`: `settings.txt` (the parameters and the PostgreSQL
settings in effect), `init.log`, `run.log` (10 s progress and per-statement latencies) and
`summary.txt`.

## The scripts

| `raft/` | |
|---|---|
| `config.toml` | the cluster: nodes, ports, backup store, commit-path syncs, `[pg_config]` |
| `install_bins.sh` | release build of `pg_raft_node`, `pg_raft_initdb`, `pg_raft_proxy` into `raft/bin` |
| `init.sh` | `pg_raft_initdb`: initdb and the first backup |
| `start.sh [ids]` | `start_node.sh` for each node, `start_proxy.sh`, then wait for a primary and every node |
| `start_node.sh id`, `start_proxy.sh` | one node / the proxy, in the background (logs in `raft/logs/`) |
| `bench.sh [--direct] [--no-init]` | pgbench through the proxy, or straight to the primary |
| `stop.sh [ids]` | the proxy, then the followers, then the primary |
| `reset.sh --yes` | stop, and delete the nodes' data and the backup store |

| `std/` | |
|---|---|
| `postgresql.std.conf` | appended to every node's `postgresql.conf` |
| `init.sh` | initdb the primary, base-backup the two standbys, start all three |
| `start.sh`, `stop.sh` | start (and wait for both standbys to stream) / stop |
| `bench.sh [--no-init]` | pgbench against the primary, after checking both standbys are quorum members |
| `reset.sh --yes` | stop, and delete the data |

## pg-raft notes

- **Commit-path syncs.** `raft/config.toml` has `leader_sync`, `follower_sync` and `log_sync`, all
  `false`: a commit is durable once a majority has the WAL in its page cache (durability by
  replication). Set them to `true` to measure the cost of syncing to disk as well.
- **Use a fresh cluster per run.** Restarting a stopped pg-raft cluster can leave a standby unable
  to rejoin ("requested timeline N is not a child of this server's history"); `start.sh` then
  fails and names the node. That is a known pg-raft bug (`docs/todo.md`).
- `start.sh` waits for the proxy to report a primary, then for every node's PostgreSQL. The
  proxy's admin page is http://127.0.0.1:8080/status.html.
- Stopping takes ~20 s: once the others are gone, the last node can't commit PostgreSQL's shutdown
  checkpoint (no quorum), so after 12 s it fails the fsync and PostgreSQL PANICs, as intended.
  `start_node.sh` disables core dumps for that reason.
- Backups: `config.toml` pushes the periodic backups out (1 day or 1 TiB of WAL) so that none
  starts in the middle of a run. Lower `backup_interval_secs` / `backup_wal_size` to include them.

## Moving to several machines

- raft: give each `[[server]]` in `config.toml` its machine's address (the ports can then be the
  same everywhere), copy the directory and `raft/bin` to every machine, and run
  `./raft/start_node.sh <id>` on each (or `./raft/start.sh <id>`, which also starts the proxy).
  `init.sh` runs once, anywhere. `backup_store` must be reachable from every machine.
- std: `PRIMARY_HOST=<the primary's address>` for `init.sh`, and the base backups run on the
  standby machines. The scripts assume all three nodes are local; split `init.sh` per machine.
- Remote clients need `pg_hba.conf` entries: both setups only trust 127.0.0.1 (pg-raft adds
  `scram-sha-256` for other addresses, which needs a password).
