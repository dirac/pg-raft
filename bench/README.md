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

The scripts keep their data, logs and results next to themselves, so put a copy of this directory
(or the whole repository) where the clusters should live, on a fast local disk, on every machine:

```bash
cp -r bench /data/testing && cd /data/testing
```

Requirements on every machine: pg-raft's dependencies and PostgreSQL 17 in `/opt/pg_raft` (see the
top-level README), FUSE, `curl` and `python3`. The PostgreSQL binaries (`pgbench`, `psql`) are the
ones in `PG_BIN` (`env.sh`, with the pgbench defaults and `RESULTS_DIR`).

Edit before the first run:

- `raft/config.toml`: `dir` (each machine's node data), `backup_store` (a directory every node
  machine reaches, e.g. an NFS mount), the `[[server]]` hostnames and ports, and `pg_hba` if the
  proxy runs on another machine.
- `std/cluster.sh`: `HOSTS` / `PORTS` (node 0 is the primary), and `PG_HBA` for other machines.

## A run on one machine

One setup at a time, on a fresh cluster each time:

```bash
# pg-raft
./raft/install_bins.sh         # release build into raft/bin; deletes old backups and node data
./raft/init.sh                 # initdb + the cluster's first backup, into backup_store
./raft/start.sh                # 3 nodes + proxy; waits for the primary and every node
./raft/bench.sh                # pgbench through the proxy (--direct: to the primary itself)
./raft/stop.sh

# streaming replication
./std/prepare.sh               # deletes the old cluster's data
./std/init.sh                  # primary + 2 standbys from base backups, started; waits for quorum
./std/bench.sh
./std/stop.sh
```

(`raft/reset.sh --yes` and `std/reset.sh --yes` also delete a cluster without rebuilding.)

## A run on several machines

Three node machines and a client machine (proxy and pgbench), all with the same `config.toml` /
`cluster.sh`.

pg-raft:

| Where | Step |
|---|---|
| every node machine | `./raft/install_bins.sh`: the binaries, and deletes the old backups and node directories |
| one node machine | `./raft/init.sh`, **after** every `install_bins.sh` (a later one would delete the new backup) |
| node machine *i* | `./raft/start_node.sh i` |
| client | `./raft/install_bins.sh --keep` (only the binaries: the proxy), `./raft/start_proxy.sh`, `./raft/wait.sh`, `./raft/bench.sh` |
| client, then every node machine | `./raft/stop.sh` |

Streaming replication:

| Where | Step |
|---|---|
| every node machine | `./std/prepare.sh`: deletes the old cluster's data |
| node 0's machine | `./std/init_primary.sh` |
| node *i*'s machine (1, 2) | `./std/init_standby.sh i` |
| client | `./std/wait.sh`, `./std/bench.sh` |
| every node machine | `./std/stop.sh` (`./std/start.sh` restarts the nodes that are there) |

Connections from other machines need `pg_hba` lines: in `raft/config.toml` (`pg_hba`, written by
`init.sh`; the client's network, for the proxy) and `std/cluster.sh` (`PG_HBA`; the standbys'
`replication` and the client's network). Without them only 127.0.0.1 is trusted, and pg-raft asks
other addresses for a `scram-sha-256` password.

## pgbench and results

pgbench takes its parameters from the environment (defaults in `env.sh`: scale 100, 16 clients,
4 threads, a 60 s warm-up, then 300 s measured). The warm-up runs the same workload unmeasured, so
the start-up isn't counted; leader elections and promotion are over before it, since `bench.sh`
starts once the cluster serves. `WARMUP=0` skips it.

```bash
SCALE=100 CLIENTS=16 THREADS=4 DURATION=300 ./raft/bench.sh
PGBENCH_ARGS="-N" ./std/bench.sh --no-init      # simple-update, reusing the loaded tables
```

Each run writes `results/<setup>-<time>/` on the machine that ran it: `settings.txt` (the
parameters and the PostgreSQL settings in effect), `init.log`, `warmup.log` (not measured), `run.log` (10 s progress and
per-statement latencies) and `summary.txt`.

## The scripts

| `raft/` | |
|---|---|
| `config.toml` | the cluster: nodes, ports, backup store, `pg_hba`, commit-path syncs, `[pg_config]` |
| `install_bins.sh [--keep]` | release build into `raft/bin`; without `--keep`, deletes the backups and this machine's node directories |
| `init.sh` | `pg_raft_initdb`: initdb and the first backup |
| `start_node.sh id` | one node, in the background (log in `raft/logs/`) |
| `start_proxy.sh` | the proxy, in the background |
| `wait.sh [secs]` | where the proxy runs: wait for a primary, then for every node's PostgreSQL |
| `start.sh` | one machine: every node, the proxy, `wait.sh` |
| `bench.sh [--direct] [--no-init]` | pgbench through the proxy, or straight to the primary |
| `stop.sh [ids]` | the proxy and the nodes running here: followers first, the primary last |
| `reset.sh --yes` | one machine: stop, and delete the nodes' data and the backup store |

| `std/` | |
|---|---|
| `cluster.sh` | the cluster: `HOSTS`, `PORTS`, `DATA_ROOT`, `PG_HBA` |
| `postgresql.std.conf` | appended to the primary's `postgresql.conf` (the standbys copy it) |
| `prepare.sh` | stop and delete the nodes whose data is on this machine |
| `init_primary.sh` | initdb node 0, configure and start it |
| `init_standby.sh i` | base-backup node *i* from the primary, as standby `s<i>`, and start it |
| `init.sh` | one machine: `init_primary.sh`, `init_standby.sh 1` and `2`, `wait.sh` |
| `wait.sh [secs]` | from anywhere: the primary is up and both standbys stream as quorum members |
| `start.sh`, `stop.sh` | the nodes whose data is on this machine |
| `bench.sh [--no-init]` | pgbench against the primary, after checking the quorum |
| `reset.sh --yes` | one machine: `prepare.sh` |

## pg-raft notes

- **Commit-path syncs.** `raft/config.toml` has `leader_sync`, `follower_sync` and `log_sync`, all
  `false`: a commit is durable once a majority has the WAL in its page cache (durability by
  replication). Set them to `true` to measure the cost of syncing to disk as well.
- **Use a fresh cluster per run** (`install_bins.sh`, `init.sh`). Restarting a stopped pg-raft
  cluster can leave a standby unable to rejoin ("requested timeline N is not a child of this
  server's history"); `wait.sh` then fails and names the node. That is a known pg-raft bug
  (`docs/todo.md`).
- Stopping takes ~20 s: once the others are gone, the last node can't commit PostgreSQL's shutdown
  checkpoint (no quorum), so after 12 s it fails the fsync and PostgreSQL PANICs, as intended.
  `start_node.sh` disables core dumps for that reason.
- Backups: `config.toml` pushes the periodic backups out (1 day or 1 TiB of WAL) so that none
  starts in the middle of a run. Lower `backup_interval_secs` / `backup_wal_size` to include them.
- The proxy's admin page: http://<proxy machine>:8080/status.html.
