# Benchmark: PostgreSQL streaming replication vs pg-raft

Scripts that set up two 3-node PostgreSQL HA clusters, and a single PostgreSQL without HA as the
baseline, all with the same PostgreSQL settings, and run the same pgbench workload against each.

| | `solo/` | `std/` | `raft/` |
|---|---|---|---|
| Nodes | one PostgreSQL (5632), no standbys | primary `pg0` (5532) + standbys `s1` (5533), `s2` (5534) | 3 pg-raft nodes (PostgreSQL on 5441–5443, raft on 7001–7003) |
| Commit | the local flush | quorum sync replication: `synchronous_standby_names = 'ANY 1 (s1, s2)'` | quorum commit through raft (2 of 3) |
| Clients | port 5632 | the primary, port 5532 | the proxy, port 5432 (`--direct`: the primary's own port) |
| Backups | none | none | pg-raft's backup store (`backup_store` in `raft/config.toml`) |
| Data | `solo/data/pg` | `std/data/pg{0,1,2}` | `raft/data/node{0,1,2}` (`dir` in `raft/config.toml`) |

All three use data checksums and the shared settings in `raft/config.toml`'s
`[pg_config]`, `std/postgresql.std.conf` and `solo/postgresql.solo.conf` (keep them the same). The WAL file settings differ, each the better one for its setup: pg-raft
forces `wal_recycle = off` and `wal_init_zero = off` (its FUSE layer); std and solo keep
PostgreSQL's defaults (`on`). All three use `wal_sync_method = fdatasync`.
Every result records the settings actually in effect.

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
- `solo/cluster.sh`: `HOST` / `PORT`, and `PG_HBA` for the client machine.

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

# one PostgreSQL, no HA: the baseline
./solo/prepare.sh              # deletes the old data
./solo/init.sh                 # initdb, configure, start
./solo/bench.sh
./solo/stop.sh
```

(`raft/reset.sh --yes`, `std/reset.sh --yes` and `solo/reset.sh --yes` also delete a cluster
without rebuilding.)

`./matrix.sh` does all three on one machine, each with the disk syncs on and off (solo and std:
`fsync`; pg-raft: `leader_sync` and `follower_sync`), on a fresh cluster each, at each client count
in `SWEEP` (default `50 100`; pg-raft through the proxy and direct), then writes
`results/summary.md`. It builds pg-raft from `PG_RAFT_REPO` (default `~/pg-raft`) and writes its
config variants into `variants/`. On a machine without the NFS mount, point `backup_store` in
`raft/config.toml` at a local directory first.

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

One PostgreSQL: `./solo/prepare.sh` and `./solo/init.sh` on the database machine, `./solo/bench.sh`
on the client.

`multi/` does all of this from the client machine over ssh, for one client, one proxy machine and
five node machines (3- or 5-node clusters). The machines are listed in `~/machines.txt`: the proxy
first, then the nodes; the machine running the scripts is the client. A file of just three
addresses is three node machines, for 3-node clusters only, with the proxy on the client. Each machine has its local
files in `/data/bench`, and the binaries, backups and results are in `~/shared/pg_raft`, a directory
every machine mounts:

```bash
./multi/deploy.sh                    # configs, a release build, bench/ copied to every machine
./multi/run.sh raft 3                # one setup on a fresh cluster: a sweep over client counts
SWEEP="10 25 50 100 125 150" ./multi/all.sh    # solo, std 3, std 5, raft 3, raft 5, then summary.sh
```

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
| `bench.sh [--direct \| --libpq] [--no-init]` | pgbench through the proxy, straight to the primary, or with every node given to libpq (it finds the primary) |
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

| `solo/` | |
|---|---|
| `cluster.sh` | `HOST`, `PORT`, `DATA_ROOT`, `PG_HBA` |
| `postgresql.solo.conf` | appended to its `postgresql.conf` |
| `prepare.sh` | stop it and delete its data |
| `init.sh` | initdb, configure and start it |
| `start.sh`, `stop.sh` | start / stop it |
| `bench.sh [--no-init]` | pgbench against it |
| `reset.sh --yes` | `prepare.sh` |

| `multi/` | |
|---|---|
| `lib.sh` | the roles (from `~/machines.txt`), `/data/bench`, `~/shared/pg_raft`, `on HOST CMD` |
| `gen.sh` | the configs for 3 and 5 nodes and for solo, in `multi/gen/`; `RAFT_SYNC=1` turns pg-raft's commit-path syncs on, `PG_FSYNC=off` runs std and solo with `fsync = off`, `WAL_COMPRESSION` (`off`, `pglz`, `lz4`, `zstd`) sets `wal_compression` for all three (lz4 by default, from the configs), `MAX_WAL_SIZE=64GB` sets `max_wal_size` for all three (all also read by `deploy.sh`) |
| `deploy.sh [--no-build]` | `gen.sh`, a release build into `~/shared/pg_raft/bin`, bench/ copied to every machine, the binaries installed |
| `run.sh solo \| std N \| raft N \| stop` | one setup on a fresh cluster, pgbench at each client count in `SWEEP` (pg-raft: through the proxy and direct), then stop it and copy the results to `~/shared/pg_raft/results`; `VARIANT=name` tags the results (`raft-direct-sync-n3-c10-…`) |
| `all.sh [setups]` | every setup in turn, then `summary.sh` |
| `summary.sh [dir]` | one table (`summary.md`, `summary.csv`) from the results' `run.log`s |

`matrix.sh` (top level): one machine, solo / std / raft, each with syncs on and off, over `SWEEP`.

## Results

### 2026-11-13: one machine, scale 100

All six PostgreSQL instances (and the proxy and pgbench) on one EC2 `m9gd.4xlarge`: 16 vCPUs
(aarch64), 61 GB, data on the local NVMe instance store, Amazon Linux 2023 (kernel 6.18).
PostgreSQL 17.9; pg-raft at `65f4762` (release build, gcc 14), the default `config.toml`: no disk
syncs on the commit path. Each setup on a fresh cluster: `pgbench -i -s 100`, 16 clients,
4 threads, the TPC-B-like default script, a 60 s warm-up, then 300 s measured.

| Setup | tps | latency avg | latency stddev | 10 s intervals |
|---|---|---|---|---|
| streaming replication, quorum sync (`ANY 1 (s1, s2)`) | **37,623** | 0.423 ms | 0.223 ms | 37,100–38,100 |
| pg-raft, through the proxy | **17,853** | 0.895 ms | 0.657 ms | 17,500–18,000 |

pg-raft reaches 47% of streaming replication's throughput. pgbench's per-statement latencies
show where the time goes:

| Statement (ms) | std | pg-raft |
|---|---|---|
| `BEGIN` | 0.025 | 0.047 |
| `UPDATE pgbench_accounts` | 0.056 | 0.092 |
| `SELECT abalance` | 0.049 | 0.070 |
| `UPDATE pgbench_tellers` | 0.048 | 0.076 |
| `UPDATE pgbench_branches` | 0.054 | 0.102 |
| `INSERT INTO pgbench_history` | 0.039 | 0.066 |
| `END` (the commit) | 0.150 | 0.442 |

The commit is ~0.3 ms slower: PostgreSQL's WAL fsync goes through FUSE and waits for a raft round
(the `FlushOp` committed by a majority) instead of a standby's flush acknowledgement. Every other
statement is ~0.02–0.05 ms slower, mostly the extra hop through the proxy. Without a warm-up
(the first run), pg-raft averaged 17,141 tps: it ramps from ~6,000 tps in its first 10 s to its
steady rate in about a minute; streaming replication reaches its rate within 20 s.

### 2026-11-13: after one walk per segment and the writes carried with the flush

Same machine and settings. Profiling the first run showed the leader sending each follower 16 times
the WAL: every PostgreSQL backend writes through a FUSE handle of its own, and each handle's walk
published again what the others had written since its last write. Now one walk state per segment
is shared by its handles, a rewrite of the partial last page sends only the bytes that differ, and
the writes travel in the fsync's `FlushOp` (or as a `WriteOp` once 64 KiB of a segment is pending).

| Per commit (16 clients) | before | after |
|---|---|---|
| WAL written | 443 B | 440 B |
| sent to each follower | 7,248 B | **531 B** |
| raft entries | 0.60 | **0.31** |

| Setup | tps | latency avg | latency stddev | `END` (commit) | 10 s intervals |
|---|---|---|---|---|---|
| streaming replication (unchanged) | 37,623 | 0.423 ms | 0.223 ms | 0.150 ms | 37,100–38,100 |
| pg-raft, before | 17,853 | 0.895 ms | 0.657 ms | 0.442 ms | 17,500–18,000 |
| pg-raft, after | **20,349** | 0.785 ms | 0.655 ms | 0.373 ms | 20,000–20,530 |

pg-raft is at 54% of streaming replication. On one machine loopback bandwidth was not the limit,
so the 14x less traffic gave +14%; across machines it matters more (at these rates, about
200 MB/s per follower before, 15 MB/s now). What is left: the commit's raft round through FUSE
(0.37 ms vs 0.15 ms), and the proxy hop on every statement (the proxy used over two cores).

### 2026-11-13: the baseline without HA, and fewer allocations in pg-raft

Same machine and settings. `solo/` is one PostgreSQL without standbys: the ceiling for both HA
setups. pg-raft: `log_seq_number()` no longer allocates (it built three `stringstream`s per FUSE
operation), raft keeps its last WAL segments open (each use cost a `stat`, allocations and a lock),
and archives skip the codecvt facet.

| Setup | tps | % of solo | latency avg | latency stddev | `END` (commit) |
|---|---|---|---|---|---|
| solo (one PostgreSQL, no HA) | **55,093** | 100% | 0.288 ms | 0.099 ms | 0.066 ms |
| streaming replication, quorum sync | 37,623 | 68% | 0.423 ms | 0.223 ms | 0.150 ms |
| pg-raft, through the proxy | **20,839** | 38% | 0.766 ms | — | 0.356 ms |

Profile of pg-raft under this load (`perf`, DWARF call graphs; CPU in % of one core):
`pg_raft_proxy` 236%, the leader node 201%, a follower 104%, pgbench 64%. In the nodes, about 40%
of the time is in system calls and wake-ups (thread hand-offs: FUSE thread, raft lock, I/O threads),
`xxhash32` is 5.6% of the leader and 9.9% of a follower (LMDB's page checksums on each commit, plus
the network frames), and `std::locale` construction still 5% of the leader: Boost's binary archive
builds a locale in its constructor whatever the flags, once per entry read from or written to LMDB
and once per message.

### 2026-11-13: archives without a locale

Boost's binary archives build a `std::locale` in their constructor, flags or not, once per message
and per raft log entry. pg-raft now uses its own archives (`src/base/archive.h`): Boost's structure
layer with the primitive writes done directly, the same bytes as `binary_oarchive` (`no_header`),
log entries decoded in place from LMDB's memory. Locale construction is gone from the profile
(it was 5% of the leader).

| Setup | tps | % of solo | latency avg | `END` (commit) | 10 s intervals |
|---|---|---|---|---|---|
| solo | 55,093 | 100% | 0.288 ms | 0.066 ms | |
| streaming replication | 37,623 | 68% | 0.423 ms | 0.150 ms | |
| pg-raft, through the proxy | **21,716** | 39% | 0.735 ms | 0.329 ms | 21,350–21,940 |

What remains in the profile: about 40% of each node in system calls and thread wake-ups,
`xxhash32` 6% of the leader and 11% of a follower (LMDB's page checksums, plus the frames), `malloc`
and `free` about 5%, and the proxy at 237% CPU.

### 2026-11-13: the proxy's cost, and proxy engines

One cluster, loaded and warmed once, then 60 s runs through each proxy configuration
(`PROXY_ARGS` for `raft/start_proxy.sh`):

| Configuration | tps | latency avg | proxy CPU |
|---|---|---|---|
| direct to the primary (`bench.sh --direct`) | **29,891** | 0.535 ms | |
| proxy, `asio` | 22,800 | 0.702 ms | 180% |
| proxy, `epoll`, 4 threads | 22,756 | 0.703 ms | 190% |
| proxy, `uring`, 4 threads | 22,862 | 0.700 ms | 189% |
| proxy, `uring`, 2 threads | 23,047 | 0.694 ms | 188% |
| proxy, `epoll`, 4 threads, `--spin-us 20` | 23,016 | 0.695 ms | 187% |
| proxy, `uring`, 4 threads, `--spin-us 20` | 23,007 | 0.695 ms | 193% |

The full benchmark without the proxy (fresh cluster, 60 s warm-up, 300 s): **30,139 tps**, 55% of
solo and 80% of streaming replication, commits 0.294 ms. The proxy costs about a quarter of the
throughput on one machine, whatever its engine: its CPU is kernel time (on loopback a send runs
the receiving side's TCP and wakes it), so the extra hop costs the same with epoll, io_uring or
busy-polling.

### 2026-11-13: fewer thread hand-offs on the commit path

A commit went through three avoidable thread wake-ups: the leader posted the AppendEntries send to
an io thread (now the caller of `exec()` sends it: it waits for the commit anyway), every write was
started by a task posted to the connection's strand (now started where it is queued, when nothing
else is in flight), and every message read was handed to its handler by a task posted to the
io_context (now called on the thread that read it).

| pg-raft | tps | % of solo | % of streaming replication | latency avg | `END` (commit) |
|---|---|---|---|---|---|
| direct, before | 30,139 | 55% | 80% | 0.528 ms | 0.294 ms |
| direct (`bench.sh --direct`) | **32,104** | 58% | 85% | 0.496 ms | 0.268 ms |
| every node to libpq (`bench.sh --libpq`) | **31,803** | 58% | 85% | 0.500 ms | 0.271 ms |

`--libpq` is what a client that follows failovers without the proxy does: libpq gets every node
(`host=a,b,c port=...`, `target_session_attrs=read-write`) and connects to the one accepting writes.

### 2026-11-13: 4 KiB pages for the raft log

Profiled with pgbench straight to the primary: appending the entry to the raft log (an LMDB write
transaction) was the largest cost of a commit, 20% of a follower's CPU and 12% of the leader's.
Every commit checksums (xxhash32) and writes each page it dirties, and the pages were 16 KiB for a
~100-byte entry. With 4 KiB pages:

| pg-raft, direct | tps | % of solo | % of streaming replication | latency avg | `END` (commit) |
|---|---|---|---|---|---|
| 16 KiB pages | 32,104 | 58% | 85% | 0.496 ms | 0.268 ms |
| 4 KiB pages | **33,609** | 61% | **89%** | 0.473 ms | 0.235 ms |

(`MDB_WRITEMAP`, which would save the `pwrite` of each dirty page, makes this LMDB, the 1.0
branch, assert in `mdb_freelist_save` on large transactions; not used.)

### 2026-11-13: checksums, and spinning on the commit

Experiments (fresh cluster each, direct, 60 s warm-up, 300 s):

| Run | tps | latency avg | `END` (commit) |
|---|---|---|---|
| baseline | 33,608 | 0.473 ms | 0.236 ms |
| the FUSE thread spins 50 µs on the commit before sleeping | 33,336 | 0.477 ms | 0.232 ms |
| spins 200 µs | 33,308 | 0.477 ms | 0.232 ms |
| no checksums at all (frames and LMDB's pages) | 33,994 | 0.468 ms | 0.226 ms |
| frame checksums off (`frame_checksums = false`, the default now) | 33,814 | 0.470 ms | 0.233 ms |
| + a CRC-32C per log entry, LMDB's page checksums off (the default now) | **33,901** | 0.469 ms | 0.227 ms |
| + a CRC-32C per log entry, and LMDB's page checksums | 33,223 | 0.479 ms | 0.237 ms |

Spinning saves a wake-up but costs more CPU than it gains: not kept. The network frames' xxhash32
is off by default (TCP checksums the segments). The raft log's values carry a CRC-32C of their key
and object, computed while serializing and checked by `RaftLog::verify()` (with the log starting at
its first index and no index missing), in place of LMDB's page checksums.

### 2026-11-17: seven machines, 3 and 5 nodes, 10 to 150 clients

Seven EC2 `m9gd.4xlarge` machines (16 vCPUs, aarch64, 61 GB, local NVMe) in one availability zone:
pgbench on one machine, the pg-raft proxy on another, and the PostgreSQL nodes on up to five
others, one node per machine (`multi/`). PostgreSQL 17.9, `max_connections = 200`. pg-raft was at
`d5e7a0a` (release build, `variant=release`, gcc 14) with no disk syncs on the commit path. Streaming replication used
quorum sync: `ANY 1 (s1, s2)` with 3 nodes and `ANY 2 (s1, s2, s3, s4)` with 5. Solo is one
PostgreSQL on a node machine.

Each run used scale 100 and the TPC-B-like script, with `min(clients, 8)` pgbench threads, a 30 s
warm-up and 120 s measured. Each setup ran on a fresh cluster for 10, 25, 50 and 100 clients, one
point after another on the same tables. A second series ran 125 and 150 clients, again on a fresh
cluster per setup. pg-raft ran each point both through the proxy and directly to the primary.

tps:

| Setup | Nodes | 10 | 25 | 50 | 100 | 125 | 150 |
|---|---|---|---|---|---|---|---|
| solo | 1 | 9,199 | 17,585 | 32,838 | 49,488 | **52,156** | 50,902 |
| streaming replication | 3 | 7,324 | 14,404 | 25,897 | 42,292 | **43,462** | 41,606 |
| streaming replication | 5 | 6,447 | 13,981 | 25,036 | 40,895 | 44,066 | **45,793** |
| pg-raft, direct | 3 | 7,090 | 14,297 | 22,921 | 37,995 | **43,001** | 42,924 |
| pg-raft, direct | 5 | 7,371 | 14,304 | 22,654 | 37,965 | 42,027 | **43,381** |
| pg-raft, through the proxy | 3 | 4,904 | 8,538 | 15,348 | 27,954 | 33,722 | **37,315** |
| pg-raft, through the proxy | 5 | 4,392 | 8,941 | 15,342 | 27,823 | 33,259 | **36,623** |

At 150 clients:

| Setup | Nodes | tps | % of solo | % of streaming replication | latency avg | latency stddev | `END` (commit) |
|---|---|---|---|---|---|---|---|
| solo | 1 | 50,902 | 100% | | 2.941 ms | 1.035 ms | 0.551 ms |
| streaming replication | 3 | 41,606 | 82% | 100% | 3.599 ms | 1.581 ms | 0.909 ms |
| pg-raft, direct | 3 | 42,924 | 84% | **103%** | 3.489 ms | 2.547 ms | 0.950 ms |
| pg-raft, through the proxy | 3 | 37,315 | 73% | 90% | 4.013 ms | 2.585 ms | 0.908 ms |
| streaming replication | 5 | 45,793 | 90% | 100% | 3.270 ms | 1.372 ms | 0.829 ms |
| pg-raft, direct | 5 | 43,381 | 85% | **95%** | 3.452 ms | 2.552 ms | 0.958 ms |
| pg-raft, through the proxy | 5 | 36,623 | 72% | 80% | 4.090 ms | 2.448 ms | 0.937 ms |

- **Most setups stop scaling at about 125 clients.** Solo peaks at about 52k tps. Streaming
  replication and pg-raft direct peak at about 43k with 3 nodes, and 43–46k with 5.
- **At its peak, pg-raft direct matches streaming replication.** With 3 nodes it reaches 99% of
  streaming replication at 125 clients and 103% at 150. With 5 nodes it reaches 95% at both. It
  trails more in the middle of the range, at 88–93% from 50 to 100 clients. On one machine it was at
  89% with 16 clients. But these runs don't match disk syncs: streaming replication fsyncs the WAL
  on the primary and on its standbys, while pg-raft syncs nothing on the commit path. With the syncs
  matched, pg-raft direct reaches 72–90% of streaming replication at 100 clients and above (next
  section).
- **5 nodes cost pg-raft almost nothing.** A commit waits for a majority, 3 of 5, and the followers
  acknowledge in parallel. Its direct throughput is within 2% of 3 nodes' at every point.
- **The proxy's cost shrinks as clients are added.** Through the proxy, pg-raft gets 60–69% of its
  direct throughput at 10 clients, 73–74% at 100 and 84–87% at 150, and it was still rising at 150.
  The extra hop adds latency to every statement but costs little throughput once the primary is
  the limit.
- **pg-raft's latency varies more.** Its stddev is 2–3 times streaming replication's up to 125
  clients, and about 2.5 ms against 1.4–1.6 ms at 150.
- **Some differences at 125 and 150 clients are noise.** Those points ran on fresh tables, while the
  points up to 100 ran one after another on the same tables. That may explain part of the jump from
  100 to 125 clients, and why 5-node streaming replication overtakes 3-node there after trailing it
  up to 100. Treat differences of a few percent between those points as noise.

### 2026-11-17: the same, with the disk syncs matched

The runs above compare unequal durability. Streaming replication and solo run with `fsync = on`: a
commit waits for the WAL fsync on the primary and, for streaming replication, on a quorum of
standbys. pg-raft runs with its commit-path syncs off: a commit is durable once a majority has the
WAL in its page cache. So two more series were run on the same machines, with the same method,
binaries and sweep:

- **pg-raft with every sync on**: `leader_sync`, `follower_sync` and `log_sync` all `true`
  (`RAFT_SYNC=1 ./multi/deploy.sh`, results tagged `sync`). The leader msyncs its WAL (PostgreSQL's
  `pg_wal` and raft's copy) before the commit round. Each follower msyncs the WAL it receives and
  syncs the raft log (LMDB) before acknowledging.
- **Streaming replication and solo with `fsync = off`** (`PG_FSYNC=off ./multi/deploy.sh`, results
  tagged `nosync`), on the primary and the standbys. This also skips the data files' fsyncs at
  checkpoints, but no checkpoint runs in a 2.5-minute run (`checkpoint_timeout = 15min`).

tps, direct to the primary for pg-raft:

| Setup | Disk syncs | 10 | 25 | 50 | 100 | 125 | 150 |
|---|---|---|---|---|---|---|---|
| solo | on | 9,199 | 17,585 | 32,838 | 49,488 | 52,156 | 50,902 |
| solo | off | 9,747 | 22,113 | 33,628 | 52,693 | 55,610 | 57,482 |
| streaming replication, 3 nodes | on | 7,324 | 14,404 | 25,897 | 42,292 | 43,462 | 41,606 |
| streaming replication, 3 nodes | off | 6,322 | 14,764 | 26,187 | 45,041 | 47,711 | 50,611 |
| pg-raft, 3 nodes | on | 5,933 | 11,508 | 19,345 | 30,900 | 34,810 | 34,894 |
| pg-raft, 3 nodes | off | 7,090 | 14,297 | 22,921 | 37,995 | 43,001 | 42,924 |
| streaming replication, 5 nodes | on | 6,447 | 13,981 | 25,036 | 40,895 | 44,066 | 45,793 |
| streaming replication, 5 nodes | off | 7,329 | 14,374 | 26,351 | 42,353 | 46,953 | 49,481 |
| pg-raft, 5 nodes | on | 5,625 | 11,311 | 19,024 | 30,918 | 33,328 | 33,048 |
| pg-raft, 5 nodes | off | 7,371 | 14,304 | 22,654 | 37,965 | 42,027 | 43,381 |

pg-raft as a percentage of streaming replication with the same syncs:

| pg-raft direct vs streaming replication | 10 | 25 | 50 | 100 | 125 | 150 |
|---|---|---|---|---|---|---|
| 3 nodes, syncs on | 81% | 80% | 75% | 73% | 80% | 84% |
| 3 nodes, syncs off | 112% | 97% | 88% | 84% | 90% | 85% |
| 5 nodes, syncs on | 87% | 81% | 76% | 76% | 76% | 72% |
| 5 nodes, syncs off | 101% | 100% | 86% | 90% | 90% | 88% |

Commit (`END`) latency in ms, 3 nodes:

| Setup | Disk syncs | 10 clients | 100 | 150 |
|---|---|---|---|---|
| solo | on / off | 0.174 / 0.139 | 0.340 / 0.251 | 0.551 / 0.354 |
| streaming replication | on / off | 0.388 / 0.387 | 0.603 / 0.510 | 0.909 / 0.657 |
| pg-raft direct | on / off | 0.648 / 0.410 | 1.167 / 0.770 | 1.418 / 0.950 |

Through the proxy, pg-raft with syncs on reaches 30,888 tps with 3 nodes and 29,326 with 5 at 150
clients, against 37,315 and 36,623 with syncs off.

- **Matched, pg-raft direct reaches 73–88% of streaming replication at the peak.** With syncs off,
  pg-raft peaks at 43.0k tps against 50.6k for streaming replication with 3 nodes (85%), and 43.4k
  against 49.5k with 5 (88%); streaming replication was still rising at 150 clients. With syncs on,
  pg-raft peaks at 34.9k against 43.5k with 3 nodes (80%), and 33.3k against 45.8k with 5 (73%).
  At 10–25 clients, with syncs off, the two are level.
- **The syncs cost pg-raft far more than they cost streaming replication.** Turning them on costs
  pg-raft 16–24% at every client count. They cost streaming replication 1–9% at most points (12% with
  5 nodes at 10 clients, 18% with 3 nodes at 150), and solo 2–11% (20% at 25 clients). At 10 clients the commit
  takes 0.24 ms longer in pg-raft with syncs on, and no longer at all in streaming replication.
  Some points are noisy: 3-node streaming replication at 10 clients was faster with fsync on.
- **The syncs are expensive because of how pg-raft makes them, not because of the disk** (below):
  the WAL is written through mmaps, and an msync of pages dirtied through a mapping costs 60–70 µs
  in the node, against 17–47 µs for a `pwrite` and `fdatasync` of the same bytes. Each round makes
  three syncs on the leader (two msyncs, then the raft log's `fdatasync`), and two on each
  follower, one after the other and with the raft lock held.

#### Where the sync time goes

Raw costs on these machines (`m9gd.4xlarge`, xfs on the instance's NVMe):

| Operation | Latency |
|---|---|
| TCP round trip between two node machines, 64 B–8 KB request | 107–136 µs |
| `pwrite` + `fdatasync`, 1–64 KB | 17–47 µs |
| `pwrite` + `msync` of the range | 17–56 µs |
| a write through an mmap + `msync` (or `fdatasync`, or `sync_file_range`), in a tight loop | 1.0–1.8 ms |

Profile of pg-raft with syncs on, 3 nodes, 100 clients, direct (`perf trace -s` for 20 s and
`perf record` for 15 s, about 29.5k tps):

- About 1,400 raft rounds a second, about 21 commits each. Per round, the leader makes 2 msyncs
  (62 µs average) and 1 `fdatasync` (40 µs), and each follower 1 msync (66–71 µs) and 1
  `fdatasync` (30 µs).
- The follower's largest cost is the msync in `RaftServer::write_blocks`: `ptep_clear_flush` (12%
  of its CPU) under `msync` → `xfs_file_fsync` → `iomap_writepages` → `folio_mkclean`. Each sync
  write-protects the mapped pages and flushes the TLB, and the next write to them faults (another
  ~7%: `__ptep_set_access_flags`, `contpte_convert`).
- The commit path, then: the leader's two msyncs (~125 µs) before the round starts, a network round
  trip (~110 µs), and the follower's msync and `fdatasync` (~100 µs). That accounts for the
  0.24 ms the syncs add per commit.

So writing the raft log and the WAL to one file with `pwrite` and syncing it once with
`fdatasync`, and nothing written through an mmap, would replace three syncs on the leader and two
on each follower with one each.

#### Does PostgreSQL write and fsync the WAL concurrently?

`src/wal_fuse//wal_fuse_concurrency` runs one PostgreSQL with `pg_wal` on `WalFuseOps` (no raft) and
pgbench, 50 clients, 8 threads, scale 100, 60 s, and counts the WAL writes and fsyncs in flight when
each enters. The node's FUSE loop is single-threaded (`fuse_loop`), so nothing can overlap there;
`--mt` runs `fuse_loop_mt` to show what PostgreSQL issues. `--fsync-delay-us 300` completes each
fsync 300 µs later, standing in for a raft round.

| FUSE loop | fsync completes | tps | commits per fsync | avg write | max WAL calls in flight |
|---|---|---|---|---|---|
| single-threaded | at once (msync) | 55,488 | 5.8 | 10.7 KB | 1 |
| multithreaded | at once | 55,005 | 5.9 | 10.7 KB | 1 |
| single-threaded | after 300 µs | 43,024 | 17.3 | 15.8 KB | 1 |
| multithreaded | after 300 µs | 42,673 | 17.5 | 15.9 KB | 1 |

No WAL write or fsync ever entered while another was in flight, even with the multithreaded loop
(which did run more threads). Nearly all of them come from backends at `COMMIT`, a write and then an
fsync, under PostgreSQL's `WALWriteLock`; the other backends wait and find their commit flushed
(group commit). A slower fsync makes larger batches, not concurrent ones. So the WAL reaches
pg-raft as one stream, one batch per fsync, and a multithreaded FUSE loop gains nothing. What
matters is the latency of each fsync round: a 300 µs fsync alone caps one PostgreSQL here at
about 43k tps.

### 2026-11-18: one large machine, syncs on and off

One EC2 machine, 96 cores (aarch64), ~740 GB, the data on its local NVMe instance store (write-through:
no volatile cache, so `fdatasync` sends no cache flush), Amazon Linux 2023. PostgreSQL 17.9, pg-raft at
`9bfe187` (the ledger), release build, gcc 14. Everything local: 3 nodes, the proxy, pgbench.
`./matrix.sh`: scale 100, TPC-B-like, 16 pgbench threads, 30 s warm-up, 120 s measured, a fresh cluster
per setup and syncs setting.

tps:

| Setup | syncs on, 50 | syncs on, 100 | syncs off, 50 | syncs off, 100 |
|---|---|---|---|---|
| solo | 228,099 | 234,826 | 271,694 | 253,796 |
| streaming replication, 3 nodes | 175,530 | 190,420 | 237,185 | 224,341 |
| pg-raft direct | 122,075 | 128,850 | 143,499 | 145,201 |
| pg-raft through the proxy | 86,893 | 99,540 | 96,809 | 113,658 |

pg-raft direct against streaming replication, same syncs: on 70% / 68%, off 61% / 65%. Commit (`END`)
in ms, 50 / 100 clients: solo 0.063 / 0.118 (off 0.025 / 0.068), streaming replication 0.119 / 0.188
(off 0.053 / 0.121), pg-raft direct 0.216 / 0.352 (off 0.170 / 0.299). pg-raft's latency stddev is
1.3–2.2 ms against 0.07–0.6 ms.

- **pg-raft peaks at about 145k tps**: with syncs off it gains 1% from 50 to 100 clients.
- **The syncs cost pg-raft less than they cost streaming replication** here: 15% / 11% against
  26% / 15% (solo 16% / 7%): the ledger's `pwrite` + `fdatasync`, not the msyncs of the earlier runs.
- **The proxy costs 22–33%**, less with more clients.

Experiments on pg-raft (syncs on, 50 clients, direct; steady 10 s intervals 126k–134k throughout):

| Change | tps | `END` |
|---|---|---|
| baseline | 122,075 | 0.216 ms |
| a follower writes its records with `pwritev2(RWF_DSYNC)` instead of `pwrite` + `fdatasync` | 123,286 | 0.214 ms |
| + send to both followers in parallel (one inline, one posted) — reverted | 123,681 | 0.212 ms |
| + exact offsets of the records after the last sparse point (branch `ledger-recent-offsets`) | 123,384 | 0.213 ms |

None moves throughput. What the measurements show:

- **A follower almost never has more than one AppendEntries waiting** (its raft sockets, sampled:
  nothing queued 91% of the time). PostgreSQL's WAL arrives one flush at a time, so grouping the
  followers' syncs can't help. A follower's `RWF_DSYNC` write takes ~30 µs: the disk write.
- **Thread wake-ups are cheap**: `perf sched timehist` shows a scheduling delay of ~1 µs (p99 2–3 µs)
  for every thread. The I/O threads wake far more often than needed (~8 per round on the leader, a few
  µs each), but that is CPU, not commit latency; simplifying `net` (its queues) wouldn't shorten a round.
- **The leader's FUSE thread** runs ~33 µs of CPU per round (37–49% busy; ~10k rounds/s, ~12 commits
  each). With frame pointers (`-fno-omit-frame-pointer -mno-omit-leaf-frame-pointer`, `perf record -g`;
  DWARF unwinding fails through static glibc and libfuse): `op_fsync` 58% — sending the AppendEntries
  45% (`Ledger::records` 19%, of it `Ledger::seek` parsing from the sparse point 12.6%; the socket write
  13.5%; serialization and queueing 11%), the ledger append 10%; `op_write` 16%; reading `/dev/fuse` 11%,
  replying 7%; `malloc`/`free` ~10% overall.
- **It waits for PostgreSQL a quarter of the time**: its waits are 2–10 µs (between a write and its
  fsync), 30–50 µs (the commit: a follower's sync and the round trip), and >150 µs gaps averaging
  ~800 µs (26% of the time) in which pg-raft has nothing to do. So cutting the FUSE thread's CPU doesn't
  raise tps; what PostgreSQL's backends do in those gaps (group commit, `WALWriteLock`) is the next
  thing to measure.

### 2026-11-18: three availability zones

Four EC2 `i4i.8xlarge` machines (32 vCPUs, x86_64, 256 GB, local NVMe), Amazon Linux 2023,
PostgreSQL 17.9: the three nodes in `us-west-2b`, `2c` and `2d`, pgbench on a fourth machine in `2a`
(`multi/` with three addresses in `machines.txt`: 3-node clusters, the proxy on the client). Round
trips: client to the primary ~0.49 ms, primary to the standbys ~0.51–0.53 ms (TCP). Scale 100,
TPC-B-like, pgbench threads `min(clients, 30)`, 30 s warm-up, 120 s measured, a fresh cluster per setup
and syncs setting, pg-raft direct to the primary (no proxy). Every transaction crosses AZs 7 times, so
throughput is far below the one-AZ runs and still rises linearly at 100 clients.

**A stall at every WAL segment switch.** The first runs (`7bc1f89`, 50 clients) had pg-raft at 88–89%
of streaming replication with a latency stddev of ~4 ms against ~1 ms. pgbench's per-transaction log
showed it: 99% of transactions under 8.5 ms, and every 0.8–3.5 s all 50 clients stalled together for
83–93 ms, each stall ending within 3 ms of the leader opening a new WAL segment. With
`wal_recycle = off` (which pg-raft forces) every segment switch creates a file, and with
`wal_init_zero = on` (PostgreSQL's default) it is zero-filled, 16 MiB through FUSE (~2,000 writes),
under `WALWriteLock`. pg-raft now forces `wal_init_zero = off` (`a89dffa`):

| pg-raft, syncs off, 50 clients | `wal_init_zero = on` | `off` |
|---|---|---|
| tps | 9,163 | 9,608 |
| latency stddev | 4.706 ms | 0.814 ms |
| p99 / p99.9 | 8.5 / 88.3 ms | 8.2 / 11.2 ms |
| transactions over 20 ms | 1,758 | 7 |

Running past 4 GiB of WAL for the first time also stopped the standbys at `0/FFFFF970`: the WAL sender
named segments from the segment number's high and low 32 bits (`60cf5e4`). Since then each setup has
the WAL settings that suit it (`9355077`, `e731d97`): `wal_sync_method = fdatasync` for all three
(for pg-raft `fdatasync` reaches FUSE as an fsync with `datasync` set), `wal_recycle` and
`wal_init_zero` `off` for pg-raft and PostgreSQL's defaults (`on`) for std and solo.

**Where a commit's time goes.** PostgreSQL holds `WALWriteLock` across the WAL fsync, so its flushes
are serial (see "Does PostgreSQL write and fsync the WAL concurrently?" above), and in pg-raft each
fsync is a raft round: across AZs, one flush per round trip, ~1,500 a second, ~6 commits each.
Streaming replication releases the lock after the local flush and waits for the standby in
`SyncRepWaitForLSN`, so its rounds overlap. That is the price of pg-raft's commit inside the fsync:
nothing is visible before a quorum has it, where a cancelled wait in synchronous replication makes a
commit visible locally first. One round on the leader, timed with bpftrace (syncs off, 50 clients):

| Step of a round | `e731d97` | `c24deef` |
|---|---|---|
| `op_fsync` → AppendEntries into `tcp_sendmsg` | 28.8 µs | 17.1 µs |
| → the first reply in the leader's TCP stack (network ~475 µs + the follower's ~33 µs) | 508 µs | 493 µs |
| reply → `handle_response` | 33.4 µs | 14.3 µs |
| `handle_response` → `op_fsync` returns | 22.9 µs | 21.8 µs |

`9650577`: the leader sends its latest records from the ledger's window (`append()` keeps their
bytes) instead of seeking and reading the file. `c24deef`: a thread per follower reads its replies,
busy-polling the socket from just before one is due; sends go to the nearest follower first (the
first reply decides the commit: 2c answered first 97% of the time, 2b's replies came ~28 µs later,
~18 µs of it from being sent second). Turning off the NIC's receive interrupt moderation
(`ethtool -C ens5 adaptive-rx off rx-usecs 0`, a machine setting, left at the default) gained 1.3%.

**25 to 100 clients** (`c5d890c`), tps and `END` (the commit):

| Setup | Syncs | 25 | 50 | 75 | 100 |
|---|---|---|---|---|---|
| solo | on | 6,450 | 12,033 | 17,391 | 22,154 |
| streaming replication | on | 5,333 (1.183 ms) | 10,206 (1.199 ms) | 14,644 (1.225 ms) | 18,684 (1.246 ms) |
| pg-raft | on | 5,037 (1.505 ms) | 9,672 (1.523 ms) | 13,453 (1.588 ms) | 16,980 (1.617 ms) |
| solo | off | 6,567 | 12,307 | 17,820 | 22,863 |
| streaming replication | off | 5,590 (1.053 ms) | 10,743 (1.066 ms) | 15,437 (1.082 ms) | 19,640 (1.097 ms) |
| pg-raft | off | 5,231 (1.400 ms) | 9,639 (1.478 ms) | 13,751 (1.513 ms) | 17,272 (1.561 ms) |

pg-raft against streaming replication with the same syncs:

| | 25 | 50 | 75 | 100 |
|---|---|---|---|---|
| syncs on | 94% | 95% | 92% | 91% |
| syncs off | 94% | 90% | 89% | 88% |

- **Syncs cost pg-raft 0–4% and streaming replication ~5%**: pg-raft's leader syncs its ledger during
  the round and a follower writes with `RWF_DSYNC` (~30 µs on this NVMe); streaming replication
  fdatasyncs on the primary before the standby's round. Solo: 2–3%.
- **The gap grows with the clients**: pg-raft's commit takes 0.11–0.16 ms longer from 25 to 100
  clients, streaming replication's 0.04–0.06 ms. A commit waits for the round in flight, then its
  own, and the rounds carry more as the clients grow.
- **Latency stddev is now like streaming replication's**: 0.6–1.8 ms against 0.5–1.4 ms.

**Without spinning** (`360da07`): both ends of each leader-to-follower connection read on a thread of
their own (the follower's requests too), sleeping until data arrives instead of spinning, and the
node runs 2 io threads instead of 8. tps against `c5d890c`:

| Syncs | 25 | 50 | 75 | 100 |
|---|---|---|---|---|
| on | 4,976 (−1.2%) | 9,666 (−0.1%) | 13,382 (−0.5%) | 16,794 (−1.1%) |
| off | 5,171 (−1.1%) | 9,648 (+0.1%) | 13,721 (−0.2%) | 16,825 (−2.6%) |

At 100 clients, syncs off, two runs each with `io_threads` 8 and 2: 17,370 and 17,018 against
16,985 and 17,001 tps (the two runs of the same setting differ by up to 2%). The simpler code costs
nothing measurable, and 2 io threads are enough.

### 2026-11-18: more clients, the commit in synchronous replication, scale 1000

Same machines and method as above: three nodes in three AZs, pgbench in a fourth, threads
`min(clients, 30)`, 30 s warm-up, 120 s measured, a fresh cluster per setup and syncs setting
(the client counts of a sweep one after another on the same tables), pg-raft direct to the
primary. `max_connections = 400` for the runs above 100 clients.

**150 to 300 clients, scale 100** (`ac34721`), tps and `END`:

| Setup | Syncs | 150 | 200 | 300 |
|---|---|---|---|---|
| solo | on | 31,107 | 38,057 | 44,362 |
| streaming replication | on | 25,607 (1.264 ms) | 30,107 (1.306 ms) | 35,137 (1.372 ms) |
| pg-raft | on | 22,389 (87%, 1.694 ms) | 26,205 (87%, 1.738 ms) | 29,199 (83%, 1.857 ms) |
| solo | off | 32,300 | 39,686 | 47,715 |
| streaming replication | off | 26,334 (1.158 ms) | 31,713 (1.176 ms) | 37,188 (1.240 ms) |
| pg-raft | off | 22,461 (85%, 1.664 ms) | 26,095 (82%, 1.729 ms) | 29,433 (79%, 1.832 ms) |

Profiled at 300 clients (syncs off): the leader's node used about 0.4 of a core; its FUSE thread,
the busiest at 18%, spent ~92% of its time waiting inside `op_fsync` for the round (~1,450 flushes
a second, ~20 commits each). The leader machine was half busy with PostgreSQL's backends; the
client 91% idle. Of the 300 backends, on average 117 waited for another transaction's row lock
(`Lock:transactionid`) and 23 for a tuple lock, 105 for their client, 40 for `WALWrite` (queued
behind the flush in flight), and 13 ran. TPC-B at scale 100 updates one of 100 branch rows in
every transaction and holds its lock until the commit returns, so a longer commit queues more
transactions on the hot rows: the widening gap at high client counts is largely that. Run-queue
latency (`runqlat`): on the follower 99% of wake-ups within 3 µs; on the leader 74% within 1 µs
but ~12% after 8–127 µs (node threads behind backends). No cpuidle driver is active on these
instances (`current_driver: none`).

**The commit in PostgreSQL's synchronous replication** (`commit_wait = "sync_rep"`, `fb98218`, not
the default). The serial rounds come from PostgreSQL holding `WALWriteLock` across the WAL fsync,
which in pg-raft is a raft round. With `commit_wait = "sync_rep"` the FUSE fsync returns once the
flush is in the leader's ledger, and the commit waits in `SyncRepWaitForLSN` instead, outside the
lock: the node is its PostgreSQL's synchronous standby (`SyncAcker`, `application_name` `pg_raft`),
reporting the raft-committed WAL end as its flush position, and the rounds of successive flushes
overlap. Stock PostgreSQL, but with its trade-off: a commit whose wait is cancelled
(`pg_cancel_backend`, `statement_timeout`) or terminated is visible before a quorum has it. What
PostgreSQL would need to change to remove that is in `docs/todo.md`. Scale 100, tps (% of
streaming replication with the same syncs) and `END`:

| Syncs | Clients | streaming replication | pg-raft, `fsync` | pg-raft, `sync_rep` |
|---|---|---|---|---|
| on | 25 | 5,333 (1.183 ms) | 4,976 (93%, 1.457 ms) | **5,480 (103%, 1.165 ms)** |
| on | 50 | 10,206 (1.199 ms) | 9,666 (95%, 1.479 ms) | **10,398 (102%, 1.182 ms)** |
| on | 100 | 18,684 (1.246 ms) | 16,794 (90%, 1.625 ms) | **18,538 (99%, 1.267 ms)** |
| on | 150 | 25,607 (1.264 ms) | 22,389 (87%, 1.694 ms) | **25,045 (98%, 1.313 ms)** |
| on | 200 | 30,107 (1.306 ms) | 26,205 (87%, 1.738 ms) | **29,385 (98%, 1.368 ms)** |
| on | 300 | 35,137 (1.372 ms) | 29,199 (83%, 1.857 ms) | **33,651 (96%, 1.494 ms)** |
| off | 25 | 5,590 (1.053 ms) | 5,171 (93%, 1.405 ms) | **5,499 (98%, 1.110 ms)** |
| off | 50 | 10,743 (1.066 ms) | 9,648 (90%, 1.484 ms) | **10,497 (98%, 1.140 ms)** |
| off | 100 | 19,640 (1.097 ms) | 16,825 (86%, 1.601 ms) | **18,766 (96%, 1.205 ms)** |
| off | 150 | 26,334 (1.158 ms) | 22,461 (85%, 1.664 ms) | **25,491 (97%, 1.250 ms)** |
| off | 200 | 31,713 (1.176 ms) | 26,095 (82%, 1.729 ms) | **30,211 (95%, 1.299 ms)** |
| off | 300 | 37,188 (1.240 ms) | 29,433 (79%, 1.832 ms) | **34,443 (93%, 1.415 ms)** |

(The `fsync` column: `360da07` up to 100 clients, `ac34721` above, the same code.) With the rounds
overlapping, pg-raft reaches 96–103% of streaming replication with the syncs on and 93–98% with
them off, 6–17% more than with the commit in the fsync, gaining more as the clients grow; its
commit grows with the load as streaming replication's does. With the syncs on it is level with or
ahead of streaming replication, whose primary flushes locally before the standby's round, while
pg-raft's leader syncs its ledger during the round.

**Scale 1000** (`5d20443`, 1,000 branch rows, the load generated on the server with
`PGBENCH_INIT_ARGS="-I dtGvp"`), tps (% of streaming replication) and `END`:

| Syncs | Clients | solo | streaming replication | pg-raft, `fsync` | pg-raft, `sync_rep` |
|---|---|---|---|---|---|
| on | 150 | 33,762 (0.757 ms) | 27,321 (1.722 ms) | 20,440 (75%, 3.442 ms) | 22,762 (83%, 2.757 ms) |
| on | 200 | 43,394 (0.803 ms) | 34,719 (1.889 ms) | 25,073 (72%, 3.745 ms) | 28,578 (82%, 2.949 ms) |
| on | 300 | 50,484 (1.264 ms) | 40,907 (2.639 ms) | 29,338 (72%, 5.008 ms) | 31,158 (76%, 4.522 ms) |
| off | 150 | 35,414 (0.583 ms) | 30,264 (1.209 ms) | 23,844 (79%, 2.496 ms) | 27,070 (89%, 1.785 ms) |
| off | 200 | 45,551 (0.613 ms) | 39,206 (1.269 ms) | 29,189 (74%, 2.806 ms) | 33,656 (86%, 2.012 ms) |
| off | 300 | 53,068 (0.924 ms) | 47,209 (1.728 ms) | 33,298 (71%, 3.956 ms) | 36,103 (76%, 3.353 ms) |

- **Less contention, more throughput for solo and streaming replication** (7–27% more than at
  scale 100), **but not for pg-raft**: with the syncs on it does 3–9% less at 150–200 clients
  (both commit modes), with them off 5–13% more.
- **But pg-raft falls further behind**: 71–79% of streaming replication with the commit in the
  fsync, 76–89% in synchronous replication (93–103% at scale 100). Every commit is longer than at
  scale 100, pg-raft's most.
- **Streaming replication's own syncs cost more**: its commit takes 1.72 ms with them on against
  1.21 ms off at 150 clients (10% at scale 100). Both point to much more WAL per commit at scale
  1000 (the accounts table, ~13 GB, is far larger than `shared_buffers`, so after a checkpoint
  most updates write a full page image), which every round of pg-raft carries and copies. Measured
  in the next section.

### 2026-11-19: full page images, `max_wal_size` and WAL compression, scale 1000

Same machines and method, PostgreSQL 17.9, scale 1000 (`-I dtGvp`), 300 clients, syncs on (pg-raft
with `RAFT_SYNC=1`: `leader_sync` and `follower_sync`), and for every setup `max_wal_size = 64GB`
(16GB above) and `max_connections = 400`; the code of `54f7aaa`. Each setting varies
`wal_compression` and `full_page_writes` on all three setups. tps (% of streaming replication with
the same setting) and `END`; where a row was run twice (`lz4`, `zstd`), both runs:

| WAL setting | solo | streaming replication | pg-raft, `fsync` | pg-raft, `sync_rep` |
|---|---|---|---|---|
| `wal_compression = off` | 54,514 (1.111 ms) | 47,120 (1.981 ms) | 38,398 (81%, 3.325 ms) | 37,944 (81%, 3.216 ms) |
| `lz4` | 55,739 (1.047 ms) | 49,278 (1.784 ms) | 46,020 / 46,200 (94%, 2.330 ms) | 47,298 / 47,143 (96%, 1.942 ms) |
| `zstd` | 54,810 (1.066 ms) | 48,949 (1.773 ms) | 46,379 / 46,021 (94%, 2.298 ms) | 44,503 / 44,383 (91%, 2.213 ms) |
| `pglz` | | | 45,762 (2.327 ms) | 46,218 (1.999 ms) |
| `off`, `full_page_writes = off` | 54,756 (1.080 ms) | 49,166 (1.787 ms) | 46,669 (95%, 2.287 ms) | 45,379 (92%, 2.168 ms) |

- **`max_wal_size = 64GB` alone** (compression off, against the 300-client syncs-on row above):
  solo +8%, streaming replication +15%, pg-raft +31% (`fsync`) and +22% (`sync_rep`), from 72% to
  81% of streaming replication. Checkpoints come less often, so fewer updates write a full page
  image.
- **The full page images are pg-raft's gap.** With any compressor, or without the images, pg-raft
  is at 91–96% of streaming replication, as at scale 100, against 81% with the images
  uncompressed. lz4, zstd and pglz all compress them enough (45.8–46.4k with the commit in the
  fsync); solo and streaming replication gain at most 5% from them, since only pg-raft copies the
  WAL into its ledger and sends it in the commit's round.
- **`sync_rep` no longer gains much**: +2.5% with lz4, −1% with compression off, −3% to −4% with
  zstd and without full page images (where at scale 100 it gained 15% at 300 clients), and its
  latency stddev is 4.6–12.5 ms against 1.4–4.1 ms with the commit in the fsync. Not yet
  understood.
- **The repeated rows agree within 0.8%** (zstd with `fsync`) and less, so differences of a few
  percent are real. Solo varies by 2% across all the settings.

### 2026-11-23: PostgreSQL 18.7, WAL capture by hooks, the commit in synchronous replication; scale 100 and 1000

**Machines.** Four EC2 `i4i.8xlarge` (32 vCPUs, x86_64, 247 GiB, local NVMe instance storage for
`/data`), Amazon Linux 2023: the three nodes in `us-west-2c` (node 0, 172.31.1.229), `2a` (node 1)
and `2b` (node 2), pgbench and the proxy on a fourth machine in `2d` (the AZs differ from
2026-11-18's). Client to primary round trip ~0.58 ms (ping). Node 0 was the primary in every run.

**Software.** pg-raft binaries built from `667816b` (release, gcc 14); bench scripts `2eb97c8`,
then `7d1fbef` (lz4 default) and `c567793` (`MAX_WAL_SIZE`). PostgreSQL 18.7 everywhere: the stock
build (`/opt/pg_raft/postgresql/18`) for solo, std and pg-raft with FUSE; pg-raft's PostgreSQL
(18.7 with the hooks patch, `pg_raft_wal` preloaded) for pg-raft with hooks. Both built with lz4.

**Method.** pgbench TPC-B-like (simple query mode), threads `min(clients, 30)`, 30 s warm-up (not
measured), 120 s measured; a fresh cluster per setup, the data loaded once, then the client counts
in ascending order on the same tables; pg-raft direct to the primary (no proxy). One run per point
(the repeats are below). Settings in every setup: `shared_buffers = 4GB`, `max_connections = 400`,
`checkpoint_timeout = 15min`, `wal_sync_method = fdatasync`, `full_page_writes = on`,
`wal_buffers = 16MB`, data checksums on, `synchronous_commit = on`. solo and std keep
`wal_recycle` and `wal_init_zero` on (PostgreSQL's defaults); pg-raft forces both off.

**Setups.**
- **solo**: one PostgreSQL on node 0's machine, no HA (the ceiling).
- **std**: PostgreSQL quorum synchronous replication, a primary and two standbys,
  `synchronous_standby_names = 'ANY 1 (s1, s2)'` (2 of 3, as raft's majority).
- **pg-raft**, 3 nodes, along two axes:
  - WAL capture (`wal_capture`): **FUSE** (`pg_wal` mounted over FUSE) or **hook** (pg-raft's
    PostgreSQL sends each WAL write and flush to the node over a Unix socket).
  - Where the commit waits (`commit_wait`): **`fsync`** (inside PostgreSQL's WAL fsync, under
    `WALWriteLock`, so rounds are serial) or **`sync_rep`** (in PostgreSQL's synchronous
    replication wait, outside the lock, so rounds overlap). With FUSE, `sync_rep` makes the node its
    PostgreSQL's synchronous standby, and a cancelled or terminated wait makes the commit visible
    before a quorum has it. With hooks, `sync_rep_wait_hook` waits for the raft commit and isn't
    interrupted (a failure PANICs), so hook + `sync_rep` keeps `fsync`'s guarantee.
- **Syncs on / off**: for solo and std, PostgreSQL's `fsync = on` / `off` (`PG_FSYNC=off`). For
  pg-raft, the raft ledger's commit-path syncs `leader_sync` and `follower_sync` `true` / `false`
  (`RAFT_SYNC=1`); pg-raft's data files and its `wal_dir` checkpoint are synced either way. Not the
  same switch: std with `fsync = off` gives up more.

**Columns.** tps (pgbench, without the initial connection time); latency average and stddev
(pgbench, per transaction); `END`: pgbench's per-statement latency of the commit (`END;`).
Percentages are of std with the same syncs, at the same client count.

**Raw data** (on the client, outside the repository): `~/shared/pg_raft/campaign/` holds the driver
scripts (`campaign*.sh`), their logs, `state.txt` and the summaries (`summary.md`: scale 100;
`summary-lz4.md`; `summary-s1000.md`; `summary-rep.md` and `rep-aggregate.md`: the repeats). Per-run
directories (`settings.txt` with the settings in effect, `run.log`, `warmup.log`, `summary.txt`) in
`~/shared/pg_raft/results/` (scale 100), `lz4-results/`, `s1000-results/` and `rep-results/`, named
`<setup>-<variant>-n<nodes>-c<clients>-<time>`.

#### Scale 100, `wal_compression = off`, 25 to 300 clients

`max_wal_size = 16GB`. tps (% of std):

**Syncs on**

| Setup | 25 | 50 | 100 | 150 | 200 | 300 |
|---|---|---|---|---|---|---|
| solo | 6,731 (111%) | 12,772 (111%) | 23,539 (113%) | 32,639 (115%) | 39,503 (117%) | 45,730 (120%) |
| std, quorum sync rep | 6,080 | 11,537 | 20,902 | 28,358 | 33,708 | 37,997 |
| pg-raft FUSE, `fsync` | 5,784 (95%) | 10,942 (95%) | 19,037 (91%) | 25,043 (88%) | 29,174 (87%) | 31,903 (84%) |
| pg-raft FUSE, `sync_rep` | 6,054 (100%) | 11,473 (99%) | 20,637 (99%) | 27,643 (97%) | 32,584 (97%) | 36,064 (95%) |
| pg-raft hook, `fsync` | 5,792 (95%) | 10,942 (95%) | 19,439 (93%) | 25,431 (90%) | 29,575 (88%) | 32,455 (85%) |
| pg-raft hook, `sync_rep` | 6,037 (99%) | 11,438 (99%) | 20,477 (98%) | 27,434 (97%) | 32,218 (96%) | 35,599 (94%) |

**Syncs off**

| Setup | 25 | 50 | 100 | 150 | 200 | 300 |
|---|---|---|---|---|---|---|
| solo | 6,854 (110%) | 13,038 (111%) | 24,249 (112%) | 34,042 (115%) | 41,830 (117%) | 49,143 (120%) |
| std, quorum sync rep | 6,218 | 11,772 | 21,577 | 29,620 | 35,623 | 40,993 |
| pg-raft FUSE, `fsync` | 5,903 (95%) | 11,106 (94%) | 19,933 (92%) | 26,681 (90%) | 31,345 (88%) | 34,388 (84%) |
| pg-raft FUSE, `sync_rep` | 6,145 (99%) | 11,580 (98%) | 20,948 (97%) | 28,287 (95%) | 33,382 (94%) | 37,486 (91%) |
| pg-raft hook, `fsync` | 5,914 (95%) | 11,134 (95%) | 19,917 (92%) | 26,719 (90%) | 31,341 (88%) | 34,924 (85%) |
| pg-raft hook, `sync_rep` | 6,132 (99%) | 11,541 (98%) | 20,867 (97%) | 28,119 (95%) | 33,140 (93%) | 37,305 (91%) |

- **pg-raft with `sync_rep` is within 1–6% of std with the syncs on** (98–100% at 25–100 clients,
  94–95% at 300), against 84–95% with the commit in the fsync. `sync_rep` gains 4–13% over `fsync`
  with the syncs on (4–9% off), more as the clients grow.
- **Hook and FUSE perform the same**: within about 2% at every point. FUSE isn't the bottleneck at this
  scale; the hooks' advantage is correctness (`sync_rep` without the early-visibility caveat).
- **Syncs off helps std more than pg-raft**: std +8% at 300 clients, pg-raft hook + `sync_rep`
  +5%, so the gap to std is wider with them off (91% at 300).
- **The gap grows with the clients**: TPC-B at scale 100 updates one of 100 branch rows per
  transaction, so a longer commit queues more transactions on the hot rows (2026-11-18). `END` at
  300 clients: std 1.230 ms, hook + `sync_rep` 1.380 ms, hook + `fsync` 1.601 ms.
- **The cost of HA itself**: std is at 83–90% of solo.

<details><summary>Scale 100: tps, latency and <code>END</code> for every point</summary>

| Setup | Clients | tps | latency avg (ms) | stddev (ms) | `END` (ms) |
|---|---|---|---|---|---|
| solo, syncs on | 25 | 6,731 | 3.714 | 0.346 | 0.556 |
| solo, syncs on | 50 | 12,772 | 3.914 | 0.446 | 0.581 |
| solo, syncs on | 100 | 23,539 | 4.246 | 0.731 | 0.622 |
| solo, syncs on | 150 | 32,639 | 4.592 | 1.075 | 0.655 |
| solo, syncs on | 200 | 39,503 | 5.056 | 1.611 | 0.704 |
| solo, syncs on | 300 | 45,730 | 6.549 | 3.282 | 0.828 |
| std, quorum sync rep, syncs on | 25 | 6,080 | 4.112 | 0.691 | 0.897 |
| std, quorum sync rep, syncs on | 50 | 11,537 | 4.333 | 0.758 | 0.929 |
| std, quorum sync rep, syncs on | 100 | 20,902 | 4.782 | 1.220 | 0.985 |
| std, quorum sync rep, syncs on | 150 | 28,358 | 5.284 | 1.751 | 1.032 |
| std, quorum sync rep, syncs on | 200 | 33,708 | 5.927 | 2.457 | 1.079 |
| std, quorum sync rep, syncs on | 300 | 37,997 | 7.882 | 5.086 | 1.230 |
| pg-raft FUSE, `fsync`, syncs on | 25 | 5,784 | 4.322 | 0.497 | 1.121 |
| pg-raft FUSE, `fsync`, syncs on | 50 | 10,942 | 4.568 | 0.621 | 1.156 |
| pg-raft FUSE, `fsync`, syncs on | 100 | 19,037 | 5.250 | 1.291 | 1.313 |
| pg-raft FUSE, `fsync`, syncs on | 150 | 25,043 | 5.985 | 2.093 | 1.397 |
| pg-raft FUSE, `fsync`, syncs on | 200 | 29,174 | 6.848 | 3.249 | 1.454 |
| pg-raft FUSE, `fsync`, syncs on | 300 | 31,903 | 9.391 | 7.162 | 1.650 |
| pg-raft FUSE, `sync_rep`, syncs on | 25 | 6,054 | 4.129 | 1.100 | 0.918 |
| pg-raft FUSE, `sync_rep`, syncs on | 50 | 11,473 | 4.357 | 0.496 | 0.938 |
| pg-raft FUSE, `sync_rep`, syncs on | 100 | 20,637 | 4.843 | 2.196 | 1.024 |
| pg-raft FUSE, `sync_rep`, syncs on | 150 | 27,643 | 5.422 | 3.551 | 1.103 |
| pg-raft FUSE, `sync_rep`, syncs on | 200 | 32,584 | 6.132 | 4.409 | 1.164 |
| pg-raft FUSE, `sync_rep`, syncs on | 300 | 36,064 | 8.307 | 8.199 | 1.350 |
| pg-raft hook, `fsync`, syncs on | 25 | 5,792 | 4.316 | 0.491 | 1.110 |
| pg-raft hook, `fsync`, syncs on | 50 | 10,942 | 4.568 | 0.582 | 1.148 |
| pg-raft hook, `fsync`, syncs on | 100 | 19,439 | 5.141 | 1.204 | 1.244 |
| pg-raft hook, `fsync`, syncs on | 150 | 25,431 | 5.893 | 1.984 | 1.358 |
| pg-raft hook, `fsync`, syncs on | 200 | 29,575 | 6.756 | 3.147 | 1.421 |
| pg-raft hook, `fsync`, syncs on | 300 | 32,455 | 9.231 | 7.113 | 1.601 |
| pg-raft hook, `sync_rep`, syncs on | 25 | 6,037 | 4.141 | 1.134 | 0.932 |
| pg-raft hook, `sync_rep`, syncs on | 50 | 11,438 | 4.370 | 0.502 | 0.961 |
| pg-raft hook, `sync_rep`, syncs on | 100 | 20,477 | 4.881 | 2.155 | 1.050 |
| pg-raft hook, `sync_rep`, syncs on | 150 | 27,434 | 5.464 | 3.606 | 1.124 |
| pg-raft hook, `sync_rep`, syncs on | 200 | 32,218 | 6.201 | 4.335 | 1.190 |
| pg-raft hook, `sync_rep`, syncs on | 300 | 35,599 | 8.415 | 8.348 | 1.380 |
| solo, syncs off | 25 | 6,854 | 3.647 | 0.204 | 0.500 |
| solo, syncs off | 50 | 13,038 | 3.834 | 0.321 | 0.517 |
| solo, syncs off | 100 | 24,249 | 4.121 | 0.547 | 0.537 |
| solo, syncs off | 150 | 34,042 | 4.402 | 0.819 | 0.552 |
| solo, syncs off | 200 | 41,830 | 4.776 | 1.202 | 0.576 |
| solo, syncs off | 300 | 49,143 | 6.095 | 2.682 | 0.684 |
| std, quorum sync rep, syncs off | 25 | 6,218 | 4.021 | 0.298 | 0.830 |
| std, quorum sync rep, syncs off | 50 | 11,772 | 4.246 | 0.458 | 0.855 |
| std, quorum sync rep, syncs off | 100 | 21,577 | 4.632 | 0.801 | 0.890 |
| std, quorum sync rep, syncs off | 150 | 29,620 | 5.061 | 1.247 | 0.918 |
| std, quorum sync rep, syncs off | 200 | 35,623 | 5.608 | 1.870 | 0.950 |
| std, quorum sync rep, syncs off | 300 | 40,993 | 7.307 | 4.083 | 1.063 |
| pg-raft FUSE, `fsync`, syncs off | 25 | 5,903 | 4.235 | 0.356 | 1.030 |
| pg-raft FUSE, `fsync`, syncs off | 50 | 11,106 | 4.501 | 0.556 | 1.085 |
| pg-raft FUSE, `fsync`, syncs off | 100 | 19,933 | 5.014 | 1.004 | 1.158 |
| pg-raft FUSE, `fsync`, syncs off | 150 | 26,681 | 5.618 | 1.622 | 1.228 |
| pg-raft FUSE, `fsync`, syncs off | 200 | 31,345 | 6.373 | 2.509 | 1.284 |
| pg-raft FUSE, `fsync`, syncs off | 300 | 34,388 | 8.712 | 5.787 | 1.474 |
| pg-raft FUSE, `sync_rep`, syncs off | 25 | 6,145 | 4.068 | 0.298 | 0.873 |
| pg-raft FUSE, `sync_rep`, syncs off | 50 | 11,580 | 4.316 | 0.479 | 0.913 |
| pg-raft FUSE, `sync_rep`, syncs off | 100 | 20,948 | 4.771 | 0.878 | 0.979 |
| pg-raft FUSE, `sync_rep`, syncs off | 150 | 28,287 | 5.299 | 1.418 | 1.040 |
| pg-raft FUSE, `sync_rep`, syncs off | 200 | 33,382 | 5.985 | 2.194 | 1.104 |
| pg-raft FUSE, `sync_rep`, syncs off | 300 | 37,486 | 7.993 | 4.929 | 1.260 |
| pg-raft hook, `fsync`, syncs off | 25 | 5,914 | 4.227 | 0.363 | 1.030 |
| pg-raft hook, `fsync`, syncs off | 50 | 11,134 | 4.489 | 0.555 | 1.080 |
| pg-raft hook, `fsync`, syncs off | 100 | 19,917 | 5.018 | 0.984 | 1.164 |
| pg-raft hook, `fsync`, syncs off | 150 | 26,719 | 5.609 | 1.578 | 1.226 |
| pg-raft hook, `fsync`, syncs off | 200 | 31,341 | 6.374 | 2.516 | 1.284 |
| pg-raft hook, `fsync`, syncs off | 300 | 34,924 | 8.579 | 5.682 | 1.441 |
| pg-raft hook, `sync_rep`, syncs off | 25 | 6,132 | 4.077 | 0.305 | 0.880 |
| pg-raft hook, `sync_rep`, syncs off | 50 | 11,541 | 4.331 | 0.480 | 0.924 |
| pg-raft hook, `sync_rep`, syncs off | 100 | 20,867 | 4.789 | 0.883 | 0.993 |
| pg-raft hook, `sync_rep`, syncs off | 150 | 28,119 | 5.330 | 1.425 | 1.057 |
| pg-raft hook, `sync_rep`, syncs off | 200 | 33,140 | 6.028 | 2.241 | 1.121 |
| pg-raft hook, `sync_rep`, syncs off | 300 | 37,305 | 8.031 | 4.985 | 1.272 |

</details>

#### Scale 100, `wal_compression = lz4`, syncs on

std and pg-raft hook + `sync_rep` again with `wal_compression = lz4` (`WAL_COMPRESSION=lz4`),
everything else as above. tps (% of std):

| Setup | 25 | 50 | 100 | 150 | 200 | 300 |
|---|---|---|---|---|---|---|
| std, quorum sync rep | 6,131 | 11,550 | 20,932 | 28,392 | 33,722 | 38,578 |
| pg-raft hook, `sync_rep` | 6,075 (99%) | 11,360 (98%) | 20,367 (97%) | 27,453 (97%) | 32,265 (96%) | 36,342 (94%) |

lz4 changes little at scale 100: under ±1% for both up to 200 clients, std +1.5% and pg-raft +2.1%
at 300 (so 94% of std either way). The accounts table (~1.5 GB) is small, so full page images are
a small part of the WAL. pg-raft's latency stddev at 300 clients drops from 8.3 to 6.7 ms. lz4 is
now the bench default (`7d1fbef`).

<details><summary>Scale 100 with lz4: tps, latency and <code>END</code></summary>

| Setup | Clients | tps | latency avg (ms) | stddev (ms) | `END` (ms) |
|---|---|---|---|---|---|
| std, quorum sync rep | 25 | 6,131 | 4.078 | 0.361 | 0.882 |
| std, quorum sync rep | 50 | 11,550 | 4.327 | 0.628 | 0.923 |
| std, quorum sync rep | 100 | 20,932 | 4.774 | 1.225 | 0.986 |
| std, quorum sync rep | 150 | 28,392 | 5.278 | 1.749 | 1.033 |
| std, quorum sync rep | 200 | 33,722 | 5.923 | 2.466 | 1.080 |
| std, quorum sync rep | 300 | 38,578 | 7.764 | 4.815 | 1.200 |
| pg-raft hook, `sync_rep` | 25 | 6,075 | 4.115 | 0.399 | 0.919 |
| pg-raft hook, `sync_rep` | 50 | 11,360 | 4.400 | 1.614 | 0.983 |
| pg-raft hook, `sync_rep` | 100 | 20,367 | 4.907 | 2.998 | 1.068 |
| pg-raft hook, `sync_rep` | 150 | 27,453 | 5.459 | 2.728 | 1.125 |
| pg-raft hook, `sync_rep` | 200 | 32,265 | 6.193 | 3.554 | 1.190 |
| pg-raft hook, `sync_rep` | 300 | 36,342 | 8.243 | 6.748 | 1.334 |

</details>

#### Scale 1000, `wal_compression = lz4`, `max_wal_size = 64GB`, syncs on, 150 to 300 clients

Scale 1000 (`PGBENCH_INIT_ARGS="-I dtGvp"`: the load generated on the server; 1,000 branch rows,
~13 GB of accounts, far larger than `shared_buffers`), `MAX_WAL_SIZE=64GB`, lz4, syncs on
(pg-raft with `leader_sync` and `follower_sync`). tps (% of std):

| Setup | 150 | 200 | 300 |
|---|---|---|---|
| solo | 35,844 (111%) | 46,085 (111%) | 56,271 (111%) |
| std, quorum sync rep | 32,375 | 41,376 | 50,525 |
| pg-raft FUSE, `fsync` | 29,583 (91%) | 38,442 (93%) | 49,256 (97%) |
| pg-raft FUSE, `sync_rep` | 30,945 (96%) | 39,965 (97%) | 48,614 (96%) |
| pg-raft hook, `fsync` | 29,666 (92%) | 38,268 (92%) | 49,327 (98%) |
| pg-raft hook, `sync_rep` | 30,262 (93%) | 38,841 (94%) | 46,177 (91%) |

- **pg-raft with `fsync` is at 91–98% of std**, 97–98% at 300 clients (49.3k against 50.5k), with
  hook and FUSE within 0.6% of each other. On 2026-11-18 (PostgreSQL 17.9, no compression,
  `max_wal_size = 16GB`) it was at 72–75%; on 2026-11-19 with lz4 and 64GB, 94%.
- **Less contention than at scale 100**: with 1,000 branch rows a longer commit queues fewer
  transactions, so pg-raft's gap shrinks as the clients grow instead of widening.
- **`sync_rep` gains at 150–200 clients and loses at 300**: +1.5–5% over `fsync` at 150–200 (FUSE
  96–97% of std), but at 300 hook + `sync_rep` does 6% less than hook + `fsync` (91% of std) and
  FUSE + `sync_rep` 1% less. Its latency stddev is 3–4 times `fsync`'s (300 clients: hook 5.6 ms,
  FUSE 4.0 ms, against 1.2–1.3 ms; std 1.4 ms), as on 2026-11-19: some commits stall. Not yet
  understood. Single runs: the repeats below say whether hook and FUSE `sync_rep` really differ.
- **solo is 11% above std** at every client count.

<details><summary>Scale 1000: tps, latency and <code>END</code></summary>

| Setup | Clients | tps | latency avg (ms) | stddev (ms) | `END` (ms) |
|---|---|---|---|---|---|
| solo | 150 | 35,844 | 4.181 | 0.404 | 0.677 |
| solo | 200 | 46,085 | 4.334 | 0.462 | 0.728 |
| solo | 300 | 56,271 | 5.321 | 0.992 | 1.061 |
| std, quorum sync rep | 150 | 32,375 | 4.629 | 0.705 | 1.094 |
| std, quorum sync rep | 200 | 41,376 | 4.827 | 0.791 | 1.180 |
| std, quorum sync rep | 300 | 50,525 | 5.922 | 1.433 | 1.685 |
| pg-raft FUSE, `fsync` | 150 | 29,583 | 5.064 | 0.790 | 1.526 |
| pg-raft FUSE, `fsync` | 200 | 38,442 | 5.196 | 0.861 | 1.564 |
| pg-raft FUSE, `fsync` | 300 | 49,256 | 6.075 | 1.173 | 1.994 |
| pg-raft FUSE, `sync_rep` | 150 | 30,945 | 4.843 | 3.018 | 1.289 |
| pg-raft FUSE, `sync_rep` | 200 | 39,965 | 4.997 | 2.935 | 1.327 |
| pg-raft FUSE, `sync_rep` | 300 | 48,614 | 6.157 | 4.045 | 1.857 |
| pg-raft hook, `fsync` | 150 | 29,666 | 5.051 | 0.761 | 1.510 |
| pg-raft hook, `fsync` | 200 | 38,268 | 5.221 | 0.943 | 1.560 |
| pg-raft hook, `fsync` | 300 | 49,327 | 6.063 | 1.286 | 1.966 |
| pg-raft hook, `sync_rep` | 150 | 30,262 | 4.950 | 3.388 | 1.392 |
| pg-raft hook, `sync_rep` | 200 | 38,841 | 5.142 | 3.573 | 1.458 |
| pg-raft hook, `sync_rep` | 300 | 46,177 | 6.483 | 5.598 | 2.099 |

</details>

#### Scale 1000, repeated (in progress)

The scale 1000 setups above (without solo) in up to 5 rounds (`campaign-rep.sh`), to measure the
run-to-run variation: each round runs std, hook + `fsync`, hook + `sync_rep`, FUSE + `fsync`, FUSE +
`sync_rep` in that order, each on a fresh cluster with the same settings, at 150, 200 and 300
clients; ~55 min a round. Started 2026-11-23 23:11 UTC; `rep-aggregate.md` will have each point's
mean, min, max and spread. Round 1 so far, tps (% of std):

| Setup | 150 | 200 | 300 |
|---|---|---|---|
| std, quorum sync rep | 32,322 | 41,456 | 50,075 |
| pg-raft hook, `fsync` | 29,402 (91%) | 38,142 (92%) | 48,985 (98%) |
| pg-raft hook, `sync_rep` | 29,896 (92%) | 38,646 (93%) | 46,379 (93%) |

Against the single runs above: std within 0.9%, hook + `fsync` within 0.9%, hook + `sync_rep`
within 1.2%; hook + `sync_rep` again falls behind `fsync` at 300 clients.

## pg-raft notes

- **Commit-path syncs.** `raft/config.toml` has `leader_sync` and `follower_sync`, both `false`:
  a commit is durable once a majority has its ledger records in the page cache (durability by
  replication). Set them to `true` (`RAFT_SYNC=1` with `multi/` sets both) to measure the cost of
  syncing the ledger to disk as well: the leader's sync runs during the round, a follower's before
  it answers. (In the results above, from before the ledger, `docs/ledger.md`, the same names
  were msyncs of the WAL, and `log_sync` synced LMDB.)
- **Use a fresh cluster per run** (`install_bins.sh`, `init.sh`). Restarting a stopped pg-raft
  cluster can leave a standby unable to rejoin ("requested timeline N is not a child of this
  server's history"); `wait.sh` then fails and names the node. That is a known pg-raft bug
  (`docs/todo.md`).
- Stopping takes ~20 s: once the others are gone, the last node can't commit PostgreSQL's shutdown
  checkpoint (no quorum), so after 12 s it fails the fsync and PostgreSQL PANICs, as intended.
  `start_node.sh` disables core dumps for that reason.
- The proxy's engine: `PROXY_ARGS="--engine uring --threads 2" ./raft/start_proxy.sh` (or
  `start.sh`); `epoll` is the default on Linux, `asio` the portable one.
- Backups: `config.toml` pushes the periodic backups out (1 day or 1 TiB of WAL) so that none
  starts in the middle of a run. Lower `backup_interval_secs` / `backup_wal_size` to include them.
- The proxy's admin page: http://<proxy machine>:8080/status.html.
