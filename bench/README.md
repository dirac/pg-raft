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

Both setups use what pg-raft forces on its PostgreSQL (`wal_sync_method = fsync`,
`wal_recycle = off`), data checksums, and the shared settings in `raft/config.toml`'s
`[pg_config]`, `std/postgresql.std.conf` and `solo/postgresql.solo.conf` (keep them the same). Every result records the
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
first, then the nodes; the machine running the scripts is the client. Each machine has its local
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
| `gen.sh` | the configs for 3 and 5 nodes and for solo, in `multi/gen/`; `RAFT_SYNC=1` turns pg-raft's commit-path syncs on, `PG_FSYNC=off` runs std and solo with `fsync = off` (both also read by `deploy.sh`) |
| `deploy.sh [--no-build]` | `gen.sh`, a release build into `~/shared/pg_raft/bin`, bench/ copied to every machine, the binaries installed |
| `run.sh solo \| std N \| raft N \| stop` | one setup on a fresh cluster, pgbench at each client count in `SWEEP` (pg-raft: through the proxy and direct), then stop it and copy the results to `~/shared/pg_raft/results`; `VARIANT=name` tags the results (`raft-direct-sync-n3-c10-…`) |
| `all.sh [setups]` | every setup in turn, then `summary.sh` |
| `summary.sh [dir]` | one table (`summary.md`, `summary.csv`) from the results' `run.log`s |

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
- The proxy's engine: `PROXY_ARGS="--engine uring --threads 2" ./raft/start_proxy.sh` (or
  `start.sh`); `epoll` is the default on Linux, `asio` the portable one.
- Backups: `config.toml` pushes the periodic backups out (1 day or 1 TiB of WAL) so that none
  starts in the middle of a run. Lower `backup_interval_secs` / `backup_wal_size` to include them.
- The proxy's admin page: http://<proxy machine>:8080/status.html.
