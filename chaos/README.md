# Chaos testing

A 3-node pg-raft cluster and its proxy on this machine, under a write load, with nodes crashed,
stopped, paused and their PostgreSQL killed at random, for hours. After every few failures the load
pauses and the run checks that nothing acknowledged was lost and that every node holds the same
data.

```bash
./chaos/fresh.sh 3                    # build, new cluster, 3 hours of chaos in the background
tail -f chaos/logs/events.log         # actions, recoveries, checks, errors
grep -E "ERROR|CHECK FAILED|FINAL" chaos/logs/events.log
python3 chaos/chaos.py stop           # stop the run's nodes and proxy (kill the run first:
                                      # pkill -f "chaos.py run")
```

```bash
WAL_CAPTURE=hook ./chaos/fresh.sh 3   # the same with PostgreSQL's WAL hooks instead of FUSE (pg-raft's
                                      # PostgreSQL, built by external/build-pg-raft-postgresql.sh)
SETUP_ONLY=1 ./chaos/fresh.sh         # a new cluster left running, no chaos (to measure or inspect)
```

Requirements: the pg-raft build environment (`b2` in `/opt/pg_raft/bin`, see the top-level
`CLAUDE.md`), PostgreSQL in `/opt/pg_raft/postgresql/18/bin` (`PG_BIN` overrides it), FUSE, and
`python3` (standard library only). Ports 6432 (proxy), 6480 (proxy admin), 6441–6443 (PostgreSQL),
7101–7103 (raft) and 26441–26443 (the WAL senders, Unix sockets in `/tmp`) must be free.

## Files

| | |
|---|---|
| `fresh.sh [HOURS]` | Builds `pg_raft_node`, `pg_raft_initdb` and `pg_raft_proxy` with `optimization=speed` (the debug variant: assertions stay on), stops a previous run, deletes its cluster, archives its logs into `old/`, writes `config.toml` from `config.toml.in`, runs `chaos.py setup`, then `chaos.py run HOURS` in the background. |
| `chaos.py setup` | `pg_raft_initdb`, the proxy and 3 nodes, `pgbench -i -s 20` through the proxy, and the `chaos_log` table. |
| `chaos.py run HOURS` | Restarts everything under itself, then runs the workloads and the failures, and checks (below). |
| `chaos.py check` | One consistency check of a running cluster. |
| `chaos.py stop` | Kills the nodes (and their PostgreSQL), removes leftover FUSE mounts, stops the proxy. |
| `config.toml.in` | The cluster: small ledger files (64 MiB) and backups every 15 min, so that compaction, backups and restores happen during a run. |

Everything a run creates stays here, and is ignored by git: `config.toml`, `bin/` (copies of the
binaries: rebuilding the repository doesn't change a running test), `data/node<N>`, `backups/`,
`logs/` (`events.log`, `node<N>.log`, `proxy.log`, `pgbench.log`, `run.out`), `old/`, `acked.txt`.

Use an optimized build, as `fresh.sh` does. At `-O0` the nodes are slow enough to miss raft's
200 ms election timeout under this load: elections churn and every one is a PostgreSQL failover.

## Workloads

All client traffic goes through the proxy (port 6432), so a failover is what a client sees.

- **pgbench**: `pgbench -c 8 -j 4 -T 20` in a loop. Runs that fail on a failover are counted, not
  errors.
- **Writer**: `INSERT INTO chaos_log(id) VALUES (n)` with `n` increasing, one `psql` each. An id
  whose insert succeeded goes into `acked.txt`; one whose insert failed or timed out into
  `unknown.txt` (it may or may not have committed).

## Failures

Every 20–60 s one action, picked at random with these weights:

| Action | Weight | What it does |
|---|---|---|
| `crash_leader` / `crash_follower` | 25 / 20 | `SIGKILL` the node, then its PostgreSQL (postmaster stopped, children killed); half the time its FUSE mount is unmounted (a machine crash), half the time left dead (a process crash: the node must clean it up). Restarted after 3–30 s. |
| `term_leader` / `term_follower` | 10 / 5 | `SIGTERM`: a clean shutdown (the leader hands over). Restarted after 3–30 s. |
| `pause_leader` / `pause_follower` | 10 / 5 | `SIGSTOP` the node for 1–8 s, then `SIGCONT` (a GC pause or a short partition; its PostgreSQL keeps running). |
| `kill_leader_pg` / `kill_follower_pg` | 8 / 5 | `SIGKILL` only the node's PostgreSQL: the node must notice and recover. |
| `crash_majority` | 7 | Crash the leader and a follower; restart both. |
| `crash_all` | 5 | Crash all three; restart them in random order. |

After each action the run waits for recovery: the proxy reports a serving primary, a write through
the proxy succeeds, and every running node's PostgreSQL accepts connections. More than 240 s is an
error (`ERROR: no recovery within 240 s`), and the run then restarts every node to go on. A node
process that exits by itself is an error too (`ERROR: node N exited unexpectedly`), and is
restarted.

## Checks

Every 5 actions, and at the end, the workloads pause and the run checks the cluster:

1. A primary serves (within 300 s).
2. On the primary, after a `CHECKPOINT`: every id in `acked.txt` is in `chaos_log` (no
   acknowledged commit lost), and pgbench's invariant holds:
   `sum(abalance)` of the accounts = `sum(tbalance)` of the tellers = `sum(bbalance)` of the
   branches = `sum(delta)` of the history.
3. Every standby replays up to the primary's flush LSN (within 180 s) and has the same row count
   and sums as the primary.

A check logs `check [round N]: OK ...` or `CHECK FAILED [round N]: ...`. The last line of a run is
`FINAL stats: {...}`: acknowledged writes, pgbench transactions, actions, unexpected exits, slow
recoveries, failed checks.

## Reading a failure

- `logs/events.log` has the action before the error and the cluster status (from the proxy's
  `/status.json`).
- `logs/node<N>.log` has the node's log and its PostgreSQL's (lines starting `[pgres]`). The node
  logs its transitions from `node.cc`: `promoting`, `serving as the primary`, `lost leadership`,
  `rebuilding pg/pg_wal`, `restoring the backup`, `PostgreSQL exited`.
- `pg_waldump -p data/node<N>/pg/pg_wal` (or `raft/wal`) and `pg_controldata -D data/node<N>/data`
  show the WAL and the control file a node restarts with.
- `sample <pid>` (macOS) shows where a hung node's threads are. Don't use `lldb --batch` on a
  running node: it kills the process when it exits.

## Notes

- One machine: the "network" is loopback, so partitions are only simulated by pauses.
- `commit_wait = "fsync"` by default; `COMMIT_WAIT=sync_rep ./chaos/fresh.sh` runs the `sync_rep` mode.
- On macOS, PostgreSQLs killed with their data directories deleted leave System V shared memory
  behind; `fresh.sh` removes the orphaned segments (macOS allows 32).
