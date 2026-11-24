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
| `election_times.py [LOGS]` | Leader election and failover times of a run (below): writes `logs/election_times.json`, prints the table and summaries. |
| `config.toml.in` | The cluster: small ledger files (64 MiB) and backups every 15 min, so that compaction, backups and restores happen during a run. |

Everything a run creates stays here, and is ignored by git: `config.toml`, `bin/` (copies of the
binaries: rebuilding the repository doesn't change a running test), `data/node<N>`, `backups/`,
`logs/` (`events.log`, `node<N>.log`, `proxy.log`, `pgbench.log`, `run.out`, `failover.jsonl`), `old/`, `acked.txt`.

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

## Election and failover times

`chaos.py run` writes `logs/failover.jsonl`: every action's fault time (just before the signal) and
every change of the proxy's primary (`/primary`, polled every 50 ms), in wall-clock ms.
`python3 chaos/election_times.py` matches them with the node logs and reports, from the fault:

| | |
|---|---|
| candidate | the first node to start an election (`PreVote for term=T won` / `New candidate`) |
| leader | a node takes full leadership of a newer term (raft: `Full leadership term=T`) |
| serving | that node's PostgreSQL is promoted and serving (`serving as the primary`) |
| proxy | the proxy has a serving primary again: another node, or the same one after a gap |

For `crash_majority` and `crash_all` it also measures from the restart that gives back a majority
(a node's `Reading config file` line): the survivors of the first crash can elect a leader before
the second lands, and that leader has no quorum.

## Report: 2-hour run, 2026-11-23

`./chaos/fresh.sh 2` on macOS (16 cores, Apple clang 17, `optimization=speed`),
`wal_capture = "fuse"`, `commit_wait = "fsync"`, the config in `config.toml.in`; 21:49–23:51.

### Outcome

| | |
|---|---|
| Actions | 97: crash_leader 22, crash_follower 25, term_leader 12, term_follower 5, pause_leader 10, kill_leader_pg 7, kill_follower_pg 4, crash_majority 7, crash_all 5 |
| Consistency checks | 20 (every 5 actions and the final one), **all OK**: no acknowledged write lost (240,229 acknowledged ids, all present), pgbench's invariant held, every standby converged |
| Unexpected node exits / slow recoveries (> 240 s) / failed checks | 0 / 0 / 0 |
| Workload | 17,484,435 pgbench transactions (366 runs, 87 ended by a failover); 98 writer inserts with an unknown outcome (failed or timed out during a failover) |
| Leaderships | 82 (terms 1–83), 72 of them promoted to serving; 26 backups stored, 3 restores from a backup |

### Leader election times

Seconds from the fault. Leader-only faults (one node: the leader crashes, its PostgreSQL is
killed, it is paused for 1–8 s, or it shuts down cleanly):

| Fault | n | Election (leader) median / p90 / max | Promoted (serving) median / p90 / max | Client-visible (proxy) median / p90 / max |
|---|---|---|---|---|
| crash_leader (`SIGKILL`) | 22 | 0.28 / 0.36 / 0.39 | 1.33 / 1.48 / 1.64 | 1.48 / 1.63 / 1.75 |
| kill_leader_pg | 7 | 0.38 / 0.48 / 0.48 | 1.44 / 1.69 / 1.69 | 1.61 / 1.88 / 1.88 |
| pause_leader (1–8 s `SIGSTOP`) | 10 | 0.29 / 0.40 / 0.40 | 1.44 / 3.98 / 3.98 | 1.52 / 4.18 / 4.18 |
| term_leader (`SIGTERM`, hand-over) | 12 | 0.93 / 1.24 / 1.26 | 1.94 / 2.23 / 2.45 | 2.07 / 2.68 / 3.57 |
| **Unplanned, all (crash, kill_pg, pause)** | **39** | **0.29 / 0.40 / 0.48** | **1.38 / 1.64 / 3.98** | **1.51 / 1.85 / 4.18** |

- An unplanned leader loss is detected in 0.2–0.4 s (the 200–300 ms election timeout), and the
  vote takes 2–3 ms: candidate to full leadership was under 5 ms in every one of these elections.
  No split votes after a single-node fault.
- Promoting PostgreSQL adds about 1.1 s (replay to `wal_end`, `promote()`, the new timeline's
  history committed, `CHECKPOINT`); the proxy sees it 0.1–0.2 s later.
- A clean shutdown is slower to elect (0.6–1.3 s): the leader hands over in its shutdown, after
  PostgreSQL's fast stop.
- Pause #4 (3.98 s to serving): raft elected node 1 in 0.25 s, but its node began promoting 2.6 s
  later; it had started a `pg_basebackup` 0.1 s before the election (not investigated further).
- After a pause, when the old leader resumes, the proxy reports no primary for ~0.11 s, then the new
  one again (9 of 10 pauses).

Majority faults, from the restart that gives back a quorum:

| Fault | n | Election median / max | Serving median / max | Proxy median / max |
|---|---|---|---|---|
| crash_majority + crash_all | 9 | 6.2 / 89.0 | 7.1 / 90.2 | 7.3 / 90.3 |

- After `crash_all`, no node campaigns until its own standby runs again ("Not starting an election:
  the standby is at (not running)"): rebuilding `pg/pg_wal` and PostgreSQL's crash recovery, 6–8 s.
- In 4 of the 12 majority faults the harness's crash step took ~60 s before the restart (the
  `quorum` column of `election_times.py`; most likely the forced FUSE unmount, whose attempts time
  out after 30 s each); the down time doesn't count in the times above.
- In #94–96 a survivor won an election while the second node was still being crashed (so no
  `leader` after the quorum restart): in #95 and #96 it promoted once the quorum was back (proxy
  1.4–2.3 s after the restart); in #94 it served 7 s before (finding 3).

### Findings

1. **Compaction on the leader causes spurious elections.** 8 of the 27 compactions on a leader were
   followed within 0.12–0.27 s by a follower's election ("Untriggered election: no message from the
   leader … for ~250–300 ms") and a PostgreSQL failover: the leader logs `Keeping the entries from
   segment …` and `Compacting: …` and goes silent long enough (its queue shows `Time in the queue
   200–364 milliseconds`) for the 200–300 ms election timeout. Compaction follows a new backup
   (`keep_from` moves), and a new leader often compacts within a second of taking over, so it can
   be deposed at once (#7, #68). This is the cause of the elections after follower faults
   (#43 crash_follower, #64 kill_follower_pg) and of the second elections in #7, #68 and #97.
   Compaction (`RaftServer`, with the server lock held) should not block heartbeats.
2. **A leader whose promotion failed can stay leader without serving.** In #35 (crash_majority)
   node 1 was elected by a survivor before that survivor was crashed; with no quorum its promotion
   failed ("PostgreSQL did not leave recovery: stepping down"), and the step-down gave up after 6 s
   ("Stepdown … did not hand over the leadership, calling it off"). When the other nodes came back
   it had a quorum again, but nothing retried the promotion, and its heartbeats kept the others from
   campaigning (PreVote refused): no primary for 89 s after the quorum was back, until a compaction
   stall (finding 1) deposed it.
3. **A leader serves after its post-promotion `CHECKPOINT` failed.** In #94 node 0 was elected
   (0.2 s), lost its quorum 0.5 s later, and its `CHECKPOINT` timed out after 60 s ("the checkpoint
   after the promotion did not complete"); `Node::promote` goes on to `set_serving(true)`, so the
   proxy routed clients to a primary that couldn't commit until the others restarted 7 s later. No acknowledged write was lost (commits wait for raft), but the
   checkpoint's protection (the standby's minimum recovery point, `node.cc` step 5) is skipped.

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
