# `node` — The pg-raft node process (design, not yet implemented)

Source: `src/node/` (to be written). The node process ties together the existing modules: `raft`,
`wal_fuse`, `wal_sender`, `process` and `status`. This document is the proposed design for review.
§10 lists the decisions that are still open.

## 1. What a node is

One process per server:

```
pg_raft_node --config config.toml --id N [--init]
```

| Component | Role in the node |
|---|---|
| `RaftServer` | Listens on `servers[N].ha_port` and decides whether this node is the leader or a follower. |
| `PgInstance` | The local PostgreSQL: data directory `wdir/node<N>/data`, port `servers[N].pg_port`. |
| `Fuse` + `WalFuseOps` | Mounted over `data/pg_wal`, backed by `wdir/node<N>/pg/pg_wal`: the WAL PostgreSQL itself writes. |
| `NodeWalTracker` | The `WalTracker`. On the leader it turns intercepted WAL writes and fsyncs into raft entries (fsync waits for a quorum). On a follower it does nothing with raft: fsync is local. |
| `WalSenderServer` | A TCP port on loopback (§2.3). The local standby replicates from it, reading `raft/wal`. |
| `PgClient` poller | Every 500 ms, reads the recovery state and LSNs and calls `RaftServer::set_pg_status()`. |

### Directory layout

```
wdir/node<N>/
  data/          PGDATA (data/pg_wal is the FUSE mount point)
  pg/pg_wal/     FUSE backing directory: the WAL that PostgreSQL itself writes
  raft/          raft log (term.lmdb, logs.lmdb)
  raft/wal/      raft's wal_dir: the replicated WAL
```

There are two WAL directories, on purpose:

- **`raft/wal`** holds the replicated WAL, and means the same thing on every node. Raft writes
  accepted bytes there (possibly before they commit), the peer connections send from it
  (`RaftServer::segment()`), and the WAL sender reads it.
- **`pg/pg_wal`** holds only what the local PostgreSQL wrote: on the primary, the WAL it
  generates; on a standby, the WAL it received from the WAL sender, which is committed WAL only.

They must be separate. A standby that restarts replays whatever valid WAL it finds in its own
`pg_wal` before it connects to the WAL sender. If raft wrote into the same directory, that would
include the uncommitted tail of the log, which a new leader may later truncate.

The PostgreSQL binaries are in `install_dir/<pg_major_version>/bin`.

## 2. The WAL path

### 2.1 Leader (PostgreSQL is the primary)

`NodeWalTracker` runs in **leader mode**:

- `async_publish(seg, off, size)` → copy the published bytes from the FUSE segment (in
  `pg/pg_wal`) into the same segment in `raft/wal`, then
  `exec(WalCommand{flushed_lsn, seg.lsn, WriteOp{off, size}}, …)`. The peer connections send the
  bytes from `raft/wal`.
- `async_fsync(seg, off, size, cb)` → a local `msync`, then
  `exec(WalCommand{flushed_lsn, seg.lsn, FlushOp{off, size}}, h)`. `h` calls `cb(ok)` when the
  entry commits, or `cb(kNoConsensus)` when it gets `kNotLeader`.

PostgreSQL's fsync, and therefore its commit, returns only once the WAL is on a majority. Entries
are appended in WAL order, so committing a `FlushOp` implies that the `WriteOp`s before it are
committed too.

**`flushed_lsn` is fetched from the primary**: it is the last value of
`pg_current_wal_flush_lsn()` read by the status poller (§5), which `NodeWalTracker` puts into
every command. It is not derived from the byte range being written or fsynced. The value is stale
but safe: PostgreSQL reports a flush LSN only after its fsync returned, so whatever the poller read
is already committed. The followers' WAL senders stream up to it (§2.2), so they only lag the
primary by up to one poll interval.

### 2.2 Follower (PostgreSQL is a standby)

- Raft writes the WAL bytes into `raft/wal` as it accepts requests.
- The WAL sender reads `raft/wal`. `RaftObserver::applied()` calls
  `WalSender::advance(state.flushed_lsn)` for each connected sender, so the standby receives only
  committed WAL.
- **Why `flushed_lsn` and not the end of the `WriteOp`s:** PostgreSQL writes and fsyncs WAL in
  whole pages. A write covers the whole page even when only 10 bytes of it are new, and so does an
  fsync. From FUSE, the `WriteOp` and `FlushOp` ranges therefore don't tell how far the WAL has
  really progressed. Only PostgreSQL's own flush LSN (`pg_current_wal_flush_lsn()`, polled from
  the primary, §2.1) marks a point the standby may safely stream to. The cost is that standbys lag
  the primary by up to one poll interval.
- The standby writes what it receives into its own `pg_wal`, which is the FUSE mount (backed by
  `pg/pg_wal`), with the tracker in **follower mode**. On a follower the FUSE layer does **not**
  go through raft and does **not** wait for a quorum: publishes are ignored, and fsync is a local
  `msync` that returns as soon as the bytes are on the local disk. The WAL the standby receives is
  already committed, so there is nothing to agree on. `pg/pg_wal` therefore only ever holds
  committed WAL, and restarting the standby is safe.
- The FUSE mount stays in place on a follower only so that promotion needs no remount; while the
  node follows, it is a passthrough with local fsync.

**Promotion needs no remount.** The new leader first commits everything it inherited, and the WAL
sender streams that from `raft/wal` to the standby. Once the standby has replayed it, the node
switches the tracker to leader mode and promotes. New WAL then goes to `pg/pg_wal` and is copied
into `raft/wal` (§2.1).

The cost of two directories: a `memcpy` of every published range on the leader, and a second copy
of the WAL on disk on every node.

### 2.3 WalSenderServer on a TCP port

The `WalSenderServer` listens on a TCP port, not on a Unix-domain socket. A Unix socket has to be
named `.s.PGSQL.<port>` in a socket directory, and that file naming caused problems.

- It binds `127.0.0.1` (only the local standby connects).
- The standby's `primary_conninfo` is `host=127.0.0.1 port=<port>`.
- The current `WalSenderServer` binds a Unix socket (`socket_dir/.s.PGSQL.<port>`); it is changed
  back to a TCP acceptor as part of phase 1. `create()` loses `socket_dir`, and `socket_path()`
  goes away; `bound_port()` stays.
- Which port: see decision 4 in §10.

## 3. Role transitions

There is no control loop. Raft knows exactly when the role changes, so it tells the node through
hooks, and the node reacts.

### 3.1 Hooks and the node strand

`RaftApplier` is renamed `RaftObserver`, since it is no longer only about applied entries, and
gains two hooks:

```cpp
struct RaftObserver {
  virtual void applied(RaftIndex, Entry const&, RaftState const&) {}
  virtual void installed(RaftState const&) {}
  virtual void became_leader(RaftTerm term) {}  // full leadership
  virtual void stepped_down(RaftTerm term) {}   // leader -> follower
};
```

Where they fire in `RaftServer`:

- **`became_leader`** fires at **full leadership**: where `type_` becomes `kLeader`, once
  `commit_idx == last_entry`. It does not fire in `make_leader()`, when a candidate merely has the
  votes. At full leadership every inherited entry is committed, so `raft/wal` holds every byte that
  the previous primary acknowledged.
- **`stepped_down`** fires in `make_follower()` when the previous role was leader. A candidate that
  falls back to follower is not an event: its PostgreSQL never stopped being a standby.

Both hooks run **with the server lock held**, so they must not block. A hook only records the
new role and posts a transition to a single **node strand**, which runs transitions one at a time.
If the role flips again while a transition runs (leader → follower → leader), the strand acts
only on the latest role when it gets to it. A stale transition is skipped, not queued.

One thing happens **inside** `stepped_down` itself, because it is cheap and must not wait: the
tracker switches to follower mode, so that nothing PostgreSQL writes after this point is
published to raft.

### 3.2 Promotion (`became_leader`)

On the node strand:

The standby replays **all** the WAL the node has, to the end. `flushed_lsn` is not used here: it
is a polled value and always stale, so it can stop short of WAL the old primary had already
acknowledged. At full leadership every entry in the log is committed, so everything in `raft/wal`
is committed WAL.

1. Advance the WAL sender to the **end of the WAL in `raft/wal`**: `state.wal_end`, the end of the
   last applied `WriteOp` (segment + offset + size). This is a small raft change: `RaftState`
   tracks `wal_end` as it applies entries. `wal_end` is page-granular (a `WriteOp` covers whole
   pages, §2.2), so it can be past the last valid record; that is fine here, because PostgreSQL
   stops replaying at the last complete record.
2. Wait until the standby has received all of it: its `StandbyStatusUpdate` (the WAL sender's
   `StatusCb`, which `WalSenderServer` must now pass on) reports
   `StandbyStatus::flushed_lsn >= wal_end` (what the standby has written to its disk). This is
   a bounded wait within the transition, with a timeout, not a loop. On timeout the node steps
   down, so that another node can take over.
3. Switch the tracker to leader mode, before PostgreSQL writes any new WAL.
4. Promote with `SELECT pg_promote()` over `PgClient`. On promotion PostgreSQL replays everything
   it received, up to the last complete record, then ends recovery and starts a new timeline
   (§7). An incomplete record at the very end is discarded, and the new timeline starts after the
   last complete one. `PgInstance::promote()` only sends SIGUSR1, which doesn't promote on
   PostgreSQL ≥ 12; it is fixed as part of this work.
5. Once PostgreSQL is out of recovery and `pg_isready` succeeds, **publish the new timeline**:
   - Fetch it from PostgreSQL over `PgClient`, for example
     `SELECT timeline_id FROM pg_control_checkpoint()`, and read its history file
     (`<timeline>.history`, in `pg/pg_wal`).
   - Append `exec(WalCommand{…, TimelineOp{timeline, content of the history file}})` and wait
     for it to commit (quorum). If it fails (`kNotLeader`), the node is no longer the leader, and
     `stepped_down` takes over.
6. Once the `TimelineOp` is committed, call `set_serving(true)`. The proxy then routes to this
   node. Clients only reach the new primary once a majority knows its timeline.

### 3.3 Demotion (`stepped_down`)

The old leader lost leadership. `make_follower()` already fails every pending `exec()` handler
with `kNotLeader`, so PostgreSQL's in-flight fsyncs fail with `kNoConsensus` and it PANICs, even
before the node strand runs. The tracker is already in follower mode (§3.1).

On the node strand:

1. `set_serving(false)`.
2. Stop PostgreSQL (Immediate) if it is still running.
3. **Cut `pg/pg_wal` back to the committed WAL.** The old primary's `pg_wal` can hold WAL that it
   wrote but never committed (its fsync failed). Truncate it at the end of the committed WAL,
   `state.wal_end` (not `flushed_lsn`, which is stale): zero the rest of that segment and remove
   the later segments. Otherwise the standby would replay that WAL on startup.
4. Restart it as a standby: `standby.signal`, and `primary_conninfo` pointing at the local
   `WalSenderServer` port. It replays its own `pg_wal` up to the committed point, then streams the
   rest from `raft/wal`.

Why this is safe without `pg_rewind`:

- The old primary never acknowledged WAL that wasn't committed, because its fsync waits for a
  quorum.
- PostgreSQL writes a data page only after the WAL covering it has been flushed, which here means
  committed. So the data directory never runs ahead of the committed WAL.
- Step 3 removes the uncommitted WAL from `pg/pg_wal`; `raft/wal` gets the new leader's bytes
  through raft.

`pg_rewind` is not needed (decision 2 in §10).

### 3.4 Startup and PostgreSQL exits

Two cases are not raft events, so no hook fires for them:

- **Startup.** The node starts PostgreSQL as a standby by itself. If raft later reaches full
  leadership, `became_leader` promotes it.
- **PostgreSQL exits on its own**, without a role change: after the PANIC in §3.3, an OOM kill, a
  crash. A watcher on the `Process` (`wait()`) posts a restart to the node strand, which restarts
  PostgreSQL in the node's current role: as a standby on a follower; on the leader, as a standby
  that is then promoted again (§3.2).

## 4. Bootstrap and joining (phase 2 of an InstallSnapshot)

### 4.1 Creating a cluster

`pg_raft_node --init` on one node:

1. `initdb` (with data checksums).
2. `write_config` and `write_hba`.
3. `create_raft_wal(wdir/node<N>/pg)`, which moves the initial segments into `pg/pg_wal` (the
   FUSE backing directory). `raft/wal` starts empty; the other nodes get the initial WAL with
   their backup.

The other nodes join as described next.

### 4.2 Joining, or recovering from `needs_backup`

A node with no data directory, or with `RaftState::needs_backup` set:

1. Sends `StatusRequest` to its peers. It picks a standby whose `applied_idx` is at least its own,
   and falls back to the leader.
2. Calls `RaftServerConnection::backup(dir, applied_idx)`.
3. Restores the backup (`PgInstance::restore`), and moves the backup's WAL into `pg/pg_wal`.
4. Reconciles WAL: the standby replays the backup's WAL from `pg/pg_wal`, then streams the rest
   from `raft/wal`. There must be no gap: the backup has to reach at least the first segment that
   `raft/wal` holds after the InstallSnapshot. A source whose `applied_idx` is behind that is
   skipped.
5. Clears `needs_backup` (new `RaftServer` API) and starts as a standby.

### 4.3 Backup provider

`set_backup_provider` serves `SnapshotRequest`s, for joining nodes and for the proxy:

- **On a standby:** `PgInstance::backup` (stop the instance and copy the data directory), then
  `verify_backup`, then restart the standby.
- **On the primary:** `pg_basebackup` (`-X stream` includes the WAL needed).
- **Response:** the list of files, relative to the data directory, plus the symbolic links. The
  backup directory is kept until the next backup or until the connection closes.

### 4.4 Raft change: `set_can_lead`

A node with no usable PostgreSQL (empty, or `needs_backup`) must not become leader, because it
would have nothing to promote. A node that can't lead doesn't start elections, but it still votes
and replicates.

## 5. Status poller

Every 500 ms, over `PgClient` on loopback:

```sql
SELECT CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_flush_lsn()::numeric END,
       CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()::numeric ELSE NULL END;
```

The result goes to:

- `RaftServer::set_pg_status()`, which reports it in `StatusResponse`;
- on the leader, `NodeWalTracker`, as the `flushed_lsn` of the commands it appends (§2.1);

## 6. Shutdown

On SIGTERM or SIGINT:

1. Raft step-down, which hands the leadership over.
2. Stop PostgreSQL (Fast).
3. Unmount FUSE.
4. Close raft.

## 7. Timelines

`pg_promote()` starts a new timeline. Followers need three things:

- `WalSender` must serve `TIMELINE_HISTORY` and handle `START_REPLICATION … TIMELINE t`,
  including ending one timeline and starting the next.
- The `.history` file written by the new primary has to be replicated. The node publishes it as a
  `TimelineOp` after promotion (§3.2, step 5), not through a FUSE hook. A follower that applies a
  `TimelineOp` writes the history file into `raft/wal`, where its WAL sender serves it for
  `TIMELINE_HISTORY`.
- The new primary writes WAL on the new timeline (the end-of-recovery record) before the
  `TimelineOp` commits, so `WriteOp`s on new-timeline segments can come before it in the log. That
  is fine: the bytes are replicated as usual, and a follower's WAL sender only switches its standby
  to the new timeline once it has applied the `TimelineOp`.
- Segment names carry the timeline. The entries already carry `LogSeqNumber{timeline, lsn}`.

## 8. Testing

1. **Steady state:**
   - 3 nodes in one process: 3 FUSE mounts and 3 PostgreSQL instances.
   - pgbench on the leader.
   - The standbys converge by LSN and row count.
2. **Failover:**
   - Kill the leader during pgbench.
   - A new leader is promoted.
   - No acknowledged transaction is lost (a row count of acknowledged inserts).
   - The old leader rejoins as a standby.
3. **Join:**
   - Add an empty node.
   - It fetches a backup, catches up, and can take over as leader.

The tests need PostgreSQL and FUSE, and skip if either is absent.

## 9. Phases

1. **Steady state:** `WalSenderServer` on a TCP port (§2.3), `Node`, `NodeWalTracker`, the
   applier, the poller, `pg_raft_node`, `--init`, a leader that promotes on start, and test 1.
2. **Failover:** the hooks and transitions (§3), `set_can_lead`, timelines (§7), and test 2.
3. **Join and backup:** §4, and test 3.

Later: WAL retention for both directories (`pg/pg_wal`: the renamed `.X` segments; `raft/wal`:
segments every node has applied past) and the proxy.

## 10. Open decisions

1. **Bootstrap:** explicit `--init` on one node (proposed), or automatically on node 0 when the
   whole cluster is empty?
2. **Demoting an old primary — resolved: no `pg_rewind`.** The old primary is restarted as a
   standby after its `pg/pg_wal` is cut back (§3.3).
3. **Scope:** implement phase 1 first and review it before failover (proposed)? Timelines need a
   `wal_sender` change that deserves its own review.
4. **WalSenderServer port:** an ephemeral port (bind port 0, read `bound_port()`, and write it into
   `primary_conninfo` each time the standby starts; proposed, no configuration needed), or a fixed
   port per server in `config.toml` (for example `servers[i].wal_port`)?
