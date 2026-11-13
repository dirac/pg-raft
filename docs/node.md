# `node` — The pg-raft node process

Source: `src/node/` — `node.{h,cc}`, `node_wal_tracker.{h,cc}`, `init.{h,cc}`, `wal_rebuild.{h,cc}`,
`pg_raft_node.cc`, `pg_raft_initdb.cc`. The node process ties together `raft`, `wal_fuse`,
`wal_sender`, `process`, `status` and `backup`. Everything below is implemented and tested (§8).

## 1. What a node is

One process per server:

```
pg_raft_initdb --config config.toml [--port N]  # once per cluster: the first backup (§4.1)
pg_raft_node --config config.toml --id N      # every server
```

| Component | Role in the node |
|---|---|
| `RaftServer` | Listens on `servers[N].ha_port` and decides whether this node is the leader or a follower. |
| `PgInstance` | The local PostgreSQL: data directory `wdir/node<N>/data`, port `servers[N].pg_port`. |
| `Fuse` + `WalFuseOps` | Mounted over `data/pg_wal`, backed by `wdir/node<N>/pg/pg_wal`: the WAL PostgreSQL itself writes. |
| `NodeWalTracker` | The `WalTracker`. On the leader it turns intercepted WAL writes and fsyncs into raft entries (fsync waits for a quorum). On a follower it does nothing with raft: fsync is local. |
| `WalSenderServer` | A Unix-domain socket (§2.3). The local standby replicates from it, reading `raft/wal`. |
| `PgClient` poller | Every 500 ms, reads the recovery state and LSNs and calls `RaftServer::set_pg_status()`. |
| `BackupStore` | Where the cluster's PostgreSQL backups are kept (`backup_store` in `config.toml`). A node without a usable data directory restores the latest one (§4). |

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

`NodeWalTracker` (`src/node/node_wal_tracker.h`) is implemented and tested against an in-process
raft ensemble (`node_wal_tracker_test`).

`NodeWalTracker` runs in **leader mode**:

- `async_publish(seg, off, size)` → copy the published bytes from the FUSE segment (in
  `pg/pg_wal`) into the same segment in `raft/wal`, and add `[off, off + size)` to the segment's
  pending range. Once that reaches `write_op_bytes` (64 KiB) it is sent at once:
  `exec(WalCommand{flushed_lsn, seg.lsn, WriteOp{range}}, …)`, so that a large transaction streams
  its WAL before its commit. Smaller writes wait for the flush. The peer connections send the
  bytes from `raft/wal`.
- `async_fsync(seg, off, size, cb)` → with `leader_sync`, a local `msync` of both copies
  (`pg/pg_wal`, and the leader's own copy in `raft/wal`, which counts towards the quorum), then
  `exec(WalCommand{flushed_lsn, seg.lsn, FlushOp{pending range}}, h)`: the flush carries the
  writes since the segment's last `WriteOp` or flush (an empty `FlushOp` when there are none, still
  the barrier). Usually one entry and one raft round per fsync. `h` calls `cb(ok)` when the entry
  commits, or `cb(kNoConsensus)` when it gets `kNotLeader`. Taking the pending range and
  appending its entry happen under one lock, so a `FlushOp` always follows the `WriteOp`s of the
  bytes before it.

- `async_timeline(timeline, content)` → `exec(WalCommand{flushed_lsn, TimelineOp{timeline,
  content}})` (§7).

PostgreSQL's fsync, and therefore its commit, returns only once the WAL is on a majority. Entries
are appended in WAL order, so committing a `FlushOp` implies that the `WriteOp`s before it are
committed too. The FUSE layer asks the tracker on every fsync, even when it sees nothing new: the
writes may have been published through a handle that has since closed.

**`flushed_lsn` is fetched from the primary**: it is the last value of
`pg_current_wal_flush_lsn()` read by the status poller (§5), which `NodeWalTracker` puts into
every command. It is not derived from the byte range being written or fsynced. The value is stale
but safe: PostgreSQL reports a flush LSN only after its fsync returned, so whatever the poller read
is already committed. The followers' WAL senders stream up to it (§2.2), so they only lag the
primary by up to one poll interval. When the poller reads a flush LSN that no entry has carried
yet, the tracker appends an entry with it (no WAL): tick entries carry none, so without it the
standbys would stay behind the last flush once the writes stop.

### 2.2 Follower (PostgreSQL is a standby)

- Raft writes the WAL bytes into `raft/wal` as it accepts requests.
- The WAL sender reads `raft/wal`. `RaftObserver::applied()` calls
  `WalSender::advance(state.flushed_lsn)` for each connected sender, so the standby receives only
  committed WAL.
- **Why `flushed_lsn` and not the end of the `WriteOp`s:** PostgreSQL writes WAL in whole pages,
  and a page can end with a record whose space is reserved but whose body hasn't been copied yet.
  The FUSE parser only walks record lengths (it checks no CRC), so a `WriteOp` can end inside such
  an incomplete record (see [wal_fuse.md](wal_fuse.md#25-walfusesegment-parse-publish-and-fsync)).
  The `WriteOp` and `FlushOp` ranges therefore don't tell how far the valid, flushed WAL reaches.
  Only PostgreSQL's own flush LSN (`pg_current_wal_flush_lsn()`, polled from the primary, §2.1)
  marks a point the standby may safely stream to. The cost is that standbys lag
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

### 2.3 WalSenderServer on a Unix-domain socket

The `WalSenderServer` listens on a Unix-domain socket, as it does today. Only the local standby
connects, so there is no network exposure and no authentication to set up.

- It binds `socket_dir/.s.PGSQL.<port>`. If that file already exists it tries the next port number
  until one is free.
- The standby's `primary_conninfo` is `host=<socket_dir> port=<bound_port()>`. The node writes it
  each time it starts the standby, because `bound_port()` can differ from the port it asked for.

## 3. Role transitions

There is no control loop. Raft knows exactly when the role changes, so it tells the node through
hooks, and the node reacts.

### 3.1 Hooks and the transition thread

`RaftObserver` has four hooks:

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
new role (under `role_mtx_`) and wakes the node's **transition thread** (`Node::transitions`),
which runs transitions one at a time. If the role flips again while a transition runs (leader →
follower → leader), the thread acts only on the latest role when it gets to it. A stale
transition is skipped, not queued.

Two things happen **inside** `stepped_down` itself, because they are cheap, must not wait, and
must not be delayed by the transition thread:

- **The tracker is fenced** (`NodeWalTracker::Mode::kFenced`): nothing PostgreSQL writes after
  this point is published, and every fsync fails with `kNoConsensus`. Follower mode would not do:
  its fsync is a local msync that succeeds, so a backend still running could have a commit
  acknowledged on the old primary.
- **PostgreSQL is killed** (`PgInstance::kill()`): SIGSTOP the postmaster so it forks nothing
  more, SIGKILL each of its children and their process groups, then the postmaster. Every
  PostgreSQL child calls `setsid()`, so a signal to the postmaster's process group would not reach
  them, and a backend in the middle of a query only notices the postmaster's death at its next
  check. `kill()` takes no lock, so it is safe with the server lock held.

### 3.2 Promotion (`became_leader`)

On the transition thread:

The standby replays **all** the WAL the node has, to the end. `flushed_lsn` is not used here: it
is a polled value and always stale, so it can stop short of WAL the old primary had already
acknowledged. At full leadership every entry in the log is committed, so everything in `raft/wal`
is committed WAL.

1. Advance the WAL sender to the **end of the WAL in `raft/wal`**: `state.wal_end`, the highest
   end (segment + offset + size) of the applied WAL bytes (`WriteOp`s and `FlushOp`s). It is a
   maximum, not the end of the last entry, because a rewritten part of a page is sent again over
   earlier offsets. A `WriteOp`
   can end inside an incomplete record (§2.2), so `wal_end` can be past the last valid record;
   that is fine here, because PostgreSQL stops replaying at the last complete record.
2. Wait until the standby has received all of it: its `StandbyStatusUpdate` (the WAL sender's
   `StatusCb`, which `WalSenderServer` passes on) reports
   `StandbyStatus::flushed_lsn >= wal_end` (what the standby has written to its disk). This is
   a bounded wait within the transition, with a timeout, not a loop. On timeout the node steps
   down, so that another node can take over.
3. Switch the tracker to leader mode, before PostgreSQL writes any new WAL.
4. Promote with `PgInstance::promote()` (the `promote` file, then SIGUSR1, as `pg_ctl promote`
   does), and wait for `pg_is_in_recovery()` to be false. On promotion PostgreSQL replays
   everything it received, up to the last complete record, then ends recovery and starts a new
   timeline (§7). An incomplete record at the very end is discarded, and the new timeline starts
   after the last complete one.
5. **The new timeline is published by the FUSE layer**, not by this step: on promotion
   PostgreSQL installs `<timeline>.history` in `pg_wal` with a rename, `WalFuseOps::op_rename`
   passes it to `WalTracker::async_timeline`, and `NodeWalTracker` (now in leader mode) appends
   `exec(WalCommand{…, TimelineOp{timeline, content}})` (§7). The node waits until that entry has
   committed. If it fails (`kNotLeader`), the node is no longer the leader, and `stepped_down`
   takes over.
6. Once PostgreSQL is out of recovery, `pg_isready` succeeds and the `TimelineOp` is committed,
   call `set_serving(true)`. The proxy then routes to this node. Clients only reach the new
   primary once a majority knows its timeline.

### 3.3 Demotion (`stepped_down`)

The old leader lost leadership. **Whenever a node loses leadership, it kills its PostgreSQL**, at
once, in the hook (§3.1): `make_follower()` has already failed every pending `exec()` handler
with `kNotLeader`, the tracker is fenced, and every PostgreSQL process is SIGKILLed. No client of
the old primary is served after this point.

On the transition thread:

1. `set_serving(false)`.
2. Reap the killed postmaster (`PgInstance::stop`), which also releases the System V shared
   memory and semaphores it left (`PgInstance::release_ipc`). (Demotion is implemented: `Node::demote` /
   `restart_as_standby`; `node_test` hands the leadership over and checks the old primary comes
   back as a standby.)
3. **Rebuild `pg/pg_wal` from `raft/wal`** (`rebuild_pg_wal`, `src/node/wal_rebuild.h`;
   implemented). The old primary's `pg_wal` can hold WAL that it wrote but never committed (its
   fsync failed), and even `raft/wal` can (the leader copies bytes in before they commit), so the
   committed end is `max(state.wal_end, the restored backup's end rounded up to its segment's
   end)`: the backup's WAL is committed, and `pg_backup_stop` ends its last segment with an
   `XLOG_SWITCH` record past the backup's End-LSN, which every standby replays. With FUSE
   unmounted (`WalFuseOps` caches mmaps by LSN), each segment or history file of `pg/pg_wal`:
   - not in `raft/wal`: deleted;
   - on a later timeline than the committed end: deleted; on an earlier one: raft's copy;
   - on the committed end's timeline: raft's copy if wholly before the end; raft's copy with
     everything from the end zeroed if it holds the end; deleted if after it;
   - `.X` (unlinked), `.partial` and `xlogtemp.*` files: deleted.
   Otherwise the standby would replay uncommitted WAL on startup.
4. Restart it as a standby: `standby.signal`, and `primary_conninfo` pointing at the local
   `WalSenderServer` port. Switch the tracker to follower mode just before. It replays its own
   `pg_wal` up to the committed point, then streams the rest from `raft/wal`. Restarting in the same
   data directory also frees what the killed processes left: PostgreSQL's System V shared memory
   segment and semaphores outlive a SIGKILL, and the next postmaster in the same directory finds
   them by the directory's inode and removes them.

Why this is safe without `pg_rewind`:

- The old primary never acknowledged WAL that wasn't committed, because its fsync waits for a
  quorum.
- PostgreSQL writes a data page only after the WAL covering it has been flushed, which here means
  committed. So the data directory never runs ahead of the committed WAL.
- Step 3 removes the uncommitted WAL from `pg/pg_wal`; `raft/wal` gets the new leader's bytes
  through raft.

`pg_rewind` is not needed (decision 2 in §9).

### 3.4 Startup and PostgreSQL exits

Two cases are not raft events, so no hook fires for them:

- **Startup.** The node restores a backup if it has no usable data directory (§4.2), then starts
  PostgreSQL as a standby by itself. If raft later reaches full leadership, `became_leader`
  promotes it.
- **PostgreSQL exits on its own**, without a role change (an OOM kill, a postmaster crash).
  **Not handled yet** (see §10): nothing watches the postmaster. The intended design: a watcher on
  the `Process` (`wait()`) wakes the transition thread, which restarts PostgreSQL in the node's
  current role: as a standby on a follower; on the leader, it steps down (or restarts as a standby
  and promotes again, §3.2).

## 4. Bootstrap, joining and backups

A node never copies a data directory from a peer. Every PostgreSQL data directory comes from the
cluster's **backup store** ([backup.md](backup.md)): a `BackupStore` whose first implementation,
`DirBackupStore`, keeps one directory per backup; an S3 implementation comes later. Backups are
named after their end LSN, so the latest one is always easy to find.

### 4.1 Creating a cluster: `pg_raft_initdb` (implemented)

`pg_raft_initdb` is a separate command (`src/node/pg_raft_initdb.cc`, `init_cluster()` in
`src/node/init.h`), run once, like `initdb`. It does not start a raft server:

1. `initdb` into a scratch directory (with data checksums), then `write_config` and `write_hba`.
2. Start PostgreSQL on it, on its own, without FUSE: this instance is not part of the cluster, its
   WAL only has to end up in the backup.
3. `pg_basebackup -X stream`, which includes the WAL the backup needs to be consistent. Verify it
   (`verify_backup`).
4. `store.begin(node, now)`, then `store.put(pending, backup, end_lsn)` (the name,
   [backup.md](backup.md#2-naming)). `pg_raft_initdb` runs before any node, so it uses node id 0. PostgreSQL runs on
   `--port` (default `servers[0].pg_port`) while the backup is taken.
5. Stop PostgreSQL and delete the scratch directory.

`pg_raft_initdb` refuses to run if the store already holds a backup, so that a second cluster can't be
created in the same store by mistake.

### 4.2 Starting a node

On start, before raft starts, the node checks whether it has a usable PostgreSQL: a data
directory, and `RaftState::needs_backup` not set. It downloads a backup **only if it doesn't**. A
node that restarts keeps its own data directory; replaying from there is faster than fetching
the latest backup, especially from S3.

Without a usable data directory:

1. `store.latest()`, then `store.get()` into a scratch directory, then `verify_backup`, and check
   the system identifier ([backup.md](backup.md#6-checks)).
2. Copy it into `data/` with `PgInstance::restore`, which **keeps the directory** and replaces its
   contents, and move its `pg_wal` contents into `pg/pg_wal` (the FUSE backing directory), as
   `create_raft_wal` does. `data/pg_wal` stays empty for the FUSE mount. The data directory is
   never replaced by a new one: PostgreSQL keys its System V shared memory and semaphores by the
   directory's inode, and what a killed PostgreSQL left in a directory that is gone leaks until
   reboot (macOS's System V limits are low).
3. Clear `needs_backup` (`set_needs_backup(false)`, §4.4).

Then, in every case: mount FUSE, start PostgreSQL as a standby (`standby.signal`, and
`primary_conninfo` pointing at the local `WalSenderServer`), and start raft.

**At bootstrap** every node restores the backup `pg_raft_initdb` made, and every raft log is empty. The
first leader reaches full leadership at once, with `wal_end = {}`, so it promotes its standby
straight away (§3.2) and publishes timeline 2. The other standbys follow it through their WAL
senders.

### 4.3 Recovering from `needs_backup`

Implemented (`Node::on_installed` / `restore_and_restart`; `node_recovery_test` deletes a
follower's whole directory and checks it restores and catches up). The `installed` hook kills
the standby at once; the transition thread restores; the backup thread asks again while
`needs_backup` stays set (no backup recent enough yet).

A node whose log was replaced by an InstallSnapshot has `needs_backup` set: `raft/wal` holds only
the snapshot's segments, and the WAL before them is missing. The node:

1. Kills PostgreSQL if it is running (`PgInstance::kill`, then `stop` to reap it).
2. Restores the latest backup as in §4.2, into the same data directory.
3. Starts PostgreSQL as a standby. It replays the backup's WAL from `pg/pg_wal`, then streams the
   rest from `raft/wal`.

**There must be no gap:** the cluster keeps every raft entry and WAL segment from the segment of
the latest backup's end LSN on ([backup.md](backup.md#5-wal-retention-and-raft-compaction)), so
after restoring it the node receives all the WAL it is missing through ordinary AppendEntries. If
the node sees an older backup than the leader compacted to (an S3 listing can lag), it keeps
`needs_backup` set, logs it, and retries.

### 4.4 Raft change: `set_needs_backup` (implemented)

A node without a usable PostgreSQL must not become leader: it would have nothing to promote.
`needs_backup` already says that, so there is no separate "can lead" flag:

- `RaftServer` doesn't start an election while `state.needs_backup` is set. It still votes and
  replicates.
- `set_needs_backup(bool)` sets or clears the flag and persists the state. The node sets it when
  it starts without a data directory, and clears it once a backup is restored (§4.2).
- InstallSnapshot sets it, as it does today.

A PostgreSQL that crashed and is being restarted (§3.4) doesn't set it: the data directory is
still there.

## 5. Status poller

Every 500 ms, over `PgClient` on loopback:

```sql
SELECT CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_flush_lsn()::numeric END,
       CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()::numeric ELSE NULL END;
```

The result goes to:

- `RaftServer::set_pg_status()`, which reports it in `StatusResponse`;
- on the leader, `NodeWalTracker`, as the `flushed_lsn` of the commands it appends (§2.1).

## 6. Shutdown

On SIGTERM or SIGINT (`Node::shutdown`, implemented):

1. Stop the transition thread and the status poller.
2. Stop PostgreSQL (Fast) **first**. On the primary its shutdown checkpoint still commits through
   raft, since the node still leads. A clean stop also releases PostgreSQL's System V shared memory
   and semaphores; a kill leaves them behind, and only a postmaster in the same data directory
   reclaims them (macOS allows 32 shared memory segments in all).
   A leader without a quorum (the last node of a cluster that is shutting down) never commits
   that checkpoint: the checkpointer's fsync waits in FUSE for a commit that never comes. On Linux
   a process waiting on a FUSE request the daemon has read cannot be killed, so it keeps
   PostgreSQL's output pipes open and `PgInstance::stop()` (which waits for them) never returns.
   (macFUSE times such requests out.) So the stop runs on its own thread, and if PostgreSQL has
   not stopped after 12 s, raft is shut down first: `close()` fails the pending `exec`s with
   `kNotLeader`, the fsync fails, the checkpointer PANICs and PostgreSQL exits.
3. Raft shutdown, which hands the leadership over; `stepped_down` finds nothing left to kill.
4. Stop the WAL sender and its senders, unmount FUSE.

## 7. Timelines

`pg_promote()` starts a new timeline. The pieces (implemented, except `NodeWalTracker`):

- **FUSE:** PostgreSQL writes the history file as `xlogtemp.<pid>` and renames it to
  `<timeline>.history`. `WalFuseOps::op_rename` reads the installed file and calls
  `WalTracker::async_timeline(timeline, content)`; on the leader, `NodeWalTracker` appends a
  `TimelineOp{timeline, content}`.
- **Raft:** every node that applies a `TimelineOp` writes the history file into `raft/wal`
  (`write_history_file`, atomic).
- **WalSender:** learns the timelines from the history files in `raft/wal`, serves
  `TIMELINE_HISTORY`, streams `START_REPLICATION … TIMELINE t` up to `t`'s switch point, then
  ends the stream with CopyDone and the `(next_tli, next_tli_startpos)` row, so the standby moves
  to the next timeline ([wal_sender.md](wal_sender.md#22-walsender-one-replication-connection)).
- **The new timeline's first segment:** PostgreSQL creates it by copying the old segment up to
  the switch point through an `xlogtemp` file, which FUSE doesn't replicate, so in `raft/wal`
  those bytes are zeros. `WalSender` reads any bytes before a timeline's start point from the
  parent timeline's segment, which holds the same bytes.
- **Order:** the history file is installed before PostgreSQL writes any WAL on the new timeline,
  so the `TimelineOp` is in the log before the `WriteOp`s of the new timeline's segments. A
  follower's WAL sender only switches its standby to the new timeline once the history file is in
  `raft/wal`.
- **InstallSnapshot:** a node whose log is replaced never applies the `TimelineOp`s from before
  the snapshot (they may be compacted away). So the InstallSnapshot carries the leader's latest
  history file (`InstallSnapshot::timeline`), which the follower writes into `raft/wal` before it
  installs the entries. The latest history file lists every earlier timeline, so one file is
  enough.
- Segment names carry the timeline. The entries already carry `LogSeqNumber{timeline, lsn}`.

## 8. Testing

The tests need PostgreSQL and FUSE, and skip if either is absent. They run nodes in one process
(3 raft servers, 3 FUSE mounts, 3 PostgreSQL).

| Test | What |
|---|---|
| `init_test` | `backup_end_lsn()` from a manifest; `init_cluster()` stores one verified backup, which restores and starts as a primary with the configured settings; a second init is refused |
| `node_wal_tracker_test` | `NodeWalTracker` against a 3-node raft ensemble: leader mode replicates bytes and completes the fsync on commit; on a follower it fails with `kNoConsensus`; follower mode adds nothing; a timeline's history file reaches every node |
| `wal_rebuild_test` | `rebuild_pg_wal`: raft's copies kept, the end's segment zeroed from the end, uncommitted segments, timelines and leftovers removed; the backup-end case |
| `node_test` | steady state and hand-over: pgbench through the proxy on the primary (timeline 2), the standbys converge; the primary steps down, another promotes to timeline 3, the old one is demoted, all converge (1200 rows) |
| `node_backup_test` | with `backup_interval_secs = 15` a standby (never the primary) stores a backup on timeline 2; no lease is left; the backup restores and holds the 100000 accounts |
| `node_recovery_test` | backups move `keep_from` and the leader compacts; a follower's directory is deleted, it restarts, restores, gets an InstallSnapshot of the oldest entries and catches up (`needs_backup` cleared) |

## 9. Decisions

1. **Bootstrap: `pg_raft_initdb`.** It creates the first backup in the backup store; every node
   starts by restoring the latest backup (§4).
2. **Demoting an old primary: no `pg_rewind`.** The old primary is restarted as a standby after its
   `pg/pg_wal` is rebuilt from `raft/wal` (§3.3).
3. **WalSenderServer transport: a Unix-domain socket** (`socket_dir/.s.PGSQL.<bound_port()>`, §2.3).
4. **Backups after bootstrap: a standby runs `pg_basebackup` against itself** when
   `backup_interval` or `backup_wal_size` since the latest backup is exceeded; the backup's name
   carries its end LSN and time, so every node computes from a listing when the next one is due
   and how much WAL to keep (`keep_from`) ([backup.md](backup.md#4-taking-backups)).
5. **Which standby backs up: staggered deadlines.** Node `i` of `N` backs up once
   `backup_interval * (1 + i/N)` (or `backup_wal_size * (1 + i/N)` of WAL) has passed since the
   latest backup, unless another node holds a live lease; the primary skips its turn
   ([backup.md](backup.md#41-which-node-backs-up-staggered-deadlines)).
6. **InstallSnapshot: the oldest entries**, trimmed to 2 WAL segments, with those segments' full
   contents; the follower restores the latest backup, whose end is in those segments
   ([backup.md](backup.md#5-wal-retention-and-raft-compaction)).

## 10. Open items

1. The node tests are slow (about 2 minutes each) and can time out when run in parallel with each
   other; run them serially. `node_recovery_test` has failed once at the restarted node's
   `start()` in a parallel run.
2. A killed test run can leak PostgreSQL's System V shared memory (macOS allows 32 segments);
   `ipcs -m` shows them, `ipcrm` removes them.
3. Joining a node that is not in `servers` (membership changes) is not supported.
4. No watcher on the postmaster (§3.4); a promotion that fails after the catch-up step doesn't
   step down; a backup holds `PgInstance::mu_` and delays a promotion; a failed rebuild or restore
   leaves `fuse_` null; `rebuild_pg_wal` deletes compacted segments a demoted primary may still
   need. Details in [`todo.md`](todo.md).
