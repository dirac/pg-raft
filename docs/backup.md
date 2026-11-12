# `backup` — PostgreSQL backup store

Source: `src/backup/` — `backup_store.{h,cc}`, `dir_backup_store.{h,cc}`, `backup_store_test.cc`.

The store holds the PostgreSQL backups of one cluster. A node restores from it when it has no
usable data directory: at bootstrap, when it joins, or after an InstallSnapshot
(`needs_backup`). See [node.md](node.md#4-bootstrap-joining-and-backups).

## 1. Interface

How a backup is written and read is behind an abstract class, so that storage can change without
touching the node:

```cpp
struct BackupInfo {
  LogSeqNumber lsn;   // the backup's end LSN: a restore is consistent from here
  std::time_t  time;  // when the backup was started
  RaftNodeId   node;  // the index of the node that took it
  string name() const;              // see §2; parse_backup_name() is the inverse
};

struct PendingBackup {              // a lease, at most one per node (§4.2)
  RaftNodeId  node;
  std::time_t started;              // the time of the backup's name
  std::time_t refreshed;            // when the node last refreshed it
};

class BackupStore {
 public:
  virtual Result<PendingBackup> begin(RaftNodeId node, std::time_t time) = 0;
  virtual Result<void>          refresh(PendingBackup& pending, std::time_t time) = 0;
  virtual Result<BackupInfo>    put(PendingBackup const& pending, path const& dir,
                                    LogSeqNumber lsn) = 0;   // atomic
  virtual Result<void>          abort(RaftNodeId node) = 0;
  virtual Result<vector<PendingBackup>> pending() const = 0;
  virtual Result<vector<BackupInfo>>    list() const = 0;    // complete backups, oldest first
  Result<std::optional<BackupInfo>>     latest() const;      // list().back()
  virtual Result<void> get(BackupInfo const& backup, path const& dest) const = 0;
  virtual Result<void> remove(BackupInfo const& backup) = 0;
};

// Scheduling (§4) and retention (§5), pure functions:
struct BackupPolicy { seconds interval; uint64_t wal_size; seconds pending_timeout; };
bool backup_due(BackupPolicy const&, unsigned index, unsigned nodes,
                optional<BackupInfo> const& latest, time_t now, LogSeqNumber wal_end);
bool pending_blocks(BackupPolicy const&, vector<PendingBackup> const&, RaftNodeId self,
                    time_t now);
LogSeqNumber keep_from(BackupInfo const& latest);
```

See `src/backup/backup_store.h` for the full comments.

Implementations:

| Class | Storage | State |
|---|---|---|
| `DirBackupStore(root)` | One subdirectory of `root` per backup | Implemented, tested |
| `S3BackupStore` | One key prefix per backup in a bucket | Later |

## 2. Naming

The name carries everything a node needs to decide when to take the next backup and how much WAL
to keep, so that any node can work it out from a listing, without downloading anything:

```
TTTTTTTT-XXXXXXXXXXXXXXXX-SSSSSSSSSS-N
timeline  end LSN         start time  node index
```

The time is seconds since the epoch, and the node index is the taker's index in `servers`, both
in decimal.

The timeline and the 64-bit LSN are fixed-width upper-case hex and the time is a fixed-width
decimal, so the names sort in the same order as `LogSeqNumber` (timeline, then LSN): the latest
backup is the one with the largest name. `BackupInfo` is parsed from the name.

- **End LSN:** WAL retention (§5) and the WAL-size trigger (§4) are computed from it.
- **Time:** when the backup was **started**. The interval trigger (§4) is computed from it. It
  is in the name rather than taken from file or object timestamps, which differ between a
  directory and S3.
- **Node index:** which node took it, so a listing shows who has been taking the backups (a node
  that keeps missing its turn stands out).

The end LSN is the point from which the restored data directory is consistent: the `End-LSN` of
the last entry of `WAL-Ranges` in the backup's `backup_manifest`. Every backup is a
`pg_basebackup` (§4), so every backup has a manifest.

## 3. `DirBackupStore`

```
root/
  00000001-0000000003000028-1762934400-0/       a complete backup (data directory, with pg_wal)
  00000002-0000000005A1F3B0-1762956000-1/
  .pending-3                                    node 3's lease (start time inside, mtime = refresh)
  .tmp-00000002-0000000006000000-1762977600-3/  a put() copying, or one that crashed
```

- `begin` writes `.pending-<node>` (the start time inside), replacing any old one; `refresh`
  sets its mtime; `pending` lists them with their mtimes; `abort` removes one.
- `put` copies into `.tmp-<name>`, fsyncs it, renames it to `<name>`, then removes the pending
  marker. The rename is the commit: names starting with `.` are never listed as backups.
- `latest` lists `root`, ignores dot entries, and returns the largest name.
- `get` copies `<name>` into `dest` (`rsync -a`, like `PgInstance::restore`).
- A leftover `.tmp-*` directory belongs to a node that crashed; that node's next `begin` or
  `abort` removes it. Other nodes leave it alone: it may be a `put` still copying.
  Stale markers are handled by the lease (§4.2).

`root` comes from `config.toml` (`backup_store = "/path"`); an `s3://bucket/prefix` URL would
select `S3BackupStore` later.

**S3:** the same commit rule applies. Upload every file first and the manifest object
(`<name>/backup.json`) last; `list` only returns backups that have a manifest. Pending
markers are objects `pending/<node>`, rewritten on each refresh; their `LastModified` is the
refresh time.

## 4. Taking backups

Implemented in `Node` (`maybe_backup`, checked every `NodeOptions::backup_check_interval`,
60 s by default); `node_backup_test` covers it.

Every backup is taken with **`pg_basebackup -X stream`**, both by `pg_raft_initdb` and afterwards. There is
no other backup method:

- It is a hot backup: the PostgreSQL being backed up keeps running.
- It writes a `backup_manifest`, which gives the exact end LSN for the name (§2) and lets
  `pg_verifybackup` check the backup.
- `-X stream` includes the WAL from the backup's start to its end, so the backup is consistent on
  its own.

After bootstrap, backups are taken by a **standby**, never by the primary: the standby runs
`pg_basebackup` against itself, over loopback, so the I/O of the backup is on the standby and the
primary is not loaded. PostgreSQL supports backups from a standby; the WAL that `-X stream` needs
comes from the standby's own walsender, which reads its `pg_wal` (the FUSE mount).

Two parameters in `config.toml` decide when the next backup is due, and a third bounds a
backup in progress:

| Parameter | Meaning |
|---|---|
| `backup_interval` | Maximum time since the latest backup (for example `6h`) |
| `backup_wal_size` | Maximum WAL since the latest backup (for example `4GiB`) |
| `pending_timeout` | How long a pending marker counts without being refreshed (§4.2; default `2m`) |

The WAL size bounds how much WAL the cluster keeps (§5) and how long a restored node needs to
catch up; the interval bounds a quiet cluster. Which node acts on them, and when, is §4.1.

The standby that takes the backup:

1. Runs `pg_basebackup -h 127.0.0.1 -p <pg_port> -D <scratch> --checkpoint=fast -X stream`
   against its own PostgreSQL (`PgInstance::backup`).
2. Verifies it (`verify_backup`, which runs `pg_verifybackup`), reads the end LSN from
   `backup_manifest`, and `put`s it with that LSN. The time in the name is when it called `begin`
   (§4.2).

Things that follow from backing up a standby:

- **The start can wait.** On a standby `--checkpoint=fast` requests a restartpoint, and a standby
  can only make one at a checkpoint record it has received from the primary. The backup can wait
  up to the primary's `checkpoint_timeout` (5 min by default) to start.
- **Promotion aborts it.** If the standby is promoted during the backup, `pg_basebackup` fails.
  The node discards the scratch directory; the next due check tries again on another standby.
- **Requirements:** `full_page_writes = on` on the primary (the default), `hot_standby = on` and
  `max_wal_senders > 0` on the standby (both defaults), and an HBA rule that lets a loopback
  `replication` connection in (see §7).

### 4.1 Which node backs up: staggered deadlines

There is no election and no message for this. Every node computes its own deadline from the
latest backup's name, and the deadlines are staggered by node, so the first node in line takes
the backup and the others only step in if it doesn't.

With `T = backup_interval`, `W = backup_wal_size`, `N = servers.size()`, `i` the node's index in
`servers` (0 … N-1), and `L = store.latest()`, node `i` starts a backup when either

```
now - L.time                  >= T + i * T / N     (time)
state.wal_end.lsn - L.lsn.lsn  >= W + i * W / N     (WAL)
```

holds, **and** no other backup is pending (§4.2).

Example: `T = 5h`, `W = 5GB`, 5 nodes. After a backup started at `t`, node 0 backs up at `t + 5h`
(or after 5 GB of WAL), node 1 at `t + 6h` (6 GB) if no backup has appeared by then, node 2 at
`t + 7h` (7 GB), and so on. A node that is down, or whose turn comes while it is the primary,
misses its deadline, and the next node covers it `T / N` later.

- **The primary never backs up.** A node whose PostgreSQL is the primary (the raft leader) skips
  its deadline. So does a node with `needs_backup` set, or whose standby is not running.
- **Checked periodically.** Each node lists the store every minute, and whenever its WAL since
  the latest backup crosses its threshold. The listing is names only.
- **Clocks.** The time rule compares wall clocks across nodes. They need to agree to well within
  `T / N` (an hour in the example); NTP is enough.
- **Cost of a miss.** Each missed deadline delays the backup by `T / N` and lets `W / N` more WAL
  accumulate. In the worst case (every node but the last unavailable) the WAL since the latest
  backup reaches `W * (2N - 1) / N`, just under `2W`.

### 4.2 A backup in progress: a refreshed lease

A backup takes time: `pg_basebackup` of a large database can take longer than `T / N`. Without
a marker, the next node in line would see no new backup at its deadline and start a second one.

A plain marker has the opposite problem: a node that writes it and then dies leaves a **false
pending marker**, and the other nodes would wait for a backup that never comes. So the marker is a
**lease** that the node keeps alive:

1. `store.pending()`: skip if any marker other than this node's own was refreshed less than
   `pending_timeout` ago (for example 2 minutes).
2. `store.begin(node, now)`.
3. Run `pg_basebackup`, then `verify_backup`, refreshing the marker every `pending_timeout / 4`
   (`store.refresh`) for as long as they run.
4. `store.put(pending, dir, end_lsn)` on success, `store.abort(node)` on failure.

A false marker therefore stops counting `pending_timeout` after its node died, not `T` later, and
the next node in line takes over at its own deadline. There is at most one marker per node, so a
node that restarts removes its own marker (`store.abort(node)`) before it does anything else.

The lease compares the refresh time, from the store's clock (file mtime, or S3 `LastModified`),
with the node's clock. `pending_timeout` must be well above the clock skew; with NTP, minutes are
plenty.

Two nodes can still start within the same few seconds (both listed before either called `begin`).
That only costs a duplicate backup; the larger name wins.

## 5. WAL retention and raft compaction

A node that restores the latest backup replays the backup's own WAL, then streams the rest from
its `raft/wal`, starting at the segment that holds the backup's end LSN (a standby requests
streaming from the start of a segment). So the cluster must keep, from that segment on:

- the WAL segments in `raft/wal`, and
- **the raft log entries** that carry them, so that a node behind can receive that WAL through
  ordinary AppendEntries.

Every node computes the same bound from the store listing:

```
keep_from = start of the segment holding latest().lsn
```

**Invariant: every entry in the raft log has its WAL in `raft/wal`.** An entry carries no WAL
bytes, only a segment, an offset and a size, so an entry whose segment is gone is useless: it
could not be sent to a follower or streamed to a standby. Segments are only deleted once no entry
refers to them, in two independent steps:

1. **Raft compaction** (`RaftServer::compact`) deletes the entries before the first entry whose
   `segment >= keep_from`. In the same LMDB transaction it records the oldest entry left in
   `RaftState` (below). `logs_` and `meta_` are in the same environment, so this is one commit:
   the recorded oldest entry is never ahead of the entries actually in the log, even after a
   crash.
2. **WAL deletion** runs on its own thread, at its own pace. It reads the recorded oldest
   segment and deletes the segments wholly before it from `raft/wal` (and the `.X` segments from
   `pg/pg_wal`). It needs no coordination with compaction: whatever it deletes, no entry refers
   to.

`RaftState` gets two per-node fields (planned):

| Field | Meaning |
|---|---|
| `first_idx` | the oldest entry in the log |
| `first_segment` | the segment of the oldest entry that has one (ticks have none); WAL deletion keeps this segment and everything after it |

They are the node's own, like `needs_backup`: each node compacts on its own schedule, so
`check_consistency` doesn't compare them. `install_snapshot` sets them too, to the first entry it
installs.

- `keep_old_logs` no longer applies: it would keep entries older than `keep_from`. Everything
  since the latest backup is kept anyway, which is what a lagging follower needs.
- Compaction runs after `compaction_lag` applied entries (as today) and whenever `keep_from`
  moves. The node passes `keep_from` to raft (`set_wal_keep_from(LogSeqNumber)`, a new
  `RaftServer` call) each time it sees a newer backup; raft does not know about the store.
- Every node applies both steps to its own log and `raft/wal`, so the invariant holds on
  followers as well as on the leader.
- Segments compare as `LogSeqNumber` (timeline, then LSN), so after a promotion the old
  timeline's segments go once the oldest entry is on the new timeline.

If no new backup is taken, nothing is compacted past the latest one and the log and the WAL grow;
the node logs a warning once the WAL since the latest backup exceeds twice `backup_wal_size`.

**Consequence for InstallSnapshot.** A follower only falls behind the leader's compaction point if
it is behind the latest backup, so it must restore that backup anyway, and after restoring it
needs the WAL from `keep_from` on. So an InstallSnapshot sends the **oldest** entries the
cluster keeps, not the latest:

- The leader sends its oldest entries, trimmed to at most **2 WAL segments**, with the full
  contents of those segments. By the invariant above, the leader still has those segments.
- The follower replaces its log with them and sets `needs_backup`. Those entries give it a real
  starting point (an index and a term) for log matching, so AppendEntries continues right after
  them and brings the rest of the WAL.
- It restores the latest backup, whose end LSN is in the first of those segments, so there is no
  gap between the backup and `raft/wal`.

Today's InstallSnapshot sends the **latest** `snapshot_entries` entries; that would leave a gap
between the backup and those entries, so it changes (decision 7 in
[node.md](node.md#10-open-decisions)).

## 6. Checks

- **One cluster per store.** Every backup in a store must come from the same `initdb`. `get`
  checks the restored `pg_controldata` system identifier against the one the node expects (from
  its own data directory if it has one, or from the latest backup) and refuses a mismatch.
- **Verify before use.** After `get`, the node runs `PgInstance::verify_backup` before it starts
  PostgreSQL on the restored directory.

## 7. Open items

1. **Code changes in `process`:** `PgInstance::backup` runs `pg_basebackup` on a standby too, and
   keeps it running (done).
   `write_hba`'s defaults need `host replication all 127.0.0.1/32 trust` (and `::1/128`). Their
   order is also wrong: the `0.0.0.0/0 scram-sha-256` rule comes before the loopback `trust` rule,
   so it matches loopback first. Today this is masked by `initdb`'s own loopback rules at the top
   of `pg_hba.conf`.

2. **Backup retention.** Which backups to keep (for example the latest N), and removing them with
   `remove`. WAL is kept only from the latest backup (§5), so an older backup can only be restored
   if its WAL is still there, for example from an archive.
3. **Margin.** Keeping WAL from the latest backup only means a corrupt latest backup leaves nothing
   to restore from. Keeping WAL from the second-latest backup would cost one more
   `backup_wal_size` of WAL.
