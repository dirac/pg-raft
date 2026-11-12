# raft

Raft over the WAL stream. The primary's WAL writes and fsyncs become log entries, and the WAL bytes
travel with them to the followers, so that a commit is acknowledged only once its WAL is durable
on a majority. See `docs/raft.md` for the full design.

## Entries and state

- **`Entry`** (`raft_types.h`) — `{time, term, WalCommand cmd}`. The primary generates the
  `WalCommand`s itself (`wal_types.h`): `WriteOp` / `FlushOp` / `OpenOp` / `CloseOp` /
  `TimelineOp` on a segment, with the primary's `flushed_lsn`. A tick entry has a `monostate`
  command.
- **WAL bytes** — a `RaftRequest` carries up to two `MemBlock`s (a segment, offset and size). Their
  bytes are copied out of the leader's segment mmap and sent as the attached buffer of the message.
  A follower writes and msyncs them only once it has accepted the request.
- **`RaftState`** (`raft_state.h`) — the state machine: `log_idx`, `timestamp`, the committed
  `flushed_lsn` (how far a standby may replay), `needs_backup` (set by InstallSnapshot), and
  `wal_end` (the highest end of the applied `WriteOp`s, how far a new leader's standby replays
  before promotion).

## RaftLog

An LMDB log with xxhash32 page checksums and 16 KiB pages. Commits are synced by a background
thread, or each one with `sync_commits`; `no_sync` is for tests.

| File | Contents |
|---|---|
| `term.lmdb` | current term and vote |
| `logs.lmdb` | entries keyed by `(idx, term)`, and the `RaftState` |

Operations: `append_entries` (on the leader after the last entry; on a follower after a matching
entry, replacing the rest), `entries_from`, `latest_entries`, `get_term`,
`save_snapshot(state, keep)` (compaction), and `install_snapshot(log_idx, entries, state)`.

## RaftServer

`RaftServer(id, acceptor, config, dir, wal_dir, options, applier)`. Instantiate it with
`Server::create<RaftServer>(...)`.

- **Elections:** gathering terms before voting, randomized timeouts, persisted votes, and two-phase
  leadership. A candidate with votes becomes the leader only once it has committed everything it
  inherited.
- **`exec(WalCommand, handler)`** — leader only. The handler gets the index when the entry
  commits, or `ErrorCode::kNotLeader`. It runs with the server lock held, so it must not call back
  into the server.
- **`RaftObserver`** — `applied(idx, entry, state)` for each applied entry (for example to
  advance the WalSender to `state.flushed_lsn`), `installed(state)` after a snapshot, and the role
  changes `became_leader(term)` (full leadership) and `stepped_down(term)`. Called with the server
  lock held.
- **`set_needs_backup(bool)`** — sets and persists `RaftState::needs_backup`; while it is set the
  node doesn't start (or take) an election.
- **`segment(lsn)`** — a segment in `wal_dir`, opened through one `WalSegmentCache`. A missing
  segment is created, zero filled.
- **`stepdown()`** — hands the leadership over through `RequestElection`. It is also used on
  shutdown, and is called off if no peer takes over.
- **Compaction** — after `compaction_lag` applied entries, every node, the leader included, saves
  the state and keeps `keep_old_logs` older entries.
- **InstallSnapshot** — the latest `snapshot_entries` entries plus the full contents of the at most
  two segments they write. The follower's log is replaced and `needs_backup` is set: it then needs
  a PostgreSQL backup (through the client channel).
- **Node hooks** — `set_serving(bool)`, `set_pg_status(primary_flushed_lsn, standby_applied_lsn)`,
  `set_backup_provider(fn)`.
- `raft_log()` is the log accessor (`log()` would hide the global logger).

`Options` (`options.h`): election timeout, reconnect interval, connection timeout, max entries per
request, tick seconds, snapshot entries, keep old logs, compaction lag, `log_req_response`,
`sync_commits`, `no_sync`.

## Connections

- **`RaftPeerConnection`** — outbound to each peer at `servers[i].ha_port`, with the handshake
  identity `Raft:<id>`. It sends this node's requests and reads the responses.
- **`RaftFollowerConnection`** — inbound from a peer: reads its requests with their WAL bytes and
  answers.
- **`PeerConnectionMgr`** — one slot per node, reconnected every `reconnect_interval_msecs`.
- **Client channel** (`client_types.h`) — any identity other than `Raft:*` gets a
  `RaftClientConnection`. The client side is `RaftServerConnection`.
  - The heartbeat status byte is `0x3` for the leader when serving, `0x1` for the leader otherwise,
    and `0` for any other node.
  - `StatusRequest` returns the leader flag, the applied index and the PostgreSQL LSNs.
  - `SnapshotRequest` returns a `SnapshotResponse` followed by the backup files;
    `RaftServerConnection::backup(dir, applied_idx)` receives them into `dir`.
  - The proxy uses the channel to find the leader. Nodes restore backups from the backup store
    (`docs/backup.md`), not over this channel, so `SnapshotRequest` and `set_backup_provider` are
    unused by the node design.

## Tests

- `raft_log_test` — fault injection (every LMDB call fails in turn, and no transaction or cursor
  leaks), plus the log operations.
- `raft_ensemble_sanity_test`, `raft_ensemble_leader_stopped_test`, `raft_ensemble_stepdown_test`,
  `raft_ensemble_snapshot_test` — ensembles in one process (`RaftTestService`), with a `Writer`
  that plays the primary (WAL bytes into the leader's segments, then `exec()`).
  `check_consistency` verifies log matching, identical states, and identical WAL bytes for every
  acknowledged write.

## Not yet done

The node process that ties `wal_fuse`, `wal_sender`, `process` and `status` to the raft server;
fetching and reconciling the PostgreSQL backup after a snapshot; hooks for `OpenOp`, `CloseOp` and
`TimelineOp`; WAL retention.
