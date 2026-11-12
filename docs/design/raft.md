# `raft` — Consensus over the WAL stream

Source: `src/raft/` — `raft_types.h`, `wal_types.h`, `client_types.h`, `request.h`,
`options.h`, `raft_log.{h,cc}`, `raft_server.{h,cc}`, `raft_peer_connection.{h,cc}`,
`raft_follower_connection.{h,cc}`, `peer_connection_mgr.{h,cc}`,
`raft_client_connection.{h,cc}`, `raft_server_connection.{h,cc}`, `match_indexes.{h,cc}`,
`vote_tally.{h,cc}`.

## Build status

`raft` is **not** in `src/Jamfile` (`build-project raft` is missing), and
`raft/Jamfile.jam` does not list `raft_server.cc`. `raft_server.{h,cc}` is partway through being
ported from an earlier ZooKeeper-style Raft. It still references `ZkRequest`, `ZkState`,
`snapshot.h`, `zk_state.h`, `net/ssl_context.h`, `banyandb::net::Server`, and `LOG_INFO` streams.
None of these exist in this repo. The log, peer and follower connections, quorum helpers, and
types are written for pg-raft. This document describes the **target design**, which the types and
connection code already encode, and marks what still has to be ported.

## 1. Purpose

Elect one leader node, whose PostgreSQL runs as the primary, and replicate its WAL to the other
nodes so that:

- A commit is acknowledged to a client only after its WAL is durable on a **majority**.
- A follower's standby replays WAL only up to the **committed** point.
- On leader failure, a node with the most complete log wins the election and promotes its
  standby.

The Raft log does not contain SQL. It contains **WAL file operations**, and the WAL bytes travel
alongside the log entries.

## 2. Data model

### 2.1 Log entry: `Entry { RaftTerm term; WalCommand cmd; }`

```cpp
struct WalCommand {
  LogSeqNumber                 flushed_lsn;   // leader's PG flush LSN when this entry was made
  optional<LogSeqNumber>       segment;       // which WAL segment the op is about
  variant<monostate,           // tick / no-op entry
          FlushOp{off,size},   // fsync of [off, off+size) — the durability barrier
          OpenOp, CloseOp,     // segment lifecycle
          WriteOp{off,size},   // bytes [off, off+size) of `segment` (payload sent out of band)
          TimelineOp{tli, content}> command;   // timeline history file
};
```

`flushed_lsn` is the "safe to stream" mark. When a follower **commits** an entry, it calls
`WalSender::advance(flushed_lsn)`. Because the value comes from polling, it can be stale, but it
is never ahead of what the leader has flushed. That makes it safe.

### 2.2 WAL payload: `MemBlock { lsn, offset, size }`

Log entries carry offsets and sizes only. The WAL bytes go in the same network message:

```
[ 6-word net header: data size, attached size, xxhash32 of each ]
[ RaftRequest (binary archive) ][ block0 bytes ][ block1 bytes ]
```

- The leader computes `mem_blocks(entries)`: for each segment, the union range of all
  `WriteOp`s. `trim()` caps a batch to **at most 2 segments**, which matches
  `inplace_vector<MemBlock,2>`.
- `send_request` attaches `WalSegment::const_buffer(off,size)` for each block as the attached
  buffers of `Connection::write_data` (up to two). These are **zero-copy from the leader's mmap**. The handler
  captures the `shared_ptr<WalSegment>`s so the mapping stays alive until the write completes.
- The follower's `read_data_and_buff()` returns the request and the attached bytes, which are
  checksummed separately.
  `RaftFollowerConnection::write()` copies each block into the local segment
  (`WalSegment::write`) and **msyncs it before** it handles the request. When a follower answers
  `AppendEntries success`, the WAL bytes for those entries are already durable on its disk.

### 2.3 Persistent state: `RaftLog` (LMDB)

| DB file | Keys | Notes |
|---|---|---|
| `term.lmdb` | `"term"` → u64 BE, `"vote"` → u16 BE | `set_current_term` only moves the term forward, and it clears `vote` when it does. `vote(term, node)` succeeds only if `term` is the current term and there is no vote yet, or the existing vote is for the same node. That makes it idempotent. |
| `logs.lmdb` (db `logs_`) | `(idx BE u64, term BE u64)` → text-archived `Entry` | Big-endian keys sort by index. On first open a sentinel `(0,0)` entry is written, so `last_entry()` always succeeds and `prev_log_idx = 0` always matches. |

Operations:

- `append_entries(prev_idx, prev_term, entries, delete_entries)`:
  - When `delete_entries` is set (the follower path), `reset_entries` checks that `(prev_idx,
    prev_term)` exists and deletes everything after it. This truncates a conflicting suffix.
  - Otherwise (the leader path), it requires `last_entry() == (prev_term, prev_idx)`.
  - It then writes the entries in one transaction:
    - On `MDB_MAP_FULL`, the map size grows ×4 and the write is retried.
    - On `MDB_TXN_FULL`, the batch is split in half recursively.
- Reads use cursors: `entries_from(idx, n)` reads forward and `latest_entries(n)` reads
  backward. `entries_from` returns `nullopt` if `idx` has already been compacted, which tells the
  caller to send a snapshot. `get_term(idx)` returns the term at `idx`.
- `delete_older_entries(idx)` is the compaction primitive.
- Every LMDB call goes through a virtual wrapper so `raft_log_test` can inject faults.
- Locking: `wmtx_` serializes writers, `rmtx_` is a shared reader lock, and `mtx_` guards the
  term DB.

## 3. Protocol

### 3.1 Messages (`raft_types.h`)

There is one envelope in each direction, keyed by the variant index:

```
RaftRequest  { term, node_id, variant<monostate /*ping*/, RequestVote, AppendEntries,
                                      InstallSnapshot, RequestElection> request,
               RequestResponseInfo info, inplace_vector<MemBlock,2> data }
RaftResponse { term, node_id, Status status, variant<monostate, RequestVoteResponse,
               AppendEntriesResponse, InstallSnapshotResponse, RequestElectionResponse> response,
               RequestResponseInfo info }
```

- `RequestResponseInfo` is copied from the request into the response. It holds `request_term`
  (used to throw away stale responses), `request_idx` and `request_seq` (used to count
  outstanding requests of each type), `epoch` (round-trip time), and `log` and `respond` flags.
- `Status` rides on **every** response. It holds the follower's `applied_idx`,
  `primary_flushed_lsn`, and `standby_applied_lsn`, which the status poller collects. The leader
  therefore sees cluster-wide replication progress without extra traffic.

### 3.2 Connection topology

```
node A (leader)                                   node B
 PeerConnectionMgr ──async_connect "Raft:A"──▶ Server::new_connection
   RaftPeerConnection  (requests →)                RaftFollowerConnection (← requests, → responses)
   handle_response(...)                             handle_peer_request(...)
```

Every node opens one **outbound** `RaftPeerConnection` to each peer. It uses these to send its
own requests and read the responses, which arrive in order. It receives requests from peers on
**inbound** `RaftFollowerConnection`s. `PeerConnectionMgr` keeps one slot per node and
reconnects closed slots every `reconnect_interval` (2 s), with a CAS on
`last_connect_attempt_` so only one attempt runs at a time. When a peer connects to us,
`reset_last_connect_attempts()` triggers an immediate reconnect back to that peer.
`post(func)` fans an action out to every live connection.

Identities in the 64-byte handshake (see [net.md](net.md)):

- `"Raft:<id>"` creates a follower connection.
- Any other identity creates a `RaftClientConnection`.

### 3.3 Startup: gathering terms

A node that restarts with `current_term == 0` must not vote until it has learned the current term
from a majority. Otherwise it could grant a vote in a term it has already seen. `gather_terms()`
sends **pings** (`monostate` requests) to peers. Each response updates the current term and marks
that peer in `votes_`. When a majority has answered, `can_vote_` is set. After that, `periodic()`
runs. A node that restarts with a persisted term > 0 can vote immediately.

### 3.4 Election

- `periodic()` runs every `election_timeout/16`. If a follower has not heard from a leader within
  a randomized timeout, it calls `make_candidate()`. The timeout is uniform over
  `[T + 20·id, 2·(T + 20·id)]` with T = 200 ms. The node-id skew reduces split votes.
- `make_candidate`: `term+1` is persisted, the node votes for itself, `votes_.clear(self, term)`
  runs, and `RequestVote{last_entry}` is sent to all peers. If some peers are unreachable, a
  reconnect is triggered as well.
- Vote granting (`handle_peer_request(RequestVote)`): the node rejects the request if the term is
  stale or `!can_vote()`. It also rejects it if its own `(last_term, last_idx)` is newer than the
  candidate's. Otherwise it calls `log.vote()`, which is persisted and returns at most one grant
  per term. A grant pushes back the local election timer.
- `handle_response(RequestVoteResponse)` ignores responses whose `req_term` is not the current
  term. It then counts the vote:
  - With a majority, the node calls `make_leader`.
  - Once a majority of responses are refusals (`has_lost_election`), it becomes a follower again.

### 3.5 Two-phase leadership

`make_leader` resets `match_indexes_`, sets `leadership_term_`, and sends `AppendEntries`. The
node stays a **candidate that has votes** until a response shows that
`commit_idx == last_entry`. Only then does it set `type_ = kLeader` (in the `AppendEntries`
response handler).

This means a new leader takes client writes, and its PostgreSQL is promoted, only after it has
committed everything inherited from earlier terms. In PostgreSQL terms, the new primary has every
WAL byte any previous primary acknowledged before it starts producing new WAL.

### 3.6 Replication (`AppendEntries`)

**Leader → peer** (`RaftPeerConnection::send_append_entries`):

1. On a new leadership term, set `next_idx = last_idx + 1`, which is optimistic.
2. If the peer is caught up and the last send was less than `election_timeout/4` ago, skip.
   Otherwise this send doubles as the heartbeat.
3. Look up `prev_log_term` for `next_idx-1`, and read up to `max_entries` (8192) entries. If
   either lookup has been compacted away, send an `InstallSnapshot` instead.
4. `trim()` to at most two segments. `respond` is true if there are more than 10 entries or the
   batch contains a `FlushOp`. **Small batches without a flush are fire-and-forget**, so the next
   request carries the progress. This keeps response traffic down for the stream of WriteOps.
5. Advance `next_idx` optimistically, attach the MemBlocks, and send.

**Follower** (`handle_peer_request(AppendEntries)`):

1. If the request's term is stale, reply with failure.
2. Otherwise, become a follower.
3. Write the blocks and msync them. This already happened in `RaftFollowerConnection::received`.
4. Accept the request only if `prev_log_idx >= commit_idx` and
   `append_entries(…, delete_entries=true)` succeeds.
5. On success, reset the election timer and set
   `commit_idx = min(leader_commit, prev+len)`, then reply `success, idx`.
6. On failure, reply `{idx, term at idx, n}`:
   - `idx = min(last, prev)`.
   - `n` is the number of consecutive uncommitted entries that share that term.
   - A missing `term` means the follower has nothing usable at that point and needs a snapshot.

**Leader on response**:

- On success, `set_in_sync()` and `update_match_index`. `MatchIndexes::commit_index` sorts the
  match indexes and returns the highest index that a quorum has reached. When `commit_idx`
  advances, `apply_entries()` runs.
- On failure, if no other AppendEntries or InstallSnapshot is outstanding
  (`should_send_append_entries`):
  - If `term` is missing, send `InstallSnapshot`.
  - Otherwise **walk back up to `min(n, 64)` entries** to find where the leader's log agrees with
    the reported term, set `next_idx` to that point, and resend. This is the fast-backtracking
    optimization.

### 3.7 Commit and apply: where the WAL becomes visible

`apply_entries()` walks `(applied, commit_idx]`:

- **Leader**: completes the waiting fsync promises. The `WalTracker::async_fsync` from
  `wal_fuse` registered a handler keyed on the index of its `FlushOp`. Completing that handler
  releases PostgreSQL's blocked `fsync`, and the transaction commits.
- **Follower**: calls `WalSender::advance(entry.cmd.flushed_lsn)` so the local standby can replay
  up to the committed point.

`handlers_` plays this role in the ported code. The ZK `exec`/response logic still has to be
replaced by the WalTracker integration in §4.

### 3.8 Snapshots and catch-up (`InstallSnapshot`)

The state machine is "the WAL segment files". A Raft snapshot is therefore the **latest
`snapshot_entries_count` (512 K) log entries, plus the full contents of the segments they refer
to** (`mem_blocks(…, full_wal_segment=true)`, at most 2 segments). `applied_indexes` records the
applied index of each node.

The follower replaces its log suffix and segment contents. A follower that is too far behind for
this, because the needed WAL has been recycled, gets a **base backup** through the client
`SnapshotRequest`/`SnapshotResponse{files, links}`, streamed with `Connection::write_file`
(each 64 KiB chunk xxhash32-verified), and then catches up through the log. `PgInstance::backup` and
`verify_backup` produce that backup.

The tunables are in `Options`:

- `snapshot_lag` = 8 K: create a new snapshot once the lag exceeds this.
- `keep_old_logs` = 16 K: entries kept after compaction.
- `compaction_lag` = 64 K: how far a follower lags before it compacts.

### 3.9 Ticks and leadership transfer

- **Tick**: if the leader has not appended anything for `tick_seconds` (2 s), it appends a
  `monostate` entry. This keeps `commit_idx` moving and checks liveness.
- **Graceful step-down** (`stepdown()`, called from `on_shutdown()`):
  1. Set `stepdown_` and force a tick.
  2. The first peer whose `AppendEntries` response shows the tick has been replicated receives a
     `RequestElection{last_entry}`.
  3. That peer becomes a candidate right away if its last entry matches. There is no election
     timeout to wait for.
  4. If the transfer fails, the leader retries `stepdown()`.
  5. `on_shutdown` waits up to 5 s for leadership to move before it closes.

### 3.10 Client channel

`RaftClientConnection` accepts non-Raft identities (the proxy and admin tools):

- **Leadership through heartbeat status**: `status()` returns `0x3` if the node is the leader
  and serving, `0x1` if it is the leader but not serving, and `0` otherwise. A client's
  `RaftServerConnection` learns this from every status response (`set_peer_status`) and offers
  `is_leader()`, `is_serving()`, and `async_leadership_check()`. The proxy can therefore track
  the leader without polling.
- `StatusRequest` → `StatusResponse{leader, applied_idx, primary_flushed_lsn,
  standby_applied_lsn}`. `RaftServerConnection::node_status()` is a blocking wrapper with a 2 s
  timeout.
- `SnapshotRequest{applied_idx}` → `SnapshotResponse{success, files, links}`, followed by the
  files. This path is partially implemented.

## 4. Integration with `wal_fuse` (to be implemented)

The leader's `RaftServer`, or an adapter object, implements `WalTracker`:

| WalTracker call (FUSE thread) | Raft action |
|---|---|
| `async_publish(seg, off, size)` | Append `Entry{term, WalCommand{flushed_lsn, seg.lsn, WriteOp{off,size}}}`. Keep `seg` referenced so `send_request` can serve the bytes. Batches are sent through `send_append_entries(check_sync_flag=true)`. |
| `async_fsync(seg, off, size, cb)` | Local `msync`, then append `FlushOp{off,size}` and register `cb` against its index. `cb(ok)` runs in `apply_entries` when the index commits. If leadership is lost, `cb(kNoConsensus)` runs. |
| open, close, unlink, timeline file | `OpenOp`, `CloseOp`, and `TimelineOp` entries. These need new `WalFuseOps` hooks. |

`PeerConnectionApi::segment(lsn)` and `FollowerConnectionApi::segment(lsn)` resolve a
`shared_ptr<WalSegment>` from a `WalSegmentCache` over the node's `raft_dir/pg_wal`, which is the
same directory the FUSE mount and the WalSender use.

## 5. Concurrency

- Server state such as `type_`, `current_term_`, and `commit_idx_` is held in atomics. State
  transitions take `mtx_`. Note that `make_*` operate on a lock passed in by the caller.
- `usage_lock_` (shared) protects against shutdown. Every request and response handler takes it.
  `close()` takes it exclusively before it tears down the log.
- Each `RaftPeerConnection` serializes its sends with `mtx_`. `sequence_[type]` packs
  (responded << 32 | sent) to give the number of outstanding requests without locks.
- `MatchIndexes` uses atomic CAS updates (values only increase) and a mutex around the quorum
  computation.
- LMDB environments allow one writer and many readers.

## 6. Testing

`raft_log_test` covers term and vote persistence, append/reset/conflict, entries_from and
latest_entries, map-full and txn-full recovery through the injectable wrappers, and key encoding.
There are no server-level tests yet.

## 7. Known issues and open items

1. **Port `raft_server.{h,cc}`.** Remove `ZkRequest`, `ZkState`, `state_`, `snapshot_`,
   `Certificates`, `banyandb`, and `LOG_*`. Implement `segment()`, the `ClientConnectionApi`
   methods (`serving`, `applied_idx`, `*_lsn`), and the WalTracker adapter. Then add
   `raft_server.cc` to the Jamfile and `raft` to `src/Jamfile`.
2. **`AppendEntriesResponse::serialize` omits `running`.** The field is never sent over the wire,
   but the docs say a vote counts only when `success && running`.
3. **`RaftLog::reset_entries` leaks a write transaction.** On an idx/term mismatch it returns
   `false` without `cursor_close` or `txn_abort`. The env stays write-locked and the next write
   deadlocks.
4. **`RaftPeerConnection::send_request` buffer bugs.**
   - In the cached-segment branch, `s` is moved into `segments` and then dereferenced.
   - In the lookup branch, `buffers.emplace_back(s->…)` uses the null `s` instead of
     `seg.value()`.
5. `RequestVote` compares `(term, idx)` tuples directly, which is correct. `RequestElection`
   requires an *exact* last-entry match, so a peer that receives a tick later fails, and the
   leader retries. That is acceptable but could be a long loop.
6. `client_types.h` defines `StatusRequest`, `StatusResponse`, `SnapshotRequest`, and
   `SnapshotResponse`, and `request.h` defines a second, conflicting set. `request.h` should be
   deleted.
7. `RaftClientConnection::received(SnapshotRequest)` replies only on the failure branch. The
   success path, which would stream files, is missing.
8. `PeerConnectionMgr::last_connect_attempt_` has a fixed size of 15, so clusters are limited to
   15 nodes. That is not asserted.
9. `InstallSnapshot` assumes the 512 K latest entries span at most 2 segments after `trim()`.
   `trim()` cuts the batch and keeps `next_idx` consistent, but this needs a test.
