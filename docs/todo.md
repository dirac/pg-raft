# pg-raft — Open items

The open items of every module, gathered from the design documents in `docs/` (each item links
to its section there). Ordered roughly by how much they matter to a running cluster.

## Correctness and durability

- [ ] **node**: restarting a stopped cluster can leave a standby unable to rejoin (seen on Linux,
  2026-11-13). Node 2 shut down as a standby whose last replayed checkpoint was at
  `0/138C57E8` on timeline 4. On restart the new leader promoted with `wal_end = 0/138C57B0` and
  forked timeline 5 there, so node 2's PostgreSQL failed with "requested timeline 5 is not a child
  of this server's history" and the node exited after `wait_ready` timed out. A standby replayed
  WAL past what the new leader treated as committed: either it received bytes beyond committed
  `wal_end`, or `wal_end` stops short. Not yet investigated. Since then the ledger changed what
  `raft/wal` holds (committed WAL only, written on apply), and a node rebuilds `pg/pg_wal` on a
  start after an unclean end (the last node of a stopping cluster always ends uncleanly: it can't
  commit its shutdown checkpoint): re-test. ([node.md §3.2](node.md#32-promotion-became_leader))

Found in the second review (2026-11-12), not fixed yet:

- [ ] **node**: nothing watches the postmaster. A PostgreSQL that dies on its own (OOM kill,
  crash) is never restarted; on the leader the node keeps reporting "serving" and the proxy keeps
  routing to a dead server. ([node.md §3.4](node.md#34-startup-and-postgresql-exits))
- [ ] **node**: a leader whose promotion failed can stay leader without serving (chaos run
  2026-11-23, #35). `Node::promote` steps down when a step fails, but with no quorum the stepdown
  is called off after `kStepdownTimeouts` election timeouts ("did not hand over the leadership,
  calling it off", `RaftServer`), and nothing retries the promotion once the quorum is back; its
  heartbeats keep the others from campaigning (PreVote refused), so the cluster had no primary for
  89 s, until finding 1 below deposed it. A leader that isn't serving should retry the promotion,
  or keep stepping down. Also, a failed `pg_walfile_name` query gives `tli = 0`, which skips the
  wait for the committed timeline. ([chaos/README.md, finding 2](../chaos/README.md#findings))
- [ ] **node**: a leader serves after its post-promotion `CHECKPOINT` failed (chaos run
  2026-11-23, #94). `Node::promote` step 5 only logs "the checkpoint after the promotion did not
  complete" and goes on to `set_serving(true)`: the proxy routed clients to a primary without a
  quorum (nothing lost, commits wait for raft), and the checkpoint's protection is skipped (the
  control file keeps the standby's minimum recovery point, which a crashed primary restarted as a
  standby trusts). Step down, or retry the checkpoint, instead of serving.
  ([chaos/README.md, finding 3](../chaos/README.md#findings))
- [ ] **node**: a backup blocks promotion. `PgInstance::backup` holds `mu_` for the whole
  `pg_basebackup` (on a standby it can wait up to `checkpoint_timeout` to start, then copy the
  whole database), and `promote()` / `is_running()` take `mu_`; a node elected during its backup
  can't promote until the backup ends. (Likely seen in the chaos run 2026-11-23, #4: elected
  0.1 s after it began a `pg_basebackup`, the node began promoting 2.6 s later; not confirmed.)
- [ ] **node**: `restart_as_standby` and `restore_and_restart` reset `fuse_` and, if the rebuild
  or restore then fails, return with it null; the next attempt calls `fuse_->stop()` on a null
  pointer (the backup thread retries the restore every minute).
- [ ] **node**: `rebuild_pg_wal` deletes every `pg/pg_wal` segment that is not in `raft/wal`,
  including segments that compaction removed from `raft/wal` because they are before the latest
  backup's end. A demoted primary whose last checkpoint's redo point is before that segment (a
  demotion within one `checkpoint_timeout` after a backup that took long) loses its redo segment
  and can't restart; nothing sets `needs_backup` to recover it. Keep the segments before
  `first_segment` (they are committed).
- [ ] **node**: `restore_latest_backup` ignores a failed `copy_file` into `raft/wal` (the `ec` is
  overwritten by the next `rename`), and renames entries out of `data/pg_wal` while iterating it.
- [ ] **wal_fuse**: `op_write` always returns `size`, hiding real I/O errors such as a failed mmap.
  ([wal_fuse.md §6](wal_fuse.md#6-open-items))
- [ ] **wal_fuse**: `WalState` doesn't range-check a corrupt `xl_tot_len` / `xlp_rem_len` and
  doesn't check CRCs. ([wal_fuse.md §6](wal_fuse.md#6-open-items))
- [ ] **backup**: a margin: WAL is kept only from the latest backup, so a corrupt latest backup
  leaves nothing to restore from; keeping WAL from the second-latest would cost one more
  `backup_wal_size`. ([backup.md §7](backup.md#7-open-items))
- [ ] **backup**: retention of old backups (keep the latest N, `remove` the rest).
  ([backup.md §7](backup.md#7-open-items))

## Robustness

- [ ] **raft**: `raft_ensemble_leader_stopped_test` crashed about half the time when four copies
  ran at once on Linux (a read of the LMDB raft log saw inconsistent data). LMDB is gone (the
  ledger, [ledger.md](ledger.md)); run it again on Linux, four at a time.
- [ ] **raft**: when the ledger doesn't open (a file header, or a file other than the last, that
  doesn't check out: `Ledger::open` fails), the node should treat its log as lost and recover like
  a wiped node (`needs_backup`, InstallSnapshot, the latest backup) instead of exiting. Startup
  checks every file header, every record of the last file (crc and chain) and the record headers
  of the others; `Ledger::verify()` (every crc) runs only on demand.

- [ ] **process**: `Process` derives the process name with `strrchr(executable, '/') + 1`, which
  crashes for a bare executable name. ([process.md §5](process.md#5-open-items))
- [ ] **process**: `wait_ready()` holds `mu_` for up to its timeout, blocking `stop()`.
- [ ] **process**: `role_` is set by `promote()` before the promotion has finished.
- [ ] **process**: `write_hba`'s defaults put `0.0.0.0/0 scram-sha-256` before the loopback
  `trust` rule and have no loopback `replication` rule (hidden today by `initdb`'s own rules).
- [ ] **process**: `write_config` / `write_hba` append duplicates on every call.
- [ ] **process**: `restore()` uses a hardcoded `/usr/bin/rsync`.
- [ ] **wal_sender**: cap `XLogData` at 128 KiB (as PostgreSQL does) instead of up to 16 MiB per
  advance. ([wal_sender.md](wal_sender.md))
- [ ] **wal_sender**: a close callback, so that the owner learns when a sender closes on an error.
- [x] **wal_sender**: `server_version` comes from `Config::pg_major_version`
  (`WalSender::set_pg_major_version`).
- [ ] **wal_sender**: `do_advance` opens `X` only, never `.X` (only matters when reading a live
  `pg_wal`, as the tests do); reading through the shared `WalSegment` cache would also avoid an
  open/pread/close per advance.
- [ ] **wal_sender**: command matching is by prefix; the end-of-timeline reply was not compared
  byte for byte with PostgreSQL's walsender.
- [ ] **wal_fuse**: an `fsync` blocks the single FUSE loop thread until the raft commit.
- [ ] **net**: `Server::acceptor_cancel` sleeps in 10 ms steps while the accept loop finishes.
  ([net.md §7](net.md#7-open-items))
- [ ] **proxy**: an accept error other than `operation_aborted` (e.g. `EMFILE`) re-accepts at
  once, a busy loop; `net::Server` waits 100 ms. `/status` starts a detached thread per request.
- [ ] **node**: `senders_` (weak pointers to WAL senders) is never pruned.
- [ ] **status**: `prepare()` and `async_exec` share read buffers; don't prepare while an exec is
  in flight (or route `prepare` through the async queue). ([status.md](status.md))
- [ ] **node**: a standby may take a backup before it has caught up with the primary (valid, but
  moves `keep_from` less than it could). ([backup.md §7](backup.md#7-open-items))

## Features

- [ ] **net**: TLS (certificates, key pairs and an SSL context in `base`; `socket_type` already has
  the `ssl_socket` alternative).
- [ ] **node**: membership changes (a node that is not in `servers`).
  ([node.md §10](node.md#10-open-items))
- [ ] **raft**: FUSE hooks for `OpenOp` / `CloseOp`, or remove them.
- [ ] **raft**: remove the unused client-channel `SnapshotRequest` / `SnapshotResponse` and
  `set_backup_provider` (nodes restore from the backup store).
- [ ] **proxy**: `ProxyOptions` (failover window, connect timeout, heartbeats, reconnect) in
  `config.toml`. ([proxy.md §8](proxy.md#8-open-items))
- [ ] **proxy**: the admin interface has no authentication or TLS.
- [ ] **status**: SCRAM authentication, non-loopback addresses, bind parameters, typed values
  (only what tests and the node poller need is there today).
- [ ] **backup**: no S3 store (decided); a shared filesystem works as a `DirBackupStore` if it has
  atomic rename, a shared mtime and a durable fsync.
- [ ] **PostgreSQL patch for an overlapping commit wait** (hook mode done: `sync_rep_wait_hook` in
  `SyncRepWaitForLSN`, served by `pg_raft_wal` and `NodeWalTracker::async_commit`; the wait ignores
  cancel and terminate requests and PANICs on failure; `pg_raft.conf` sets
  `restart_after_crash = off`. Left: a session's `SET synchronous_commit = local` (or `off`) still
  skips the wait; FUSE mode still uses stock synchronous replication.) Today a commit waits for the
  quorum inside the WAL fsync, under `WALWriteLock`, so the raft rounds are serial (the cost
  against streaming replication, `bench/README.md` 2026-11-18). Waiting instead as PostgreSQL's
  synchronous standby (`SyncRepWaitForLSN`, branch `sync-rep-commit`, `commit_wait = "sync_rep"`)
  overlaps the rounds, but stock PostgreSQL then lets a commit become visible without the quorum.
  What would have to change, in `src/backend/replication/syncrep.c` (17.9) and around it, behind a
  setting (e.g. `synchronous_commit_wait = uninterruptible`):
  - `SyncRepWaitForLSN`, `QueryCancelPending` (a cancel request, `statement_timeout`,
    `transaction_timeout`): today it stops waiting with a WARNING and the commit becomes visible.
    It should keep waiting (clear the flag, don't break).
  - `ProcDiePending` (`pg_terminate_backend`, a fast shutdown): today it stops waiting with a
    WARNING and the commit becomes visible before the process exits. It should end the process
    without the commit becoming visible: PANIC, since a committed transaction can't be aborted.
  - Postmaster death: already exits without the commit becoming visible (the process dies).
  - A PANIC restarts every backend and replays the local WAL, which would bring the unreplicated
    commit back: done, `pg_raft.conf` forces `restart_after_crash = off`, so that the node restarts
    PostgreSQL as a standby on a `pg_wal` rebuilt from raft (as after any unclean end).
  - `synchronous_standby_names` emptied or changed by a reload (`ALTER SYSTEM`) releases every
    waiter (`SyncRepUpdateSyncStandbysDefined`): the node must own it (reject or rewrite changes).
  - A session can set `synchronous_commit` to `local` or `off` and skip the wait: its own choice,
    but no longer the cluster's guarantee; document it, or force it (a role or database setting).
  - The patch is small (one function plus a setting); it has to be carried, and checked, for each
    PostgreSQL major version that pg-raft supports (it already builds its own, `external/`).

## Tests and housekeeping

- [ ] **node tests**: about 2 minutes each and can time out in parallel; run serially.
  `node_recovery_test` failed once at the restarted node's `start()` in a parallel run — not yet
  investigated. On Linux (2026-11-13), with all three running together, `node_test` lost its
  leader mid-pgbench (the leader process went silent for >1 s, past the tests' 1 s election
  timeout) and `node_backup_test`'s standby took its backup before replaying pgbench's WAL; both
  pass when run alone. ([node.md §10](node.md#10-open-items))
- [ ] A killed test run can leak PostgreSQL's System V shared memory (macOS allows 32 segments):
  `ipcs -m`, then `ipcrm -m <id>`.
- [ ] `data_test` takes ~50 s and times out under ASAN.
- [ ] `src/status/prompt.txt` (the prompt that generated `PgClient`) is tracked; keep or delete.
- [ ] `src/wal_fuse/tmp/` (two 16 MiB WAL segments, untracked) is a leftover; delete or ignore.
