# pg-raft — Open items

The open items of every module, gathered from the design documents in `docs/` (each item links
to its section there). Ordered roughly by how much they matter to a running cluster.

## Correctness and durability

Found in the second review (2026-11-12), not fixed yet:

- [ ] **node**: nothing watches the postmaster. A PostgreSQL that dies on its own (OOM kill,
  crash) is never restarted; on the leader the node keeps reporting "serving" and the proxy keeps
  routing to a dead server. ([node.md §3.4](node.md#34-startup-and-postgresql-exits))
- [ ] **node**: `Node::promote` gives up without stepping down when PostgreSQL doesn't leave
  recovery, when the timeline isn't committed in time, or when PostgreSQL isn't running: the node
  stays leader and never serves, so the cluster has no primary until something else changes.
  Only the standby catch-up step steps down. Also, a failed `pg_walfile_name` query gives `tli =
  0`, which skips the wait for the committed timeline.
- [ ] **node**: a backup blocks promotion. `PgInstance::backup` holds `mu_` for the whole
  `pg_basebackup` (on a standby it can wait up to `checkpoint_timeout` to start, then copy the
  whole database), and `promote()` / `is_running()` take `mu_`; a node elected during its backup
  can't promote until the backup ends.
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
- [ ] **raft**: `compact_log` runs under the server lock and, when `keep_from` moves, reads and
  applies every entry since the previous boundary (a whole backup interval of entries) and deletes
  them in one LMDB transaction: commits stall meanwhile, and at high write rates the delete can
  hit `MDB_TXN_FULL`, which aborts the process.
- [ ] **raft**: `send_install_snapshot` reads the first entry and `boundary_state()` separately;
  a compaction in between pairs the entries with a later boundary state.

- [ ] **raft**: `RaftLog::install_snapshot` runs three transactions (delete every entry, append the
  new ones, save the states); a crash in between leaves a log that doesn't match its stored state.
  Do it in one transaction. ([raft.md §8](raft.md#8-open-items))
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
- [ ] **wal_sender**: `server_version` is hardcoded to `17.0`; take it from
  `Config::pg_major_version`.
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

## Tests and housekeeping

- [ ] **node tests**: about 2 minutes each and can time out in parallel; run serially.
  `node_recovery_test` failed once at the restarted node's `start()` in a parallel run — not yet
  investigated. ([node.md §10](node.md#10-open-items))
- [ ] A killed test run can leak PostgreSQL's System V shared memory (macOS allows 32 segments):
  `ipcs -m`, then `ipcrm -m <id>`.
- [ ] `data_test` takes ~50 s and times out under ASAN.
- [ ] `src/status/prompt.txt` (the prompt that generated `PgClient`) is tracked; keep or delete.
- [ ] `src/wal_fuse/tmp/` (two 16 MiB WAL segments, untracked) is a leftover; delete or ignore.
