# pg-raft — Open items

The open items of every module, gathered from the design documents in `docs/` (each item links
to its section there). Ordered roughly by how much they matter to a running cluster.

## Correctness and durability

- [ ] **raft**: `RaftLog::install_snapshot` runs three transactions (delete every entry, append the
  new ones, save the states); a crash in between leaves a log that doesn't match its stored state.
  Do it in one transaction. ([raft.md §8](docs/raft.md#8-open-items))
- [ ] **wal_fuse**: `op_write` always returns `size`, hiding real I/O errors such as a failed mmap.
  ([wal_fuse.md §6](docs/wal_fuse.md#6-open-items))
- [ ] **wal_fuse**: `WalState` doesn't range-check a corrupt `xl_tot_len` / `xlp_rem_len` and
  doesn't check CRCs. ([wal_fuse.md §6](docs/wal_fuse.md#6-open-items))
- [ ] **backup**: a margin: WAL is kept only from the latest backup, so a corrupt latest backup
  leaves nothing to restore from; keeping WAL from the second-latest would cost one more
  `backup_wal_size`. ([backup.md §7](docs/backup.md#7-open-items))
- [ ] **backup**: retention of old backups (keep the latest N, `remove` the rest).
  ([backup.md §7](docs/backup.md#7-open-items))

## Robustness

- [ ] **process**: `Process` derives the process name with `strrchr(executable, '/') + 1`, which
  crashes for a bare executable name. ([process.md §5](docs/process.md#5-open-items))
- [ ] **process**: `wait_ready()` holds `mu_` for up to its timeout, blocking `stop()`.
- [ ] **process**: `role_` is set by `promote()` before the promotion has finished.
- [ ] **process**: `write_hba`'s defaults put `0.0.0.0/0 scram-sha-256` before the loopback
  `trust` rule and have no loopback `replication` rule (hidden today by `initdb`'s own rules).
- [ ] **process**: `write_config` / `write_hba` append duplicates on every call.
- [ ] **process**: `restore()` uses a hardcoded `/usr/bin/rsync`.
- [ ] **wal_sender**: cap `XLogData` at 128 KiB (as PostgreSQL does) instead of up to 16 MiB per
  advance. ([wal_sender.md](docs/wal_sender.md))
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
  ([net.md §7](docs/net.md#7-open-items))
- [ ] **status**: `prepare()` and `async_exec` share read buffers; don't prepare while an exec is
  in flight (or route `prepare` through the async queue). ([status.md](docs/status.md))
- [ ] **node**: a standby may take a backup before it has caught up with the primary (valid, but
  moves `keep_from` less than it could). ([backup.md §7](docs/backup.md#7-open-items))

## Features

- [ ] **net**: TLS (certificates, key pairs and an SSL context in `base`; `socket_type` already has
  the `ssl_socket` alternative).
- [ ] **node**: membership changes (a node that is not in `servers`).
  ([node.md §10](docs/node.md#10-open-items))
- [ ] **raft**: FUSE hooks for `OpenOp` / `CloseOp`, or remove them.
- [ ] **raft**: remove the unused client-channel `SnapshotRequest` / `SnapshotResponse` and
  `set_backup_provider` (nodes restore from the backup store).
- [ ] **proxy**: `ProxyOptions` (failover window, connect timeout, heartbeats, reconnect) in
  `config.toml`. ([proxy.md §8](docs/proxy.md#8-open-items))
- [ ] **proxy**: the admin interface has no authentication or TLS.
- [ ] **status**: SCRAM authentication, non-loopback addresses, bind parameters, typed values
  (only what tests and the node poller need is there today).
- [ ] **backup**: no S3 store (decided); a shared filesystem works as a `DirBackupStore` if it has
  atomic rename, a shared mtime and a durable fsync.

## Tests and housekeeping

- [ ] **node tests**: about 2 minutes each and can time out in parallel; run serially.
  `node_recovery_test` failed once at the restarted node's `start()` in a parallel run — not yet
  investigated. ([node.md §10](docs/node.md#10-open-items))
- [ ] A killed test run can leak PostgreSQL's System V shared memory (macOS allows 32 segments):
  `ipcs -m`, then `ipcrm -m <id>`.
- [ ] `data_test` takes ~50 s and times out under ASAN.
- [ ] `src/status/prompt.txt` (the prompt that generated `PgClient`) is tracked; keep or delete.
- [ ] `src/wal_fuse/tmp/` (two 16 MiB WAL segments, untracked) is a leftover; delete or ignore.
