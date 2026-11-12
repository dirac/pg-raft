# `process` — Child processes and PostgreSQL instance lifecycle

Source: `src/process/` — `process.{h,cc}`, `pg_instance.{h,cc}`.

## 1. Purpose

pg-raft manages PostgreSQL directly instead of through an external supervisor. This module
provides:

- `Process`: a minimal `fork`/`exec` wrapper. It captures output line by line and sends it to
  the logger.
- `PgInstance`: everything the HA layer needs to do to one PostgreSQL data directory, namely
  initialize, configure, start, stop, reload, promote, back up, verify, and restore.

The HA control loop is meant to drive `PgInstance`. For example, the Raft leader promotes its
local standby, and a lagging follower restores from a backup.

## 2. `Process`

### Interface

| Method | Behaviour |
|---|---|
| `Process(io_ctx, exe, args)` | Records the executable and arguments. The process name used for logging is the executable's basename. |
| `start()` | Creates the stdout and stderr pipes, `fork()`s, and in the child `dup2`s the pipes and runs `execv`. On exec failure the child calls `_exit(127)`. Returns false if the process is already running or if pipe/fork fails. |
| `stop(timeout_ms=5000)` | Sends `SIGTERM`, polls `try_wait()` every 50 ms, and sends `SIGKILL` + `wait()` when the deadline passes. |
| `kill()` | Sends `SIGKILL`. Does not wait. |
| `wait()` / `try_wait()` | `waitpid` (blocking or `WNOHANG`). Decodes the status as an exit code, or `-signum` if a signal ended the process. Also waits for the output readers to drain. |
| `~Process()` | Kills and reaps the child if it is still running. |

### Output capture

The read ends of the pipes become `asio::readable_pipe`s on the shared io_context.
`readline<err>()` is a self-rescheduling `async_read_until('\n')`, and it calls
`on_stdout_line` / `on_stderr_line` for each complete line. On EOF it flushes any partial last
line and counts down `io_latch_` (initialized to 2). `wait()` blocks on the latch, so **all
output is logged before `wait()` returns**. Subclasses override the line callbacks:
`PgInstance` tags lines with `pgres`, and `controldata()` parses `key: value` lines.

## 3. `PgInstance`

### State

| Field | Purpose |
|---|---|
| `pg_bin_`, `data_dir_`, `port_` | Installation, PGDATA, and listen port. Passed to postgres as `-D` / `-p`. |
| `role_` | `kPrimary` or `kStandby`. Set at construction and changed to `kPrimary` by `promote()`. |
| `postgres_` | The `postgres` server process, a `Process` subclass that tags its output lines. |
| `mu_` | A recursive mutex that serializes all control operations. |
| `backup_sem_` | A binary semaphore that allows only one backup at a time. |

### Setup sequence for a fresh node

```
init()              initdb -D data_dir --data-checksums  (adds checksums unless -k is given)
write_config(m)     appends defaults ⊕ m to postgresql.conf
write_hba(lines)    appends lines + defaults to pg_hba.conf
create_raft_wal(r)  mkdir r/pg_wal; rename every file in data_dir/pg_wal into it
                    → data_dir/pg_wal is now empty and ready to be a FUSE mount point
start()             postgres -D data_dir -p port
wait_ready(port)    loops pg_isready every 300 ms until it succeeds or times out (30 s)
```

**Configuration that pg-raft requires**: `write_config` always includes the following unless the
caller overrides them.

| Setting | Why |
|---|---|
| `wal_sync_method = fsync` | The FUSE layer sees `fsync`. It does not see `fdatasync`/`O_DSYNC`, or `F_FULLFSYNC` on macOS. Commit durability goes through `op_fsync`, so this setting is required. |
| `wal_recycle = off` | Recycled segments would be renamed and overwritten in place. pg-raft wants every segment to be written once with fresh content. |
| `wal_level = replica` | Lets standbys stream. |
| `fsync = on`, `synchronous_commit = on` | Commit waits for fsync, which waits for quorum. |
| `listen_addresses = '*'` | Peers and the proxy connect over the network. |

The default HBA trusts loopback and requires `scram-sha-256` everywhere else.

### Runtime control

- `stop(mode, timeout)` sends `SIGTERM` (Smart), `SIGINT` (Fast), or `SIGQUIT` (Immediate),
  polls for the process to exit, and sends `SIGKILL` after `timeout`.
- `reload()` sends `SIGHUP`.
- `kill()` kills PostgreSQL at once: SIGSTOP the postmaster, SIGKILL each child (`child_pids()`:
  libproc on macOS, `/proc` on Linux) and its process group, then the postmaster. Every
  PostgreSQL child calls `setsid()`, so signalling the postmaster's process group doesn't reach
  them. It takes no lock and doesn't wait (the node calls it from a raft hook); `stop()` reaps the
  postmaster afterwards.
- `Process` starts every child in a process group of its own (`setpgid`); `kill_group()` SIGKILLs
  that group.
- `promote()` creates the `promote` file in PGDATA, sends `SIGUSR1` and sets `role_ = kPrimary`,
  as `pg_ctl promote` does (PostgreSQL ≥ 12 promotes on SIGUSR1 only with the file). The
  promotion is asynchronous: wait for `pg_is_in_recovery()` to be false.

### Backup, verify, and restore

```
SemaphoreLock lk = backup(dest);          // owns the semaphore on success
lk = verify_backup(dest, std::move(lk));  // still owning means verified
restore(dest) / restore_and_start(dest);  // empty data_dir (keep it), rsync -a dest/ into it
```

- **Concurrency**: `backup()` tries to acquire `backup_sem_`. If another backup is already
  running, the call blocks until that backup finishes. It then returns a lock that owns the
  semaphore **without running a second backup**, because the caller wanted a recent backup and
  one was just made. The returned `SemaphoreLock` keeps other backups out until the caller is done
  with the files (for example, until they have been shipped).
- **Primary** backups use `pg_basebackup -h 127.0.0.1 --checkpoint=fast -X stream`, which is a
  hot backup.
- **Standby** backups do a Fast stop, then `rsync -a` the data directory, which is a cold
  backup. The instance is left stopped.
- **Verification** compares `pg_controldata` output for the live directory and the backup:
  - The *Database system identifier* must match.
  - If the cluster state is `shut down` or `shut down in recovery`, it runs
    `pg_checksums --check`.
  - Otherwise it runs `pg_verifybackup`, which needs the manifest that `pg_basebackup` wrote.

  Any failure releases the lock, so the returned lock evaluates to false.

### Thread safety

Every public method takes `mu_` (recursive, because `backup()` calls `stop()`). `backup()` takes
the semaphore *before* `mu_`. That way, a second caller waiting for a backup to finish blocks on
the semaphore and does not hold `mu_` while it waits.

## 4. Testing

`process_test` covers start, stop, kill, exit codes, and output capture. `pg_instance_test` runs
real PostgreSQL 17 from `/opt/pg_raft/postgresql/17/bin` and covers init, config/HBA,
`create_raft_wal`, start/stop, and the backup/verify/restore paths. It skips if PostgreSQL is
not installed.

## 5. Known issues and open items

1. **Promotion** — fixed: `promote()` creates the `promote` file before it signals
   (`timeline_switch_test` promotes through it). `role_` is still set before the promotion has
   finished.
2. `Process` derives the process name with `strrchr(executable, '/') + 1`. That crashes for a bare
   executable name with no `/`.
3. `wait_ready()` holds `mu_` for up to 30 s, which blocks `stop()` on the same instance.
4. Standby backups leave the instance stopped. The caller must restart it, and the API should
   say so. Planned: back up a standby with `pg_basebackup` too, keeping it running; every backup
   in the backup store is a `pg_basebackup` ([backup.md](backup.md#4-taking-backups)).
5. `restore()` uses `/usr/bin/rsync` with a hardcoded path.
6. `write_config` / `write_hba` append on every call. Calling either twice duplicates entries.
   PostgreSQL uses the last value, but the files keep growing.
7. `write_hba`'s defaults put the `0.0.0.0/0 scram-sha-256` rule before the loopback `trust` rule,
   so loopback matches `scram-sha-256` first, and there is no `replication` rule for loopback.
   `initdb`'s own loopback rules at the top of `pg_hba.conf` hide this today.
8. **System V IPC after a kill.** PostgreSQL's main shared memory is an anonymous mmap that the OS
   frees, but a 56-byte System V segment and (on macOS) 7 System V semaphore sets outlive a
   SIGKILL. A postmaster started in the same data directory removes them (their keys come from
   the directory's inode); one started in a new directory doesn't, so they leak until reboot.
   `restore()` therefore keeps the data directory and replaces its contents. And `stop()`, when it
   reaps a postmaster that a signal killed, calls `release_ipc()`: it removes the segment that
   `postmaster.pid` names (if the postmaster is gone and nothing is attached) and the semaphore
   sets that pass PostgreSQL's own test (the last semaphore holds 537 and was last operated on by
   that postmaster). `kill_releases_ipc` kills one of two running instances and checks that only
   its objects are removed.
