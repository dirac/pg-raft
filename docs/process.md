# `process` — Child processes and PostgreSQL instance lifecycle

Source: `src/process/` — `process.{h,cc}`, `pg_instance.{h,cc}`.

## 1. Purpose

pg-raft manages PostgreSQL directly instead of through an external supervisor:

- `Process`: a minimal `fork`/`exec` wrapper. It captures the child's output line by line and
  sends it to the logger.
- `PgInstance`: everything the node does to one PostgreSQL data directory: initialize, configure,
  start, stop, kill, reload, promote, back up, verify, and restore.

## 2. `Process`

### Interface

| Method | Behaviour |
|---|---|
| `Process(io_ctx, exe, args)` / `Process(exe, args)` | Records the executable and arguments (the second form uses the global io_context). The process name used for logging is the executable's basename. |
| `start()` | Creates the stdout and stderr pipes and `fork()`s. The child joins a **process group of its own** (`setpgid`, also done by the parent against the race), `dup2`s the pipes, **closes every other inherited descriptor** (the parent's sockets, e.g. raft's listening port: asio's sockets are not close-on-exec on macOS, and a PostgreSQL that outlives the node must not keep them), and runs `execv`; on exec failure it calls `_exit(127)`. Returns false if the process is already running or if pipe/fork fails. |
| `stop(timeout_ms=5000)` | Sends `SIGTERM`, polls `try_wait()` every 50 ms, and sends `SIGKILL` + `wait()` when the deadline passes. |
| `kill()` | Sends `SIGKILL`. Does not wait. |
| `kill_group()` | Sends `SIGKILL` to the child's process group. Takes no lock (the pid is atomic) and doesn't wait. |
| `wait()` / `try_wait()` | `waitpid` (blocking or `WNOHANG`). Decodes the status as an exit code, or `-signum` if a signal ended the process. Also waits for the output readers to drain. |
| `pid()` | The child's pid, or -1. |
| `~Process()` | Kills and reaps the child if it is still running. |

`child_pids(pid)` lists a process's direct children (libproc on macOS, `/proc` on Linux).

### Output capture

The read ends of the pipes become `asio::readable_pipe`s on the io_context. `readline<err>()` is a
self-rescheduling `async_read_until('\n')`, and it calls `on_stdout_line` / `on_stderr_line` for
each complete line (by default, `logger::plog` with the process name). On EOF it flushes any
partial last line and counts down `io_latch_` (initialized to 2). `wait()` blocks on the latch, so
**all output is logged before `wait()` returns**. Subclasses override the line callbacks:
`PgInstance` tags lines with `pgres`.

## 3. `PgInstance`

`PgInstance(pg_bin, data_dir, port, role = Role::kStandby)`.

### State

| Field | Purpose |
|---|---|
| `pg_bin_`, `data_dir_`, `port_` | Installation, PGDATA, and listen port, passed to postgres as `-D` / `-p`. |
| `role_` | `kPrimary` or `kStandby`. Set at construction and changed to `kPrimary` by `promote()`. |
| `postgres_` | The `postgres` server process, a `Process` subclass that tags its output lines. |
| `mu_` | A recursive mutex that serializes the control operations (`restore_and_start()` calls `restore()`, `stop()` calls `release_ipc()`). |
| `backup_sem_` | A binary semaphore that allows only one backup at a time. |

### Setup sequence for a fresh data directory

```
init()              initdb -D data_dir --data-checksums  (adds checksums unless -k is given)
write_config(m)     appends defaults ⊕ m to postgresql.conf
write_hba(lines)    appends lines + defaults to pg_hba.conf
create_raft_wal(r)  mkdir r/pg_wal; rename every file in data_dir/pg_wal into it
                    → data_dir/pg_wal is now empty and ready to be a FUSE mount point
start()             postgres -D data_dir -p port
wait_ready(port)    pg_isready every 300 ms until this instance is ready, or 30 s
```

**Configuration that pg-raft requires**: `write_config` always includes the following unless the
caller overrides them.

| Setting | Why |
|---|---|
| `wal_sync_method = fdatasync` | Commit durability goes through `op_fsync`. `fdatasync` reaches it as an fsync with `datasync` set, handled the same way, and skips the file's metadata. The FUSE layer never sees `open_sync`/`open_datasync` (no call) or macOS's `F_FULLFSYNC`. |
| `wal_recycle = off` | Recycled segments would be renamed and overwritten in place. pg-raft wants every segment to be written once with fresh content. |
| `wal_init_zero = off` | With `wal_recycle = off` every segment switch creates a new file, and zero-filling it writes 16 MiB through FUSE (about 2,000 writes) while `WALWriteLock` is held: every commit stalled ~85 ms at each switch. Off, PostgreSQL writes one byte at the segment's end. |
| `wal_level = replica` | Lets standbys stream. |
| `fsync = on`, `synchronous_commit = on` | Commit waits for fsync, which waits for a quorum. |
| `listen_addresses = '*'` | The proxy connects over the network. |

The default HBA trusts loopback and requires `scram-sha-256` everywhere else (see open item 4).

### Runtime control

- `wait_ready(port, timeout_s = 30)` polls `pg_isready`, and when a server answers checks that it
  is **this** instance: `postmaster.pid` names this instance's pid on its first line, and its 8th
  line (the postmaster's status) is `ready`, or `standby` for a hot standby. A PostgreSQL that
  failed to bind a port another one holds writes the file and exits without reaching either; it
  is not ready, and a server answering that is not this instance fails the wait at once.
- `stop(mode, timeout)` sends `SIGTERM` (Smart), `SIGINT` (Fast) or `SIGQUIT` (Immediate), polls
  for the process to exit, and sends `SIGKILL` after `timeout`. When it reaps a postmaster that a
  signal killed, it calls `release_ipc()` (below).
- `kill()` kills PostgreSQL at once: SIGSTOP the postmaster (so it forks nothing more), SIGKILL
  each child (`child_pids()`) and its process group, then the postmaster. Every PostgreSQL child
  calls `setsid()`, so a signal to the postmaster's process group would not reach them. It takes
  no lock and doesn't wait: the node calls it from a raft hook. `stop()` reaps the postmaster
  afterwards.
- `reload()` sends `SIGHUP`.
- `promote()` creates the `promote` file in PGDATA, sends `SIGUSR1` and sets `role_ = kPrimary`,
  as `pg_ctl promote` does (PostgreSQL ≥ 12 promotes on SIGUSR1 only with the file). The promotion
  is asynchronous: wait for `pg_is_in_recovery()` to be false.
- `pid()` is the postmaster's pid.

### System V IPC after a kill

PostgreSQL's main shared memory is an anonymous mmap that the OS frees, but a 56-byte System V
segment and (on macOS) 7 System V semaphore sets outlive a SIGKILL. A postmaster started in the
same data directory removes them (their keys come from the directory's inode); one started in a
new directory doesn't, so they leak until reboot, and macOS allows only 32 segments. Hence:

- `restore()` keeps the data directory and replaces its contents.
- `release_ipc()` removes what a killed postmaster left: the segment that `postmaster.pid` names
  (7th line: key and id), if that postmaster is gone and nothing is attached, and the semaphore
  sets that pass PostgreSQL's own ownership test (their last semaphore holds `PGSemaMagic`, 537,
  and was last operated on by that postmaster). `stop()` calls it whenever it reaps a postmaster
  killed by a signal, so every kill path cleans up.

### Backup, verify, and restore

```
SemaphoreLock lk = backup(dest);          // owns the semaphore on success
lk = verify_backup(dest, std::move(lk));  // still owning means verified
restore(dest) / restore_and_start(dest);  // empty data_dir (keep it), rsync -a dest/ into it
```

- **Every backup is a `pg_basebackup`** (`-h 127.0.0.1 --checkpoint=fast -X stream`), on a
  primary or a standby alike: the instance keeps running, and the backup has a `backup_manifest`.
- **Concurrency**: `backup()` tries to acquire `backup_sem_`. If another backup is already running,
  the call blocks until it finishes, then returns a lock that owns the semaphore **without running
  a second backup** (the caller wanted a recent backup, and one was just made). The returned
  `SemaphoreLock` keeps other backups out until the caller is done with the files.
- **Verification** compares `pg_controldata` for the live directory and the backup: the *Database
  system identifier* must match; a backup in state `shut down` or `shut down in recovery` is
  checked with `pg_checksums --check`, any other with `pg_verifybackup`. Any failure releases the
  lock, so the returned lock evaluates to false.

### Thread safety

Every public method takes `mu_`, except `kill()` (lock-free by design). `backup()` takes the
semaphore *before* `mu_`, so a second caller waiting for a backup blocks on the semaphore and does
not hold `mu_` while it waits.

## 4. Testing

- `process_test`: start, stop, kill, exit codes, output capture, `kill_group` reaching a shell's
  background child, and that a child inherits no descriptor but stdin/stdout/stderr.
- `pg_instance_test` (real PostgreSQL 17, skips if absent): init, config and HBA,
  `create_raft_wal`, start/stop, backup/verify/restore, `kill()` taking down a backend in the
  middle of a query, `restore()` keeping the directory, `release_ipc()` removing only the killed
  instance's IPC, and `wait_ready()` refusing a server that is not this instance.

## 5. Open items

1. `role_` is set by `promote()` before the promotion has finished.
2. `Process` derives the process name with `strrchr(executable, '/') + 1`, which crashes for a bare
   executable name with no `/`.
3. `wait_ready()` holds `mu_` for up to its timeout, which blocks `stop()` on the same instance.
4. `write_hba`'s defaults put the `0.0.0.0/0 scram-sha-256` rule before the loopback `trust` rule,
   so loopback matches `scram-sha-256` first, and there is no `replication` rule for loopback.
   `initdb`'s own loopback rules at the top of `pg_hba.conf` hide this today.
5. `write_config` / `write_hba` append on every call: calling either twice duplicates entries
   (PostgreSQL uses the last value, but the files keep growing).
6. `restore()` uses `/usr/bin/rsync` with a hardcoded path.
