# process

Two classes for launching and managing child processes, with support for PostgreSQL instances.

## Process

A thin wrapper around `fork`/`exec`: `Process(executable, args)`, optionally with an
`io_context`.

- `start()` — forks and execs. The child's stdout and stderr are read line by line through pipes
  on the io_context and passed to `on_stdout_line()` / `on_stderr_line()`, which subclasses can
  override; by default they log through `logger::plog` with the executable's name as the prefix.
- `stop(timeout_ms = 5000)` — sends `SIGTERM`, and `SIGKILL` if the process hasn't exited by the
  timeout.
- `kill()` — sends `SIGKILL` without waiting.
- `wait()` / `try_wait()` — block until the process exits, or poll. They return the exit code, or
  `-signal` if a signal killed it. All the output is relayed before `wait()` returns.
- The destructor kills and reaps a process that is still running.

## PgInstance

A wrapper around `Process` for one PostgreSQL data directory:
`PgInstance(pg_bin, data_dir, port, role = Role::kStandby)`. Every method takes a recursive mutex.

**Setup sequence** (new node):
1. `init(initdb_args)` — runs `initdb -D data_dir`, adding `--data-checksums` unless `-k` is
   given.
2. `write_config(map)` — appends `key = value` lines to `postgresql.conf`. The pg-raft defaults
   are always included unless overridden: `wal_sync_method = fsync` (the FUSE layer sees fsync),
   `wal_recycle = off`, `wal_level = replica`, `listen_addresses = '*'`, `fsync = on`,
   `synchronous_commit = on`.
3. `write_hba(lines)` — appends the lines to `pg_hba.conf`, plus defaults that trust loopback and
   require `scram-sha-256` from anywhere else.
4. `create_raft_wal(raft_dir)` — moves the WAL segments from `data_dir/pg_wal` into
   `raft_dir/pg_wal`, leaving `pg_wal` empty as the FUSE mount point. Call it after `init()` and
   before `start()`.
5. `start()` / `wait_ready(port)` — start postgres (`-D data_dir -p port`), then poll
   `pg_isready` every 300 ms (30 s by default).

**Runtime control:**
- `stop(StopMode, timeout_ms)` — Smart (`SIGTERM`), Fast (`SIGINT`, the default) or Immediate
  (`SIGQUIT`), then `SIGKILL` once the timeout passes.
- `reload()` — `SIGHUP`.
- `promote()` — `SIGUSR1`, and `role()` becomes `kPrimary`. PostgreSQL ≥ 12 also needs a
  `promote` file in the data directory, which this doesn't create yet.

**Backup and restore:**
- `backup(dest_dir)` → `SemaphoreLock`, which evaluates to true on success. On a primary it runs
  `pg_basebackup --checkpoint=fast -X stream`, which is a hot backup. On a standby it stops the
  instance (Fast) and copies the data directory with `rsync -a`; the instance is left stopped.
  Only one backup runs at a time: a concurrent call waits for the running one and returns a lock
  without taking another backup.
- `verify_backup(dir, lock)` — checks that `pg_controldata` reports the same system identifier for
  the data directory and the backup. It then runs `pg_checksums --check` for a backup of a stopped
  instance, or `pg_verifybackup` for a `pg_basebackup`. On failure the lock is released.
- `restore(dir)` / `restore_and_start(dir)` — `rsync -a dir/` into the data directory (then
  start).

## Tests

`process_test`; `pg_instance_test` (needs PostgreSQL at `/opt/pg_raft/postgresql/17/bin`, and
skips if it is absent).
