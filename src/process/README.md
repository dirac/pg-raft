# process

Two classes for launching and managing child processes, with special support for PostgreSQL instances.

## Process

Thin wrapper around `fork`/`exec`. Construct with a binary path and an argument list, then:

- `start()` — forks and execs. stdout/stderr of the child are captured line-by-line and logged via `logger::plog` with the binary name as a prefix.
- `stop(signal)` — sends `SIGTERM`, waits briefly, then escalates to `SIGKILL` if the process doesn't exit.
- `kill()` — sends `SIGKILL` immediately.
- `wait()` / `try_wait()` — block or poll for exit status.

## PgInstance

Higher-level wrapper around `Process` for a full PostgreSQL cluster. Handles the entire lifecycle:

**Setup sequence** (for a new primary):
1. `init(extra_args)` — runs `initdb`
2. `write_config(map)` — appends key/value pairs to `postgresql.conf`; also injects required pg-raft defaults (`wal_sync_method=fsync`, `wal_recycle=off`) unless explicitly overridden
3. `write_hba(lines)` — appends lines to `pg_hba.conf`
4. `create_raft_wal()` — moves the initial WAL segments from `data_dir/pg_wal` into `raft_dir/pg_wal`, leaving `pg_wal` empty for the FUSE mount; must be called after `init()` and before `start()`
5. `start()` / `wait_ready(port)` — starts postgres and polls `pg_isready`

**Runtime controls:**
- `stop(StopMode)` — Smart (`SIGTERM`), Fast (`SIGINT`), Immediate (`SIGQUIT`)
- `reload()` — `SIGHUP` (reload config)
- `promote()` — `SIGUSR1` (promote standby to primary)

**Backup/restore:**
- `backup(dest)` — recursive copy of the data directory (cold backup; postgres must be stopped)
- `restore(src)` — copies a backup into the data directory
- `restore_and_start(src)` — restore then start in one step
