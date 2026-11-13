# Sourced by the solo/ scripts.
SOLO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SOLO_DIR/../env.sh"
source "${SOLO_CLUSTER:-$SOLO_DIR/cluster.sh}"

LOG_DIR="$SOLO_DIR/logs"
mkdir -p "$LOG_DIR"
PGDATA_DIR="$DATA_ROOT/pg"

pg_running() { "$PG_BIN/pg_ctl" -D "$PGDATA_DIR" status > /dev/null 2>&1; }

start_pg() {
  pg_running && { log "already running"; return 0; }
  "$PG_BIN/pg_ctl" -D "$PGDATA_DIR" -l "$LOG_DIR/pg.log" -w -t 120 start > /dev/null \
    || { tail -20 "$LOG_DIR/pg.log"; die "PostgreSQL did not start (log: $LOG_DIR/pg.log)"; }
  log "started on port $PORT"
}

stop_pg() {
  pg_running || return 0
  "$PG_BIN/pg_ctl" -D "$PGDATA_DIR" -m fast -w -t 120 stop > /dev/null && log "stopped"
}
