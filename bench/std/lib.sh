# Sourced by the std/ scripts.
STD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$STD_DIR/../env.sh"

DATA_ROOT="${DATA_ROOT:-$STD_DIR/data}"   # node N in $DATA_ROOT/pgN
LOG_DIR="$STD_DIR/logs"
mkdir -p "$LOG_DIR"

# Node 0 is the primary; nodes 1 and 2 are standbys s1 and s2 (synchronous_standby_names).
PRIMARY_HOST="${PRIMARY_HOST:-127.0.0.1}"  # what the standbys connect to
PORTS=(5532 5533 5534)
NODE_COUNT=${#PORTS[@]}

node_dir() { echo "$DATA_ROOT/pg$1"; }

pg_running() { "$PG_BIN/pg_ctl" -D "$(node_dir "$1")" status > /dev/null 2>&1; }

start_pg() {  # start_pg ID
  local id=$1
  pg_running "$id" && { log "pg$id is already running"; return 0; }
  "$PG_BIN/pg_ctl" -D "$(node_dir "$id")" -l "$LOG_DIR/pg$id.log" -w -t 120 start > /dev/null \
    || { tail -20 "$LOG_DIR/pg$id.log"; die "pg$id did not start (log: $LOG_DIR/pg$id.log)"; }
  log "pg$id started on port ${PORTS[$id]}"
}

stop_pg() {  # stop_pg ID
  local id=$1
  pg_running "$id" || return 0
  "$PG_BIN/pg_ctl" -D "$(node_dir "$id")" -m fast -w -t 120 stop > /dev/null && log "pg$id stopped"
}
