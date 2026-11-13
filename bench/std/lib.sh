# Sourced by the std/ scripts.
STD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$STD_DIR/../env.sh"
source "${STD_CLUSTER:-$STD_DIR/cluster.sh}"

LOG_DIR="$STD_DIR/logs"
mkdir -p "$LOG_DIR"
NODE_COUNT=${#PORTS[@]}
(( ${#HOSTS[@]} == NODE_COUNT )) || die "cluster.sh: HOSTS and PORTS differ in length"

node_dir() { echo "$DATA_ROOT/pg$1"; }

# Whether node ID's data directory is on this machine.
local_node() { [[ -f "$(node_dir "$1")/PG_VERSION" ]]; }

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

# wait_quorum TIMEOUT_SECS: until the primary accepts connections and both standbys stream as
# quorum members. Works from any machine that pg_hba lets in.
wait_quorum() {
  local timeout=$1 host=${HOSTS[0]} port=${PORTS[0]} i n=0
  wait_ready "$host" "$port" "$timeout" || { echo "the primary ($host:$port) is not ready" >&2; return 1; }
  for ((i = 0; i < timeout; i++)); do
    n="$(psql_val "$host" "$port" \
      "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming' AND sync_state = 'quorum'")"
    [[ "$n" == $((NODE_COUNT - 1)) ]] && return 0
    sleep 1
  done
  echo "only $n of $((NODE_COUNT - 1)) standbys streaming as quorum members after $timeout s" >&2
  return 1
}
