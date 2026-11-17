# Sourced by the raft/ scripts.
RAFT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$RAFT_DIR/../env.sh"

CONFIG="${CONFIG:-$RAFT_DIR/config.toml}"
BIN="${BIN:-$RAFT_DIR/bin}"          # release binaries (copied from the pg-raft build)
LOG_DIR="$RAFT_DIR/logs"
RUN_DIR="$RAFT_DIR/run"              # pid files
mkdir -p "$LOG_DIR" "$RUN_DIR"

# Values from config.toml (top-level keys only; good enough for this file).
cfg() { sed -nE "s/^$1[[:space:]]*=[[:space:]]*\"?([^\"#]*[^\"#[:space:]])\"?.*/\1/p" "$CONFIG" | head -1; }
DATA_ROOT="$(cfg dir)"
BACKUP_STORE="$(cfg backup_store)"
PROXY_PG_PORT="$(cfg proxy_pg_port)"
PROXY_HTTP_PORT="$(cfg proxy_http_port)"
NODE_COUNT="$(grep -c '^\[\[server\]\]' "$CONFIG")"
# Where the proxy runs (pgbench, wait.sh and stop.sh reach it there): this machine by default.
PROXY_HOST="${PROXY_HOST:-127.0.0.1}"

# server_field ID FIELD: hostname / pg_port / ha_port of server ID.
server_field() {
  awk -v id="$1" -v f="$2" '
    /^\[\[server\]\]/ { n++; next }
    /^\[/             { n = -1 }
    n == id + 1 && $1 == f { gsub(/"/, "", $3); print $3; exit }' "$CONFIG"
}

pid_alive() { [[ -f "$1" ]] && kill -0 "$(cat "$1")" 2>/dev/null; }

# primary_json: the proxy's view of the primary ({"server": N, ...} or {"server": null}).
primary_json() { curl -s --max-time 2 "http://$PROXY_HOST:$PROXY_HTTP_PORT/primary.json"; }

# primary_id: the serving primary's server index, or nothing.
primary_id() {
  primary_json | python3 -I -c 'import json,sys
try:
    s = json.load(sys.stdin).get("server")
    print("" if s is None else s)
except Exception:
    pass'
}

# wait_nodes TIMEOUT_SECS: until every node's PostgreSQL accepts connections, reached at its
# configured hostname (so it works from the client machine too); prints each node's role.
wait_nodes() {
  local timeout=$1 n host port i role
  for ((n = 0; n < NODE_COUNT; n++)); do
    host="$(server_field "$n" hostname)" port="$(server_field "$n" pg_port)"
    for ((i = 0; i < timeout * 2; i++)); do
      pid_alive "$RUN_DIR/node$n.pid" || [[ ! -f "$RUN_DIR/node$n.pid" ]] \
        || { echo "node $n exited; see $LOG_DIR/node$n.log" >&2; return 1; }
      "$PG_BIN/pg_isready" -q -h "$host" -p "$port" -d postgres && break
      sleep 0.5
    done
    "$PG_BIN/pg_isready" -q -h "$host" -p "$port" -d postgres \
      || { echo "node $n's PostgreSQL ($host:$port) is not ready after ${timeout} s" >&2; return 1; }
    role="$(psql_val "$host" "$port" \
      "SELECT CASE WHEN pg_is_in_recovery() THEN 'standby' ELSE 'primary' END" 2>&1)"
    log "node $n: $role ($host:$port)"
  done
}

# wait_primary TIMEOUT_SECS: until the proxy reports a serving primary and the primary port
# accepts connections.
wait_primary() {
  local timeout=$1 i id
  for ((i = 0; i < timeout * 2; i++)); do
    id="$(primary_id)"
    if [[ -n "$id" ]] && "$PG_BIN/pg_isready" -q -h "$PROXY_HOST" -p "$PROXY_PG_PORT" -d postgres; then
      echo "$id"
      return 0
    fi
    sleep 0.5
  done
  return 1
}
