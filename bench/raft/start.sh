#!/bin/bash
# Start every node on this machine and the proxy, then wait for a serving primary.
#   ./start.sh            all nodes in config.toml
#   ./start.sh 0 2        only nodes 0 and 2 (when the others run on other machines)
set -euo pipefail
source "$(dirname "$0")/lib.sh"

[[ -n "$(ls -A "$BACKUP_STORE" 2>/dev/null)" ]] || die "no backup in $BACKUP_STORE: run ./init.sh first"

ids=("$@")
(( ${#ids[@]} )) || ids=($(seq 0 $((NODE_COUNT - 1))))
for id in "${ids[@]}"; do
  pid_alive "$RUN_DIR/node$id.pid" || "$RAFT_DIR/start_node.sh" "$id"
done
pid_alive "$RUN_DIR/proxy.pid" || "$RAFT_DIR/start_proxy.sh"

log "waiting for a primary (election, then promotion) ..."
id="$(wait_primary 180)" || die "no primary after 180 s; see $LOG_DIR/node*.log and http://127.0.0.1:$PROXY_HTTP_PORT/status.html"

# Every node started here must be up too: a primary and one follower are a quorum, so a node that
# failed (see its log) would otherwise go unnoticed and the cluster would run on 2 nodes.
log "waiting for every node's PostgreSQL ..."
for n in "${ids[@]}"; do
  port="$(server_field "$n" pg_port)"
  for ((i = 0; i < 240; i++)); do
    pid_alive "$RUN_DIR/node$n.pid" || die "node $n exited; see $LOG_DIR/node$n.log"
    "$PG_BIN/pg_isready" -q -h 127.0.0.1 -p "$port" -d postgres && break
    sleep 0.5
  done
  "$PG_BIN/pg_isready" -q -h 127.0.0.1 -p "$port" -d postgres \
    || die "node $n's PostgreSQL (port $port) is not ready after 120 s; see $LOG_DIR/node$n.log"
  role="$(psql_val 127.0.0.1 "$port" "SELECT CASE WHEN pg_is_in_recovery() THEN 'standby' ELSE 'primary' END")"
  log "node $n: $role on port $port"
done
log "primary: server $id (pg_port $(server_field "$id" pg_port)); clients: -h 127.0.0.1 -p $PROXY_PG_PORT"
