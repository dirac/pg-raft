#!/bin/bash
# Start the proxy in the background: clients connect to proxy_pg_port, forwarded to the primary.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

pidf="$RUN_DIR/proxy.pid"
pid_alive "$pidf" && die "the proxy is already running (pid $(cat "$pidf"))"

# PROXY_ARGS: e.g. "--engine asio", "--threads 4" (pg_raft_proxy --help).
# shellcheck disable=SC2086
setsid nohup "$BIN/pg_raft_proxy" --config "$CONFIG" ${PROXY_ARGS:-} >> "$LOG_DIR/proxy.log" 2>&1 < /dev/null &
echo $! > "$pidf"
sleep 1
pid_alive "$pidf" || { tail -20 "$LOG_DIR/proxy.log"; die "the proxy exited (log: $LOG_DIR/proxy.log)"; }
log "proxy started (pid $(cat "$pidf"), primary port $PROXY_PG_PORT, admin http://127.0.0.1:$PROXY_HTTP_PORT/status.html)"
