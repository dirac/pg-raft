#!/bin/bash
# Wait until the cluster serves: the proxy (running on this machine) reports a primary, and every
# node's PostgreSQL accepts connections at its configured hostname. Run it where the proxy runs, or
# anywhere with PROXY_HOST set to the proxy's machine.
#   ./raft/wait.sh [TIMEOUT_SECS]      default 180
set -euo pipefail
source "$(dirname "$0")/lib.sh"

timeout="${1:-180}"
if [[ "$PROXY_HOST" == 127.0.0.1 ]]; then
  pid_alive "$RUN_DIR/proxy.pid" || die "the proxy isn't running here: ./raft/start_proxy.sh (or set PROXY_HOST)"
fi
log "waiting for a primary (election, then promotion) ..."
id="$(wait_primary "$timeout")" \
  || die "no primary after $timeout s; see the nodes' logs and http://$PROXY_HOST:$PROXY_HTTP_PORT/status.html"
log "waiting for every node's PostgreSQL ..."
wait_nodes "$timeout" || die "a node is not up"
log "primary: server $id ($(server_field "$id" hostname):$(server_field "$id" pg_port)); clients: -h $PROXY_HOST -p $PROXY_PG_PORT"
