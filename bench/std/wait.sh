#!/bin/bash
# Wait until the cluster serves: the primary accepts connections and both standbys stream as
# quorum members. Runs from any machine (the client's too).
#   ./std/wait.sh [TIMEOUT_SECS]      default 120
set -euo pipefail
source "$(dirname "$0")/lib.sh"

log "waiting for the primary and both standbys ..."
wait_quorum "${1:-120}" || die "the cluster is not ready; see $LOG_DIR/pg*.log on the node machines"
psql_val "${HOSTS[0]}" "${PORTS[0]}" \
  "SELECT application_name || ': ' || state || ', ' || sync_state FROM pg_stat_replication ORDER BY 1"
log "primary: ${HOSTS[0]}:${PORTS[0]}"
