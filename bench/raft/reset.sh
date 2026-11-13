#!/bin/bash
# Stop everything and delete the cluster: every node's directory and the backup store.
# Needed before init.sh can create a new cluster (e.g. after changing [pg_config]).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

[[ "${1:-}" == "--yes" ]] || die "this deletes $DATA_ROOT and $BACKUP_STORE; run with --yes"
"$RAFT_DIR/stop.sh"
log "deleting $DATA_ROOT and $BACKUP_STORE"
rm -rf "$DATA_ROOT" "$BACKUP_STORE"
rm -f "$LOG_DIR"/*.log
