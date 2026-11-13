#!/bin/bash
# Prepare this machine for a fresh run: stop the solo PostgreSQL and delete its data and logs.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

stop_pg
[[ -d "$PGDATA_DIR" ]] && { log "deleting $PGDATA_DIR"; rm -rf "$PGDATA_DIR"; }
rm -f "$LOG_DIR"/*.log
log "ready"
