#!/bin/bash
# Stop everything and delete the cluster's data directories (then ./init.sh creates a new one).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

[[ "${1:-}" == "--yes" ]] || die "this deletes $DATA_ROOT; run with --yes"
"$STD_DIR/stop.sh"
log "deleting $DATA_ROOT"
rm -rf "$DATA_ROOT"
rm -f "$LOG_DIR"/*.log
