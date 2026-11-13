#!/bin/bash
# Prepare this machine for a fresh run: stop the nodes whose data directories are here and delete
# them (and the logs). Run it on every node machine, then init_primary.sh / init_standby.sh.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

for ((id = 0; id < NODE_COUNT; id++)); do
  [[ -d "$(node_dir "$id")" ]] || continue
  stop_pg "$id"
  log "deleting $(node_dir "$id")"
  rm -rf "$(node_dir "$id")"
done
rm -f "$LOG_DIR"/*.log
log "ready"
