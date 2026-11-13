#!/bin/bash
# Stop the cluster: the primary first (fast shutdown sends the standbys everything it wrote),
# then the standbys.
set -uo pipefail
source "$(dirname "$0")/lib.sh"

for id in $(seq 0 $((NODE_COUNT - 1))); do stop_pg "$id"; done
log "stopped"
