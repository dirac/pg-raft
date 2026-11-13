#!/bin/bash
# Stop the nodes on this machine: the primary first (a fast shutdown sends the standbys
# everything it wrote), then the standbys.
set -uo pipefail
source "$(dirname "$0")/lib.sh"

for ((id = 0; id < NODE_COUNT; id++)); do local_node "$id" && stop_pg "$id"; done
log "stopped"
