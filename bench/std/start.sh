#!/bin/bash
# Start the nodes whose data directories are on this machine (the primary first). On a single
# machine, then wait for the quorum (on several, run wait.sh on the client once all are up).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

started=0
for ((id = 0; id < NODE_COUNT; id++)); do
  local_node "$id" && { start_pg "$id"; started=$((started + 1)); }
done
(( started )) || die "no node data here ($DATA_ROOT): run init_primary.sh / init_standby.sh first"
(( started == NODE_COUNT )) && "$STD_DIR/wait.sh"
exit 0
