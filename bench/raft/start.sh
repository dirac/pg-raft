#!/bin/bash
# Single machine: start every node and the proxy here, then wait until the cluster serves.
# (On several machines: start_node.sh <id> on each node machine, then start_proxy.sh and wait.sh
# on the client machine.)
set -euo pipefail
source "$(dirname "$0")/lib.sh"

[[ -n "$(ls -A "$BACKUP_STORE" 2>/dev/null)" ]] || die "no backup in $BACKUP_STORE: run ./raft/init.sh first"

for ((id = 0; id < NODE_COUNT; id++)); do
  pid_alive "$RUN_DIR/node$id.pid" || "$RAFT_DIR/start_node.sh" "$id"
done
pid_alive "$RUN_DIR/proxy.pid" || "$RAFT_DIR/start_proxy.sh"
"$RAFT_DIR/wait.sh"
