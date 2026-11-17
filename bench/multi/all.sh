#!/bin/bash
# From the client machine: every setup in turn (solo, std 3, std 5, raft 3, raft 5), each on a
# fresh cluster with the same sweep (multi/run.sh's settings), then the summary table.
#   ./multi/all.sh [SETUP...]      e.g. ./multi/all.sh "raft 3" "raft 5"
set -euo pipefail
source "$(dirname "$0")/lib.sh"

setups=("$@")
(( ${#setups[@]} )) || setups=("solo" "std 3" "std 5" "raft 3" "raft 5")
failed=()
for s in "${setups[@]}"; do
  log "######## $s"
  # shellcheck disable=SC2086
  "$MULTI_DIR/run.sh" $s || { failed+=("$s"); log "######## $s FAILED"; "$MULTI_DIR/run.sh" stop || true; }
done
"$MULTI_DIR/summary.sh" | tee "$SHARED/results/summary.md"
(( ${#failed[@]} == 0 )) || die "failed: ${failed[*]}"
