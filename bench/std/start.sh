#!/bin/bash
# Start the primary and both standbys (init.sh first), then wait until both standbys stream and
# the primary has a synchronous quorum.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

[[ -d "$(node_dir 0)" ]] || die "no cluster in $DATA_ROOT: run ./init.sh first"
for id in $(seq 0 $((NODE_COUNT - 1))); do start_pg "$id"; done

p=${PORTS[0]}
wait_ready 127.0.0.1 "$p" 60 || die "the primary is not ready"
log "waiting for both standbys to stream ..."
for ((i = 0; i < 120; i++)); do
  n="$(psql_val 127.0.0.1 "$p" "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'")"
  [[ "$n" == 2 ]] && break
  sleep 1
done
[[ "$n" == 2 ]] || die "only $n standbys streaming after 120 s; see $LOG_DIR/pg*.log"
psql_val 127.0.0.1 "$p" "SELECT application_name || ': ' || state || ', ' || sync_state FROM pg_stat_replication ORDER BY 1"
log "primary: pg0; clients: -h 127.0.0.1 -p $p"
