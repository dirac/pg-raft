#!/bin/bash
# From the client machine (after deploy.sh): one setup on a fresh cluster, a pgbench sweep over
# client counts, then stop it.
#   ./multi/run.sh solo        one PostgreSQL on node 0's machine
#   ./multi/run.sh std N       streaming replication, quorum sync, N = 3 or 5 nodes
#   ./multi/run.sh raft N      pg-raft, N nodes; each point through the proxy and direct
#   ./multi/run.sh stop        stop whatever runs on the proxy and node machines
#
# Settings (env): SWEEP (client counts, "10 25 50 100"), MAX_THREADS (pgbench -j = min(clients,
# this), 8), WARMUP (30 s per point), DURATION (120 s per point), SCALE (100), RAFT_MODES
# ("proxy direct"), PGBENCH_ARGS. The tables are loaded once, at the sweep's first point.
# Results: $REMOTE_DIR/results/<setup>-n<N>-c<clients>-<time>/ here, copied to $SHARED/results.
set -euo pipefail
: "${WARMUP:=30}" "${DURATION:=120}"
source "$(dirname "$0")/lib.sh"

SWEEP="${SWEEP:-10 25 50 100}"
MAX_THREADS="${MAX_THREADS:-8}"
RAFT_MODES="${RAFT_MODES:-proxy direct}"

# stop_all: every setup's processes on the proxy and node machines (the 5-node configs name them
# all), whatever ran last.
stop_all() {
  log "stopping anything left on the proxy and node machines"
  on_each "CONFIG=$(raft_config 5) ./raft/stop.sh > /dev/null 2>&1;
           STD_CLUSTER=$(std_cluster 5) ./std/stop.sh > /dev/null 2>&1;
           SOLO_CLUSTER=$SOLO_CLUSTER_FILE ./solo/stop.sh > /dev/null 2>&1; true" \
    "$PROXY_HOST" "${NODE_HOSTS[@]}"
}

# sweep SETUP_DIR N BENCH_ENV MODES...: pgbench at each client count, in each mode (the bench.sh
# option, "" for none). The first run loads the tables.
sweep() {
  local dir=$1 n=$2 env=$3; shift 3
  local modes=("$@") first=1 c mode args
  for c in $SWEEP; do
    for mode in "${modes[@]}"; do
      args=""
      [[ -n "$mode" ]] && args="--$mode"
      (( first )) || args+=" --no-init"
      first=0
      log "== $dir n=$n clients=$c ${mode:+($mode)}"
      on "$CLIENT_HOST" "$env RESULT_TAG=n$n-c$c CLIENTS=$c THREADS=$(( c < MAX_THREADS ? c : MAX_THREADS )) \
        SCALE=$SCALE WARMUP=$WARMUP DURATION=$DURATION PGBENCH_ARGS='$PGBENCH_ARGS' ./$dir/bench.sh $args"
    done
  done
}

save_results() {
  rsync -a "$REMOTE_DIR/results/" "$SHARED/results/"
  log "results copied to $SHARED/results"
}

run_solo() {
  local env="SOLO_CLUSTER=$SOLO_CLUSTER_FILE" h=${NODE_HOSTS[0]}
  stop_all
  on "$h" "$env ./solo/prepare.sh && $env ./solo/init.sh"
  sweep solo 1 "$env" ""
  on "$h" "$env ./solo/stop.sh"
  save_results
}

run_std() {
  local n=$1 env="STD_CLUSTER=$(std_cluster "$1")"
  stop_all
  log "std, $n nodes: deleting the old data on every node machine"
  on_each "STD_CLUSTER=$(std_cluster 5) ./std/prepare.sh > /dev/null" "${NODE_HOSTS[@]}"
  on "${NODE_HOSTS[0]}" "$env ./std/init_primary.sh"
  # The standbys one at a time: concurrent pg_basebackups each force a checkpoint, which can
  # invalidate another one's temporary replication slot (the primary then terminates it).
  local i
  for ((i = 1; i < n; i++)); do
    on "${NODE_HOSTS[$i]}" "$env ./std/init_standby.sh $i" 2>&1 | sed -u "s/^/  [${NODE_HOSTS[$i]}] /"
  done
  on "$CLIENT_HOST" "$env ./std/wait.sh 300"
  sweep std "$n" "$env" ""
  on_each "$env ./std/stop.sh" $(nodes "$n")
  save_results
}

run_raft() {
  local n=$1 cfg
  cfg="$(raft_config "$1")"
  local env="CONFIG=$cfg PROXY_HOST=$PROXY_HOST"
  stop_all
  log "raft, $n nodes: deleting the backups and every node directory"
  for h in "${NODE_HOSTS[@]}"; do
    on "$h" "CONFIG=$(raft_config 5) ./raft/install_bins.sh --from $SHARED/bin" 2>&1 | sed "s/^/  [$h] /"
  done
  on "${NODE_HOSTS[0]}" "CONFIG=$cfg ./raft/init.sh"
  on_each "CONFIG=$cfg ./raft/start_node.sh {i}" $(nodes "$n")
  on "$PROXY_HOST" "CONFIG=$cfg ./raft/start_proxy.sh"
  on "$CLIENT_HOST" "$env ./raft/wait.sh 300"
  local modes=() m
  for m in $RAFT_MODES; do [[ "$m" == proxy ]] && modes+=("") || modes+=("$m"); done
  sweep raft "$n" "$env" "${modes[@]}"
  stop_raft "$n"
  save_results
}

# stop_raft N: followers first, then the primary, then the proxy.
stop_raft() {
  local n=$1 cfg primary i followers=()
  cfg="$(raft_config "$n")"
  primary="$(on "$CLIENT_HOST" "CONFIG=$cfg PROXY_HOST=$PROXY_HOST bash -c 'source raft/lib.sh; primary_id'" || true)"
  for ((i = 0; i < n; i++)); do [[ "$i" != "$primary" ]] && followers+=("$i"); done
  log "stopping raft: followers ${followers[*]}, then the primary ${primary:-?}"
  local pids=()
  for i in "${followers[@]}"; do
    on "${NODE_HOSTS[$i]}" "CONFIG=$cfg ./raft/stop.sh $i" 2>&1 | sed -u "s/^/  [${NODE_HOSTS[$i]}] /" &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p" || true; done
  [[ -n "$primary" ]] && on "${NODE_HOSTS[$primary]}" "CONFIG=$cfg ./raft/stop.sh $primary" 2>&1 \
    | sed -u "s/^/  [${NODE_HOSTS[$primary]}] /"
  on "$PROXY_HOST" "CONFIG=$cfg ./raft/stop.sh" 2>&1 | sed -u "s/^/  [$PROXY_HOST] /"
}

case "${1:-}" in
  solo) run_solo ;;
  std)  [[ "${2:-}" =~ ^[35]$ ]] || die "usage: run.sh std 3|5"; run_std "$2" ;;
  raft) [[ "${2:-}" =~ ^[35]$ ]] || die "usage: run.sh raft 3|5"; run_raft "$2" ;;
  stop) stop_all ;;
  *) sed -n '2,14p' "$0"; exit 1 ;;
esac
log "done: $*"
