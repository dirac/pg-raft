#!/bin/bash
# One machine, 3 nodes: solo, std and raft, each with the disk syncs on and off, on a fresh cluster
# per (setup, syncs), pgbench at each client count in SWEEP, then one table (results/summary.md).
#   syncs on:  solo/std fsync = on;  raft leader_sync = follower_sync = true
#   syncs off: solo/std fsync = off; raft leader_sync = follower_sync = false
# raft runs each point through the proxy and direct to the primary.
# Results: results/<setup>-<sync|nosync>-n<N>-c<clients>-<time>/.
#
#   ./matrix.sh                      (about 45 minutes with the defaults)
#   SWEEP="50 100 150" ./matrix.sh
#
# Env: SWEEP (client counts, "50 100"), MAX_THREADS (pgbench -j = min(clients, this), 16), WARMUP
# (30 s per point), DURATION (120 s per point), SCALE, PG_RAFT_REPO (the pg-raft repository that
# raft/install_bins.sh builds, default ~/pg-raft). The variants' configs are generated from
# raft/config.toml, std/cluster.sh and solo/cluster.sh into variants/.
set -euo pipefail
cd "$(dirname "$0")"
export WARMUP="${WARMUP:-30}" DURATION="${DURATION:-120}"
source ./env.sh
SWEEP="${SWEEP:-50 100}"
MAX_THREADS="${MAX_THREADS:-16}"
export PG_RAFT_REPO="${PG_RAFT_REPO:-$HOME/pg-raft}"

V="$PWD/variants"
mkdir -p "$V"
sed -E 's/^(leader|follower)_sync([[:space:]]*)=[[:space:]]*false/\1_sync\2= true/' raft/config.toml \
  > "$V/raft-sync.toml"
{ cat std/cluster.sh; echo 'PG_CONF=("fsync = off")'; } > "$V/std-nosync.sh"
{ cat solo/cluster.sh; echo 'PG_CONF=("fsync = off")'; } > "$V/solo-nosync.sh"

sweep() {
  local dir=$1 n=$2 variant=$3; shift 3
  local first=1 c mode args
  for c in $SWEEP; do
    for mode in "$@"; do
      args=""
      [[ "$mode" != proxy && -n "$mode" ]] && args="--$mode"
      (( first )) || args+=" --no-init"
      first=0
      log "== $dir ($variant) n=$n clients=$c ${mode:+($mode)}"
      # shellcheck disable=SC2086
      RESULT_TAG="$variant-n$n-c$c" CLIENTS=$c THREADS=$(( c < MAX_THREADS ? c : MAX_THREADS )) \
        ./$dir/bench.sh $args
    done
  done
}

for variant in sync nosync; do
  if [[ $variant == sync ]]; then
    solo_c=$PWD/solo/cluster.sh std_c=$PWD/std/cluster.sh raft_c=$V/raft-sync.toml
  else
    solo_c=$V/solo-nosync.sh std_c=$V/std-nosync.sh raft_c=$PWD/raft/config.toml
  fi
  log "######## solo $variant"
  ( export SOLO_CLUSTER=$solo_c; ./solo/prepare.sh; ./solo/init.sh; sweep solo 1 $variant ""; ./solo/stop.sh )
  log "######## std $variant"
  ( export STD_CLUSTER=$std_c; ./std/prepare.sh; ./std/init.sh; sweep std 3 $variant ""; ./std/stop.sh )
  log "######## raft $variant"
  ( export CONFIG=$raft_c; ./raft/install_bins.sh; ./raft/init.sh; ./raft/start.sh
    sweep raft 3 $variant proxy direct; ./raft/stop.sh )
done
# The table (multi/summary.sh's, without its machine list).
bash -c "$(sed -n '/^python3 -I/,$p' multi/summary.sh)" _ "$RESULTS_DIR" | tee "$RESULTS_DIR/summary.md"
log "all done"
