#!/bin/bash
# Stop the proxy and the nodes on this machine (SIGTERM: PostgreSQL stops, raft hands over,
# FUSE unmounts). A node that doesn't exit in 90 s is killed and its FUSE mount removed.
#   ./stop.sh          everything started from here
#   ./stop.sh 1        only node 1
set -uo pipefail
source "$(dirname "$0")/lib.sh"

stop_pid() {  # stop_pid NAME PIDFILE
  local name=$1 pidf=$2 pid i
  pid_alive "$pidf" || { rm -f "$pidf"; return 0; }
  pid="$(cat "$pidf")"
  log "stopping $name (pid $pid)"
  kill -TERM "$pid"
  for ((i = 0; i < 180; i++)); do
    kill -0 "$pid" 2>/dev/null || { rm -f "$pidf"; return 0; }
    sleep 0.5
  done
  log "$name did not stop: killing it"
  kill -KILL "$pid" 2>/dev/null
  rm -f "$pidf"
}

# Followers first, the primary last: fewer hand-overs on the way down. (Asked before the proxy,
# which knows the primary, goes.)
primary="$(primary_id 2>/dev/null)"
ids=("$@")
if (( ${#ids[@]} == 0 )); then
  stop_pid proxy "$RUN_DIR/proxy.pid"
  ids=($(seq 0 $((NODE_COUNT - 1))))
fi
for id in "${ids[@]}"; do [[ "$id" != "$primary" ]] && stop_pid "node $id" "$RUN_DIR/node$id.pid"; done
for id in "${ids[@]}"; do [[ "$id" == "$primary" ]] && stop_pid "node $id" "$RUN_DIR/node$id.pid"; done

# Leftovers of a killed node: its FUSE mount, and a PostgreSQL still holding the data directory.
for id in "${ids[@]}"; do
  mnt="$DATA_ROOT/node$id/data/pg_wal"
  if mount | grep -q " $mnt type fuse"; then
    log "unmounting the leftover FUSE mount $mnt"
    fusermount -u -z "$mnt"
  fi
  pmf="$DATA_ROOT/node$id/data/postmaster.pid"
  if [[ -f "$pmf" ]] && kill -0 "$(head -1 "$pmf")" 2>/dev/null; then
    log "stopping the leftover PostgreSQL of node $id"
    "$PG_BIN/pg_ctl" -D "$DATA_ROOT/node$id/data" -m immediate stop > /dev/null 2>&1
  fi
done
log "stopped"
