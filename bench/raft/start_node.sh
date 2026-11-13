#!/bin/bash
# Start node ID in the background: ./start_node.sh ID
# On its first start a node restores the latest backup, then mounts FUSE over its pg_wal, starts
# PostgreSQL as a standby and joins raft; the elected leader promotes its PostgreSQL.
# On several machines, run this on each one with its own ID (the same config.toml everywhere).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

id="${1:?usage: start_node.sh ID}"
(( id < NODE_COUNT )) || die "no server $id in $CONFIG ($NODE_COUNT servers)"
pidf="$RUN_DIR/node$id.pid"
pid_alive "$pidf" && die "node $id is already running (pid $(cat "$pidf"))"

mkdir -p "$DATA_ROOT"
# No core dumps: the last node of a cluster that stops can't commit PostgreSQL's shutdown
# checkpoint (no quorum left), so the node fails its fsync and PostgreSQL PANICs on purpose.
# systemd-coredump would spend ~30 s on each core (shared_buffers is in it).
ulimit -c 0
setsid nohup "$BIN/pg_raft_node" --config "$CONFIG" --id "$id" >> "$LOG_DIR/node$id.log" 2>&1 < /dev/null &
echo $! > "$pidf"
sleep 2
pid_alive "$pidf" || { tail -20 "$LOG_DIR/node$id.log"; die "node $id exited (log: $LOG_DIR/node$id.log)"; }
log "node $id started (pid $(cat "$pidf"), pg_port $(server_field "$id" pg_port), log $LOG_DIR/node$id.log)"
