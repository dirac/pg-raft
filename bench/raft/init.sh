#!/bin/bash
# Create the cluster once: initdb + the first backup, written into backup_store.
# pg_raft_initdb refuses a store that already holds a backup (use reset.sh --yes first).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

mkdir -p "$DATA_ROOT" "$BACKUP_STORE"
log "pg_raft_initdb --config $CONFIG (backup store: $BACKUP_STORE, log: $LOG_DIR/initdb.log)"
if ! "$BIN/pg_raft_initdb" --config "$CONFIG" > "$LOG_DIR/initdb.log" 2>&1; then
  grep -E "ERROR|FATAL" "$LOG_DIR/initdb.log" | tail -5 >&2
  die "pg_raft_initdb failed (log: $LOG_DIR/initdb.log)"
fi
log "cluster created; backups in $BACKUP_STORE:"
ls -1 "$BACKUP_STORE"
