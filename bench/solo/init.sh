#!/bin/bash
# On the database machine: initdb the single PostgreSQL, configure it and start it.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

[[ -e "$PGDATA_DIR" ]] && die "$PGDATA_DIR exists: run ./solo/prepare.sh first"
mkdir -p "$DATA_ROOT"

# --data-checksums, as pg-raft's initdb uses.
log "initdb"
"$PG_BIN/initdb" -D "$PGDATA_DIR" --data-checksums > "$LOG_DIR/initdb.log" 2>&1 \
  || { cat "$LOG_DIR/initdb.log"; die "initdb failed"; }
{
  echo
  cat "$SOLO_DIR/postgresql.solo.conf"
  echo "port = $PORT"
} >> "$PGDATA_DIR/postgresql.conf"
for line in "${PG_HBA[@]}"; do echo "$line"; done >> "$PGDATA_DIR/pg_hba.conf"
start_pg
