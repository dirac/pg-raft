#!/bin/bash
# Create the replication cluster: initdb the primary (pg0), then base-backup the two standbys
# (pg1 = s1, pg2 = s2) from it. Leaves all three running. Use reset.sh --yes to start over.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

[[ -e "$(node_dir 0)" ]] && die "$(node_dir 0) exists: run ./reset.sh --yes first"
mkdir -p "$DATA_ROOT"

# The primary. --data-checksums, as pg-raft's initdb uses.
log "initdb pg0 (primary)"
"$PG_BIN/initdb" -D "$(node_dir 0)" --data-checksums > "$LOG_DIR/initdb.log" 2>&1 \
  || { cat "$LOG_DIR/initdb.log"; die "initdb failed"; }
{
  echo
  cat "$STD_DIR/postgresql.std.conf"
  echo "port = ${PORTS[0]}"
} >> "$(node_dir 0)/postgresql.conf"
start_pg 0

# The standbys: a base backup each, then standby.signal and the connection to the primary.
# application_name is how synchronous_standby_names knows them.
for id in 1 2; do
  log "pg_basebackup pg$id (standby s$id)"
  "$PG_BIN/pg_basebackup" -h "$PRIMARY_HOST" -p "${PORTS[0]}" -D "$(node_dir "$id")" \
    -X stream --checkpoint=fast > "$LOG_DIR/basebackup$id.log" 2>&1 \
    || { cat "$LOG_DIR/basebackup$id.log"; die "pg_basebackup failed"; }
  touch "$(node_dir "$id")/standby.signal"
  {
    echo "port = ${PORTS[$id]}"
    echo "primary_conninfo = 'host=$PRIMARY_HOST port=${PORTS[0]} application_name=s$id'"
  } >> "$(node_dir "$id")/postgresql.auto.conf"
  start_pg "$id"
done

"$STD_DIR/start.sh"
