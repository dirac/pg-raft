#!/bin/bash
# On standby N's machine: base-backup it from the primary, configure it as s<N>, start it.
#   ./std/init_standby.sh N
set -euo pipefail
source "$(dirname "$0")/lib.sh"

id="${1:?usage: init_standby.sh N}"
(( id >= 1 && id < NODE_COUNT )) || die "standbys are 1..$((NODE_COUNT - 1))"
[[ -e "$(node_dir "$id")" ]] && die "$(node_dir "$id") exists: run ./std/prepare.sh first"
wait_ready "${HOSTS[0]}" "${PORTS[0]}" 60 || die "the primary (${HOSTS[0]}:${PORTS[0]}) is not ready"
mkdir -p "$DATA_ROOT"

log "pg_basebackup pg$id (standby s$id) from ${HOSTS[0]}:${PORTS[0]}"
"$PG_BIN/pg_basebackup" -h "${HOSTS[0]}" -p "${PORTS[0]}" -D "$(node_dir "$id")" \
  -X stream --checkpoint=fast > "$LOG_DIR/basebackup$id.log" 2>&1 \
  || { cat "$LOG_DIR/basebackup$id.log"; die "pg_basebackup failed"; }
touch "$(node_dir "$id")/standby.signal"
# application_name is how synchronous_standby_names knows it.
{
  echo "port = ${PORTS[$id]}"
  echo "primary_conninfo = 'host=${HOSTS[0]} port=${PORTS[0]} application_name=s$id'"
} >> "$(node_dir "$id")/postgresql.auto.conf"
start_pg "$id"
