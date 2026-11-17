#!/bin/bash
# On the primary's machine: initdb node 0, configure it and start it.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

[[ -e "$(node_dir 0)" ]] && die "$(node_dir 0) exists: run ./std/prepare.sh first"
mkdir -p "$DATA_ROOT"

# --data-checksums, as pg-raft's initdb uses.
log "initdb pg0 (primary)"
"$PG_BIN/initdb" -D "$(node_dir 0)" --data-checksums > "$LOG_DIR/initdb.log" 2>&1 \
  || { cat "$LOG_DIR/initdb.log"; die "initdb failed"; }
{
  echo
  cat "$STD_DIR/postgresql.std.conf"
  echo "port = ${PORTS[0]}"
  echo "synchronous_standby_names = '$(sync_standby_names)'"
  # PG_CONF (cluster file, optional): more settings, e.g. "fsync = off".
  for line in ${PG_CONF[@]+"${PG_CONF[@]}"}; do echo "$line"; done
} >> "$(node_dir 0)/postgresql.conf"
# The extra lines go after initdb's (which only match the loopback and local sockets).
for line in "${PG_HBA[@]}"; do echo "$line"; done >> "$(node_dir 0)/pg_hba.conf"
start_pg 0
