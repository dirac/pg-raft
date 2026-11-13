#!/bin/bash
# Run pgbench against the pg-raft cluster (start.sh first).
#   ./bench.sh             through the proxy's primary port (what a client would use)
#   ./bench.sh --direct    straight to the primary's PostgreSQL (no proxy hop; std's equivalent)
#   ./bench.sh --no-init   reuse the tables of a previous run
# Settings (env): SCALE CLIENTS THREADS DURATION PGBENCH_ARGS (see ../env.sh).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

direct=0 skip_init=0
for a in "$@"; do
  case "$a" in
    --direct) direct=1 ;;
    --no-init) skip_init=1 ;;
    -h|--help) sed -n '2,7p' "$0"; exit 0 ;;
    *) die "unknown option $a" ;;
  esac
done

id="$(wait_primary 60)" || die "no primary: is the cluster running (./start.sh)?"
if (( direct )); then
  host="$(server_field "$id" hostname)" port="$(server_field "$id" pg_port)" setup=raft-direct
else
  host=127.0.0.1 port="$PROXY_PG_PORT" setup=raft-proxy
fi
log "primary is server $id; benchmarking $host:$port"
run_pgbench "$setup" "$host" "$port" "$skip_init"
