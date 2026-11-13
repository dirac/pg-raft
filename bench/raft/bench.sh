#!/bin/bash
# Run pgbench against the pg-raft cluster (start.sh first).
#   ./bench.sh             through the proxy's primary port (what a client would use)
#   ./bench.sh --direct    straight to the primary's PostgreSQL (no proxy hop; std's equivalent)
#   ./bench.sh --libpq     no proxy: libpq is given every node and finds the primary itself
#                          (host=a,b,c port=..., target_session_attrs=read-write), as a client
#                          that follows failovers without a proxy would
#   ./bench.sh --no-init   reuse the tables of a previous run
# Settings (env): SCALE CLIENTS THREADS DURATION PGBENCH_ARGS (see ../env.sh).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

direct=0 libpq=0 skip_init=0
for a in "$@"; do
  case "$a" in
    --direct) direct=1 ;;
    --libpq) libpq=1 ;;
    --no-init) skip_init=1 ;;
    -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
    *) die "unknown option $a" ;;
  esac
done

id="$(wait_primary 60)" || die "no primary: is the cluster running (./start.sh)?"
if (( libpq )); then
  # Every node, in config order; libpq connects to the first that accepts writes.
  host="" port=""
  for ((n = 0; n < NODE_COUNT; n++)); do
    host+="${host:+,}$(server_field "$n" hostname)"
    port+="${port:+,}$(server_field "$n" pg_port)"
  done
  export PGTARGETSESSIONATTRS=read-write
  setup=raft-libpq
elif (( direct )); then
  host="$(server_field "$id" hostname)" port="$(server_field "$id" pg_port)" setup=raft-direct
else
  host=127.0.0.1 port="$PROXY_PG_PORT" setup=raft-proxy
fi
log "primary is server $id; benchmarking $host:$port"
run_pgbench "$setup" "$host" "$port" "$skip_init"
