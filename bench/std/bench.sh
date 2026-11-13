#!/bin/bash
# Run pgbench against the replication cluster's primary (start.sh first).
#   ./bench.sh             initialize, then run
#   ./bench.sh --no-init   reuse the tables of a previous run
# Settings (env): SCALE CLIENTS THREADS DURATION PGBENCH_ARGS (see ../env.sh).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

skip_init=0
for a in "$@"; do
  case "$a" in
    --no-init) skip_init=1 ;;
    -h|--help) sed -n '2,6p' "$0"; exit 0 ;;
    *) die "unknown option $a" ;;
  esac
done

p=${PORTS[0]}
wait_ready 127.0.0.1 "$p" 30 || die "the primary is not running (./start.sh)"
n="$(psql_val 127.0.0.1 "$p" "SELECT count(*) FROM pg_stat_replication WHERE sync_state = 'quorum'")"
[[ "$n" == 2 ]] || die "expected 2 quorum standbys, found $n: commits would block or not be replicated"
run_pgbench std 127.0.0.1 "$p" "$skip_init"
