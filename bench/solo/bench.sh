#!/bin/bash
# Run pgbench against the single PostgreSQL (from any machine; the client's on several machines).
#   ./solo/bench.sh             initialize, then run
#   ./solo/bench.sh --no-init   reuse the tables of a previous run
# Settings (env): SCALE CLIENTS THREADS WARMUP DURATION PGBENCH_ARGS (see ../env.sh).
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

wait_ready "$HOST" "$PORT" 30 || die "PostgreSQL ($HOST:$PORT) is not ready (./solo/start.sh)"
run_pgbench solo "$HOST" "$PORT" "$skip_init"
