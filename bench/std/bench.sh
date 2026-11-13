#!/bin/bash
# Run pgbench against the primary (from any machine; the client's on several machines).
#   ./std/bench.sh             initialize, then run
#   ./std/bench.sh --no-init   reuse the tables of a previous run
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

# Both standbys must be quorum members: otherwise commits block, or aren't replicated.
wait_quorum 30 || die "the cluster is not ready (./std/wait.sh)"
run_pgbench std "${HOSTS[0]}" "${PORTS[0]}" "$skip_init"
