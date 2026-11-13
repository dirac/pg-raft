#!/bin/bash
# Single machine: stop the cluster and delete its data (prepare.sh), so that init.sh can create a
# new one.
set -euo pipefail
[[ "${1:-}" == "--yes" ]] || { echo "error: this deletes the cluster's data; run with --yes" >&2; exit 1; }
exec "$(dirname "$0")/prepare.sh"
