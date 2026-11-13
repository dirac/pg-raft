#!/bin/bash
# Stop the solo PostgreSQL and delete its data (prepare.sh), so that init.sh can create a new one.
set -euo pipefail
[[ "${1:-}" == "--yes" ]] || { echo "error: this deletes the data; run with --yes" >&2; exit 1; }
exec "$(dirname "$0")/prepare.sh"
