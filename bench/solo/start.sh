#!/bin/bash
# Start the solo PostgreSQL (on the database machine).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

[[ -f "$PGDATA_DIR/PG_VERSION" ]] || die "no data in $PGDATA_DIR: run ./solo/init.sh first"
start_pg
