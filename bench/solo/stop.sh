#!/bin/bash
# Stop the solo PostgreSQL (on the database machine).
set -uo pipefail
source "$(dirname "$0")/lib.sh"

stop_pg
