#!/bin/bash
# Build pg-raft's release binaries and copy them into raft/bin, which the other scripts run.
# A copy, so that rebuilding the repository (e.g. a debug build) doesn't change what a benchmark
# runs.  PG_RAFT_REPO defaults to the repository this script is in.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

repo="${PG_RAFT_REPO:-$(cd "$RAFT_DIR/../.." && pwd)}"
[[ -f "$repo/Jamroot.jam" ]] || die "no pg-raft repository at $repo (set PG_RAFT_REPO)"

log "building the release binaries in $repo"
(cd "$repo" && /opt/pg_raft/bin/b2 -j"$(nproc)" variant=release \
   src/node//pg_raft_node src/node//pg_raft_initdb src/proxy//pg_raft_proxy) \
   > "$LOG_DIR/build.log" 2>&1 || { tail -30 "$LOG_DIR/build.log"; die "build failed"; }

mkdir -p "$BIN"
for f in "$repo"/src/node/bin/*/release/*/*/*/*/pg_raft_node \
         "$repo"/src/node/bin/*/release/*/*/*/*/pg_raft_initdb \
         "$repo"/src/proxy/bin/*/release/*/*/*/*/pg_raft_proxy; do
  [[ -x "$f" ]] || die "missing $f"
  cp "$f" "$BIN/"
done
log "installed into $BIN:"
ls -l "$BIN"
