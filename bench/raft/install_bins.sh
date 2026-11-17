#!/bin/bash
# Prepare this machine for a fresh run: build pg-raft's release binaries and copy them into
# raft/bin (a copy, so that rebuilding the repository doesn't change what a benchmark runs), then
# delete what a previous cluster left:
#   - every backup in backup_store (shared by all the nodes: a new cluster's init.sh refuses a
#     store that holds one, and a node restores the latest one);
#   - this machine's node directories under `dir` (a node with a data directory doesn't restore
#     the new cluster's backup) and the logs.
#
#   ./raft/install_bins.sh          on every node machine, before init.sh
#   ./raft/install_bins.sh --keep   only the binaries (the client machine: proxy + pgbench)
#   ./raft/install_bins.sh --from DIR   copy the binaries from DIR (built once, e.g. on a shared
#                                       mount) instead of building them here
#
# Run it on every node machine first, then init.sh once: a later install_bins.sh would delete the
# new cluster's backup. PG_RAFT_REPO defaults to the repository this script is in.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

keep=0 from=""
for a in "$@"; do
  [[ "$from" == next ]] && { from="$a"; continue; }
  case "$a" in
    --keep) keep=1 ;;
    --from) from=next ;;
    --from=*) from="${a#--from=}" ;;
    -h|--help) sed -n '2,18p' "$0"; exit 0 ;;
    *) die "unknown option $a" ;;
  esac
done

if (( !keep )); then
  for f in "$RUN_DIR"/node*.pid; do
    pid_alive "$f" && die "a node is running here ($(basename "$f" .pid)): ./raft/stop.sh first"
  done
fi

mkdir -p "$BIN"
if [[ -n "$from" ]]; then
  [[ "$from" != next ]] || die "--from needs a directory"
  for b in pg_raft_node pg_raft_initdb pg_raft_proxy; do
    [[ -x "$from/$b" ]] || die "missing $from/$b"
    cp "$from/$b" "$BIN/"
  done
else
  repo="${PG_RAFT_REPO:-$(cd "$RAFT_DIR/../.." && pwd)}"
  [[ -f "$repo/Jamroot.jam" ]] || die "no pg-raft repository at $repo (set PG_RAFT_REPO)"
  log "building the release binaries in $repo"
  (cd "$repo" && /opt/pg_raft/bin/b2 -j"$(nproc)" variant=release \
     src/node//pg_raft_node src/node//pg_raft_initdb src/proxy//pg_raft_proxy) \
     > "$LOG_DIR/build.log" 2>&1 || { tail -30 "$LOG_DIR/build.log"; die "build failed"; }
  for f in "$repo"/src/node/bin/*/release/*/*/*/*/pg_raft_node \
           "$repo"/src/node/bin/*/release/*/*/*/*/pg_raft_initdb \
           "$repo"/src/proxy/bin/*/release/*/*/*/*/pg_raft_proxy; do
    [[ -x "$f" ]] || die "missing $f"
    cp "$f" "$BIN/"
  done
fi
log "installed into $BIN: $(cd "$BIN" && echo *)"

(( keep )) && exit 0

if [[ -d "$BACKUP_STORE" ]]; then
  stale=("$BACKUP_STORE"/* "$BACKUP_STORE"/.[!.]*)
  stale=($(ls -d "${stale[@]}" 2>/dev/null || true))
  if (( ${#stale[@]} )); then
    log "deleting ${#stale[@]} stale entries from $BACKUP_STORE"
    rm -rf "${stale[@]}"
  fi
else
  log "backup_store $BACKUP_STORE is not reachable from here; not cleaned"
fi
for d in "$DATA_ROOT"/node*; do
  [[ -d "$d" ]] || continue
  mount | grep -q " $d/data/pg_wal type fuse" && fusermount -u -z "$d/data/pg_wal"
  log "deleting the old node directory $d"
  rm -rf "$d"
done
rm -f "$LOG_DIR"/node*.log "$LOG_DIR"/proxy.log "$LOG_DIR"/initdb.log
log "ready: run ./raft/init.sh once (on one node machine), then ./raft/start_node.sh <id> on each"
