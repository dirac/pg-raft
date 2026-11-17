#!/bin/bash
# From the client machine, before a series of runs (and after changing pg-raft or bench/):
#   1. generate the configs (gen.sh);
#   2. build pg-raft's release binaries here, once, and copy them into $SHARED/bin;
#   3. copy bench/ to $REMOTE_DIR on every machine, this one included (the data, logs and results
#      already there are kept);
#   4. install the binaries into each copy's raft/bin.
#   ./multi/deploy.sh [--no-build]
set -euo pipefail
source "$(dirname "$0")/lib.sh"

build=1
[[ "${1:-}" == "--no-build" ]] && build=0

"$MULTI_DIR/gen.sh"

repo="$(cd "$MULTI_DIR/../.." && pwd)"
mkdir -p "$SHARED/bin" "$SHARED/results" "$SHARED/backups"
if (( build )); then
  build_log="$SHARED/build.log"
  log "building the release binaries in $repo (log: $build_log)"
  (cd "$repo" && /opt/pg_raft/bin/b2 -j"$(nproc)" variant=release \
     src/node//pg_raft_node src/node//pg_raft_initdb src/proxy//pg_raft_proxy) \
     > "$build_log" 2>&1 || { tail -30 "$build_log"; die "build failed"; }
  for b in node/pg_raft_node node/pg_raft_initdb proxy/pg_raft_proxy; do
    f=("$repo"/src/"${b%/*}"/bin/*/release/*/*/*/*/"${b#*/}")
    [[ -x "${f[0]}" ]] || die "missing ${b#*/} in $repo/src/${b%/*}/bin"
    cp "${f[0]}" "$SHARED/bin/"
  done
  echo "$(git -C "$repo" rev-parse --short HEAD)$(git -C "$repo" diff --quiet HEAD -- src || echo +changes)" \
    > "$SHARED/bin/VERSION"
  log "binaries in $SHARED/bin (pg-raft $(cat "$SHARED/bin/VERSION"))"
fi
[[ -x "$SHARED/bin/pg_raft_node" ]] || die "no binaries in $SHARED/bin: run without --no-build"

excludes=(--exclude=/data/ --exclude=/raft/data/ --exclude=/std/data/ --exclude=/solo/data/
          --exclude=logs/ --exclude=run/ --exclude=/results/ --exclude=/raft/bin/)
log "copying bench/ to $REMOTE_DIR on every machine"
mkdir -p "$REMOTE_DIR"
rsync -a "${excludes[@]}" "$MULTI_DIR/../" "$REMOTE_DIR/"
for host in "$PROXY_HOST" "${NODE_HOSTS[@]}"; do
  rsync -a -e "ssh -i $SSH_KEY -o BatchMode=yes" "${excludes[@]}" "$MULTI_DIR/../" "$host:$REMOTE_DIR/" &
done
wait

log "installing the binaries"
on_each "CONFIG=$(raft_config 5) ./raft/install_bins.sh --keep --from $SHARED/bin > /dev/null && echo ok" \
  "$CLIENT_HOST" "$PROXY_HOST" "${NODE_HOSTS[@]}"
log "deployed: client $CLIENT_HOST, proxy $PROXY_HOST, nodes ${NODE_HOSTS[*]}"
