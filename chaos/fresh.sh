#!/bin/bash
# A fresh chaos run: build pg-raft (optimized, assertions on), stop whatever runs here, archive the
# logs, create a new cluster and start chaos.py run in the background.
#   ./chaos/fresh.sh [HOURS]       default 3
set -euo pipefail
C="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$C/.." && pwd)"
B2="${B2:-/opt/pg_raft/bin/b2}"

# Optimized, but the debug variant: assertions stay on (-O0 binaries are too slow for 200 ms
# election timeouts: they stall heartbeats and churn elections).
(cd "$REPO" && "$B2" -j"${JOBS:-8}" optimization=speed \
  src/node//pg_raft_node src/node//pg_raft_initdb src/proxy//pg_raft_proxy > "$C/build.log" 2>&1) \
  || { tail -20 "$C/build.log"; echo "build failed (log: $C/build.log)" >&2; exit 1; }
find_bin() { find "$REPO/src/$1/bin" -path "*optimization-speed*" -name "$2" -type f -perm -u+x | head -1; }

cd "$C"
pkill -f "chaos.py run" || true
sleep 1
[ -f config.toml ] && python3 chaos.py stop || true
sleep 3
rm -rf data backups acked.txt unknown.txt next_id proxy.pid
# System V shared memory of PostgreSQLs killed with their data directories deleted: nothing else
# reclaims it (a postmaster reclaims only its own directory's segment), and macOS allows 32. Only
# segments nothing is attached to (ipcrm marks an attached one for removal: a running PostgreSQL's).
if [ "$(uname)" = Darwin ]; then
  for id in $(ipcs -m -o | awk -v u="$USER" '$1=="m" && $5==u && $7==0 {print $2}'); do
    ipcrm -m "$id" 2>/dev/null || true
  done
fi

mkdir -p old bin
[ -d logs ] && mv logs "old/logs.$(date +%Y%m%d-%H%M%S)"
mkdir logs
sed "s|@DIR@|$C|g" config.toml.in > config.toml
cp "$(find_bin node pg_raft_node)" "$(find_bin node pg_raft_initdb)" "$(find_bin proxy pg_raft_proxy)" bin/

python3 chaos.py setup > logs/setup.out 2>&1 || { tail -5 logs/setup.out; exit 1; }
python3 chaos.py stop
nohup python3 -u chaos.py run "${1:-3}" > logs/run.out 2>&1 &
echo "chaos run started (${1:-3} h): tail -f $C/logs/events.log"
