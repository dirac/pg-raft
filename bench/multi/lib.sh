# Sourced by the multi/ scripts: the benchmark spread over several machines, driven from the
# client machine (the one running pgbench).
#
#   client  this machine: pgbench, and the driver (multi/*.sh)
#   proxy   the first address in $MACHINES: pg_raft_proxy
#   nodes   the next five: PostgreSQL (solo: node 0; std and raft: nodes 0..N-1, N = 3 or 5)
#
# Every machine gets a copy of bench/ in $REMOTE_DIR, on its local disk (data, logs, results);
# $SHARED, mounted on every machine, holds the binaries, pg-raft's backup store and a copy of the
# results.
MULTI_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$MULTI_DIR/../env.sh"

MACHINES="${MACHINES:-$HOME/machines.txt}"     # one address per line: the proxy, then the nodes
SSH_KEY="${SSH_KEY:-$HOME/studio.pem}"
REMOTE_DIR="${REMOTE_DIR:-/data/bench}"        # bench/ on every machine, this one included
SHARED="${SHARED:-$HOME/shared/pg_raft}"       # the same path on every machine
NETWORK="${NETWORK:-172.31.0.0/16}"            # trusted in pg_hba (client, proxy, replication)
GEN_DIR="$MULTI_DIR/gen"                       # generated configs (copied with bench/)

mapfile -t _machines < <(grep -Eo '^[0-9.]+' "$MACHINES")
PROXY_HOST="${_machines[0]}"
NODE_HOSTS=("${_machines[@]:1}")
CLIENT_HOST="$(hostname -I | awk '{print $1}')"
(( ${#NODE_HOSTS[@]} >= 5 )) || die "$MACHINES: need the proxy's address and 5 node machines'"

# The configs, by their path on every machine.
raft_config()  { echo "$REMOTE_DIR/multi/gen/raft-$1.toml"; }
std_cluster()  { echo "$REMOTE_DIR/multi/gen/std-$1.sh"; }
SOLO_CLUSTER_FILE="$REMOTE_DIR/multi/gen/solo.sh"

# on HOST CMD: run the shell command CMD in $REMOTE_DIR on HOST (locally on this machine).
on() {
  local host=$1 cmd=$2
  if [[ "$host" == "$CLIENT_HOST" ]]; then
    (cd "$REMOTE_DIR" && bash -c "$cmd")
  else
    ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$host" \
      "cd $REMOTE_DIR && $cmd" < /dev/null
  fi
}

# on_each CMD HOST...: CMD on every HOST at once, output prefixed with the host; fails if any does.
# In CMD, {i} is replaced by the host's position in the list.
on_each() {
  local cmd=$1; shift
  local pids=() i=0 host rc=0
  for host in "$@"; do
    on "$host" "${cmd//\{i\}/$i}" 2>&1 | sed -u "s/^/  [$host] /" &
    pids+=($!)
    i=$((i + 1))
  done
  for p in "${pids[@]}"; do wait "$p" || rc=1; done
  return $rc
}

# The first N node machines.
nodes() { echo "${NODE_HOSTS[@]:0:$1}"; }
