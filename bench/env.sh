# Shared settings for the two setups (sourced by the scripts in raft/ and std/).
#
#   raft/  PostgreSQL + pg-raft: 3 nodes, quorum commit through raft, clients via pg_raft_proxy
#   std/   PostgreSQL streaming replication: 1 primary + 2 standbys, quorum sync replication
#
# Every value can be overridden from the environment, e.g. SCALE=500 ./raft/bench.sh

TESTING_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PG_BIN="${PG_BIN:-/opt/pg_raft/postgresql/17/bin}"

# The backups (NFS). pg-raft's backup store lives here; std takes no backups.
BACKUP_ROOT="${BACKUP_ROOT:-/mnt/efs/fs1/pg_raft}"

# pgbench defaults (see bench.sh --help).
SCALE="${SCALE:-100}"          # pgbench -i -s: 100 = 10M accounts, ~1.5 GB
CLIENTS="${CLIENTS:-16}"
THREADS="${THREADS:-4}"
DURATION="${DURATION:-300}"    # seconds, measured
# An unmeasured run of the same workload right before the measured one (0: none), so that the
# start-up (caches, connections, the first segments, pg-raft's first rounds) isn't counted.
# The cluster is already serving (wait.sh): elections and promotion are before it.
WARMUP="${WARMUP:-60}"         # seconds
PGBENCH_ARGS="${PGBENCH_ARGS:-}"  # extra arguments for the run, e.g. "-N" or "-S"
PGBENCH_INIT_ARGS="${PGBENCH_INIT_ARGS:-}"  # extra arguments for the load, e.g. "-I dtGvp" (rows
                                            # generated on the server: much faster at scale 1000)

RESULTS_DIR="${RESULTS_DIR:-$TESTING_DIR/results}"

# The PostgreSQL settings both setups must share for a fair comparison. raft/config.toml's
# [pg_config] and std/postgresql.std.conf each carry them; bench.sh records the settings that were
# actually in effect next to every result. pg-raft forces wal_sync_method = fsync and
# wal_recycle = off (its FUSE layer needs them), so std uses them too.
COMPARED_SETTINGS="shared_buffers max_connections max_wal_size checkpoint_timeout wal_sync_method \
wal_recycle wal_init_zero synchronous_commit fsync full_page_writes wal_compression wal_buffers \
synchronous_standby_names data_checksums server_version"

log() { echo "[$(date '+%H:%M:%S')] $*"; }
die() { echo "error: $*" >&2; exit 1; }

# wait_ready HOST PORT TIMEOUT_SECS: until PostgreSQL accepts connections.
wait_ready() {
  local host=$1 port=$2 timeout=$3 i
  for ((i = 0; i < timeout * 2; i++)); do
    "$PG_BIN/pg_isready" -q -h "$host" -p "$port" -d postgres && return 0
    sleep 0.5
  done
  return 1
}

# psql_val HOST PORT SQL: one value, no decoration.
psql_val() {
  "$PG_BIN/psql" -X -A -t -q -h "$1" -p "$2" -d postgres -c "$3"
}

# run_pgbench SETUP HOST PORT: initialize, then run; results in $RESULTS_DIR/<setup>-<time>/.
run_pgbench() {
  local setup=$1 host=$2 port=$3 skip_init=$4
  # RESULT_TAG (e.g. n3-c50) goes into the directory name.
  local out="$RESULTS_DIR/$setup${RESULT_TAG:+-$RESULT_TAG}-$(date +%Y%m%d-%H%M%S)"
  mkdir -p "$out"

  {
    echo "setup=$setup host=$host port=$port tag=${RESULT_TAG:-}"
    echo "scale=$SCALE clients=$CLIENTS threads=$THREADS warmup=$WARMUP duration=$DURATION args=$PGBENCH_ARGS"
    echo "date=$(date -Is) host=$(hostname) nproc=$(nproc)"
    for s in $COMPARED_SETTINGS; do
      echo "$s = $(psql_val "$host" "$port" "SHOW $s" 2>/dev/null)"
    done
  } > "$out/settings.txt"

  if [[ "$skip_init" != 1 ]]; then
    log "pgbench -i -s $SCALE $PGBENCH_INIT_ARGS (log: $out/init.log)"
    # shellcheck disable=SC2086
    "$PG_BIN/pgbench" -h "$host" -p "$port" -i -s "$SCALE" -q $PGBENCH_INIT_ARGS postgres \
      > "$out/init.log" 2>&1 \
      || { cat "$out/init.log"; die "pgbench -i failed"; }
    # A checkpoint, so that the run doesn't start with the load's dirty pages.
    psql_val "$host" "$port" "CHECKPOINT" > /dev/null
  fi

  if (( WARMUP > 0 )); then
    log "warm-up: pgbench -c $CLIENTS -j $THREADS -T $WARMUP $PGBENCH_ARGS, not measured (log: $out/warmup.log)"
    # shellcheck disable=SC2086
    "$PG_BIN/pgbench" -h "$host" -p "$port" -c "$CLIENTS" -j "$THREADS" -T "$WARMUP" \
      -P 10 $PGBENCH_ARGS postgres > "$out/warmup.log" 2>&1 \
      || { cat "$out/warmup.log"; die "pgbench (warm-up) failed"; }
  fi

  log "pgbench -c $CLIENTS -j $THREADS -T $DURATION $PGBENCH_ARGS (log: $out/run.log)"
  # shellcheck disable=SC2086
  "$PG_BIN/pgbench" -h "$host" -p "$port" -c "$CLIENTS" -j "$THREADS" -T "$DURATION" \
    -P 10 -r $PGBENCH_ARGS postgres > "$out/run.log" 2>&1 \
    || { cat "$out/run.log"; die "pgbench failed"; }

  grep -E "^(number of transactions actually processed|latency average|latency stddev|tps)" \
    "$out/run.log" | tee "$out/summary.txt"
  log "results in $out"
}
