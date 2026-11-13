# The streaming-replication cluster (sourced by the std/ scripts): node 0 is the primary, nodes 1
# and 2 are the standbys s1 and s2 of synchronous_standby_names. All on this machine by default;
# on several machines, give each node its machine's address and use the same file everywhere.

HOSTS=(127.0.0.1 127.0.0.1 127.0.0.1)
PORTS=(5532 5533 5534)

# Where each machine keeps its nodes' data directories (node N in $DATA_ROOT/pgN).
DATA_ROOT="${DATA_ROOT:-$STD_DIR/data}"

# Lines appended to the primary's pg_hba.conf (initdb's own lines trust only the loopback and the
# local socket). Other machines need them: the standbys' replication connections and the client
# (pgbench). The standbys get the file through their base backups.
PG_HBA=(
  # "host replication all 10.0.0.0/16 trust"
  # "host all         all 10.0.0.0/16 trust"
)
