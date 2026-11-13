# The single PostgreSQL (sourced by the solo/ scripts): no standbys, no HA. The baseline the two
# HA setups are measured against. On several machines, HOST is the database machine's address.

HOST=127.0.0.1
PORT=5632

# Where its data directory goes, on the database machine.
DATA_ROOT="${DATA_ROOT:-$SOLO_DIR/data}"

# Lines appended to pg_hba.conf: the client machine's network, when pgbench runs elsewhere.
PG_HBA=(
  # "host all all 10.0.0.0/16 trust"
)
