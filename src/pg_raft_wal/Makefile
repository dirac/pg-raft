# pg_raft_wal: built with PGXS against pg-raft's patched PostgreSQL (postgresql-18.7, built and
# installed by external/build-pg-raft-postgresql.sh into build/postgresql/<major>):
#   make -C src/pg_raft_wal PG_CONFIG=build/postgresql/18/bin/pg_config install
MODULE_big = pg_raft_wal
OBJS = pg_raft_wal.o
PGFILEDESC = "pg_raft_wal - the WAL of a pg-raft primary, replicated through raft"

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
