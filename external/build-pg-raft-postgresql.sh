#!/bin/bash
# pg-raft's PostgreSQL: the source in postgresql-<version>/ (with pg-raft's WAL hooks in XLogWrite:
# wal_write_hook, wal_flush_hook, wal_timeline_hook in access/xlog.h) and the pg_raft_wal extension
# (src/pg_raft_wal), built out of tree in build/postgresql-<version>/ and installed into
# build/postgresql/<major>/ (a node's install_dir is then <repo>/build/postgresql). The stock
# PostgreSQL in /opt/pg_raft/postgresql is left alone.
#
#   ./external/build-pg-raft-postgresql.sh            configure (once), build, install
#   JOBS=16 ./external/build-pg-raft-postgresql.sh
#
# The configure options are those of install-postgresql.sh (the stock build).
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
VERSION="${VERSION:-18.7}"
MAJOR="${VERSION%%.*}"
SRC="$REPO/postgresql-$VERSION"
BUILD="$REPO/build/postgresql-$VERSION"
PREFIX="$REPO/build/postgresql/$MAJOR"
DEPS="${DEPS:-/opt/pg_raft}"  # OpenSSL, lz4 (and pkg-config) of external/install.sh
JOBS="${JOBS:-$(getconf _NPROCESSORS_ONLN)}"

export PATH="/usr/bin:/bin:$DEPS/bin:$PATH"
mkdir -p "$BUILD"
cd "$BUILD"

if [ ! -f config.status ]; then
  echo "configure (log: $BUILD/configure.log)"
  "$SRC/configure" --prefix="$PREFIX" --with-ssl=openssl --without-icu \
    --with-includes="$DEPS/include" --with-libraries="$DEPS/lib:$DEPS/lib64" \
    --without-readline --without-zlib --with-lz4 > configure.log 2>&1 \
    || { tail -20 configure.log; exit 1; }
fi

echo "build (log: $BUILD/make.log)"
make -j"$JOBS" world-bin > make.log 2>&1 || { grep -E "error" make.log | head -20; exit 1; }
make install-world-bin > install.log 2>&1 || { tail -20 install.log; exit 1; }

echo "pg_raft_wal"
make -C "$REPO/src/pg_raft_wal" PG_CONFIG="$PREFIX/bin/pg_config" install > "$BUILD/pg_raft_wal.log" 2>&1 \
  || { cat "$BUILD/pg_raft_wal.log"; exit 1; }
make -C "$REPO/src/pg_raft_wal" PG_CONFIG="$PREFIX/bin/pg_config" clean > /dev/null 2>&1 || true

echo "installed: $PREFIX ($("$PREFIX/bin/postgres" --version))"
