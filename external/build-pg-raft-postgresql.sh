#!/bin/bash
# pg-raft's PostgreSQL: the PostgreSQL release with pg-raft's WAL hooks
# (patch_files/postgresql-<version>-pg-raft-wal-hooks.patch: wal_write_hook, wal_flush_hook,
# wal_timeline_hook in access/xlog.h; sync_rep_wait_hook in replication/syncrep.h), and the
# pg_raft_wal extension (src/pg_raft_wal).
#
#  1. downloads postgresql-<version>.tar.gz into external/SOURCES (checked against
#     external/checksums, as install-postgresql.sh does);
#  2. extracts it into build/postgresql-<version>/ and applies patch_files/postgresql.patch (the
#     libpq Makefile, as the stock build) and the hooks patch; a stamp holds the patches' checksum,
#     and a change of either extracts the source again;
#  3. configures it (once; the options of install-postgresql.sh), builds and installs it into
#     build/postgresql/<major>/ (a node's install_dir is then <repo>/build/postgresql);
#  4. builds and installs pg_raft_wal there.
#
# build/ is not in git. The stock PostgreSQL in /opt/pg_raft/postgresql is left alone.
#
#   ./external/build-pg-raft-postgresql.sh
#   JOBS=16 VERSION=18.7 ./external/build-pg-raft-postgresql.sh
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
VERSION="${VERSION:-18.7}"
MAJOR="${VERSION%%.*}"
JOBS="${JOBS:-$(getconf _NPROCESSORS_ONLN)}"
SRC="$REPO/build/postgresql-$VERSION"
PREFIX="$REPO/build/postgresql/$MAJOR"
LIBPQ_PATCH="$REPO/external/patch_files/postgresql.patch"
HOOKS_PATCH="$REPO/external/patch_files/postgresql-$VERSION-pg-raft-wal-hooks.patch"

[ -f "$HOOKS_PATCH" ] || { echo "no hooks patch for PostgreSQL $VERSION: $HOOKS_PATCH" >&2; exit 1; }

# install-functions.sh: download(), the dependencies' prefix (/opt/pg_raft: OpenSSL, lz4,
# pkg-config) and PATH; it changes into external/.
source "$REPO/external/install-functions.sh"
DEPS="$prefix"

# 1. The release.
TARBALL="SOURCES/postgresql-$VERSION.tar.gz"
download "https://ftp.postgresql.org/pub/source/v$VERSION/postgresql-$VERSION.tar.gz" "$TARBALL"

# 2. The patched source.
STAMP="$SRC/.pg-raft-patches"
want="$(cat "$LIBPQ_PATCH" "$HOOKS_PATCH" | shasum -a 256 | cut -d' ' -f1)"
if [ "$(cat "$STAMP" 2>/dev/null)" != "$want" ]; then
  echo "extracting $TARBALL into $SRC"
  rm -rf "$SRC"
  mkdir -p "$REPO/build"
  tar xzf "$TARBALL" -C "$REPO/build"
  (cd "$SRC" && patch -s -p0 < "$LIBPQ_PATCH" && patch -s -p1 < "$HOOKS_PATCH")
  echo "$want" > "$STAMP"
fi

# 3. PostgreSQL.
cd "$SRC"
if [ ! -f config.status ]; then
  echo "configure (log: $SRC/configure.log)"
  ./configure --prefix="$PREFIX" --with-ssl=openssl --without-icu \
    --with-includes="$DEPS/include" --with-libraries="$DEPS/lib:$DEPS/lib64" \
    --without-readline --without-zlib --with-lz4 > configure.log 2>&1 \
    || { tail -20 configure.log; exit 1; }
fi
echo "build (log: $SRC/make.log)"
make -j"$JOBS" world-bin > make.log 2>&1 || { grep -E "error" make.log | head -20; exit 1; }
make install-world-bin > install.log 2>&1 || { tail -20 install.log; exit 1; }

# 4. The extension.
echo "pg_raft_wal"
make -C "$REPO/src/pg_raft_wal" PG_CONFIG="$PREFIX/bin/pg_config" install > "$SRC/pg_raft_wal.log" 2>&1 \
  || { cat "$SRC/pg_raft_wal.log"; exit 1; }
make -C "$REPO/src/pg_raft_wal" PG_CONFIG="$PREFIX/bin/pg_config" clean > /dev/null 2>&1 || true

echo "installed: $PREFIX ($("$PREFIX/bin/postgres" --version))"
