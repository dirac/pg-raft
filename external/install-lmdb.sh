#!/bin/bash

source "$(dirname -- "$0")/install-functions.sh"

VERSION=$LMDB_VERSION

SOURCE=SOURCES/LMDB_${VERSION}.tar.gz

download "https://github.com/LMDB/lmdb/archive/refs/tags/LMDB_${VERSION}.tar.gz" ${SOURCE}

cd BUILDS && rm -rf lmdb-LMDB_${VERSION}
exit_on_error $? ${LINENO} "cd BUILDS"

gunzip -d < ../${SOURCE} | tar xvf -
exit_on_error $? ${LINENO} "gunzip -d < ../${SOURCE} | tar xvf -"

cd "lmdb-LMDB_${VERSION}/libraries/liblmdb"
exit_on_error $? ${LINENO} "cd liblmdb"

sed_prefix=$(echo $prefix | sed 's/\//\\\//g')
sed 's/^prefix[[:space:]]=.*$/prefix = _SET_PREFIX_/' Makefile | \
sed "s/_SET_PREFIX_/${sed_prefix}/" | \
sed 's/^ILIBS2[[:space:]]*=.*$/ILIBS2 = /' \
  >  Makefile.pg_raft
exit_on_error $? ${LINENO} "sed makefile"

make -f Makefile.pg_raft
exit_on_error $? ${LINENO} "make -f Makefile.pg_raft"

make -f Makefile.pg_raft install
exit_on_error $? ${LINENO} "make install"

cd ${DIR}
exit_on_error $? ${LINENO} 'cd ${DIR}'

rm -r BUILDS/lmdb-LMDB_${VERSION}
exit_on_error $? ${LINENO} 'rm -r BUILDS/liblmdb'

