#!/bin/bash

source "$(dirname -- "$0")/install-functions.sh"

VERSION=$LZ4_VERSION

SOURCE=SOURCES/lz4-$VERSION.tar.gz

download https://github.com/lz4/lz4/archive/refs/tags/v${VERSION}.tar.gz $SOURCE

cd BUILDS && rm -rf lz4-${VERSION}
exit_on_error $? ${LINENO} "cd BUILDS && rm -rf lz4-${VERSION}"

tar -xvf ../${SOURCE}
exit_on_error $? ${LINENO} "tar -xvf ../${SOURCE} | tar xvf -"

cd lz4-${VERSION}
exit_on_error $? ${LINENO} "cd openssl-${VERSION}"

make
exit_on_error $? ${LINENO} 'make'

make install PREFIX=${prefix}
exit_on_error $? ${LINENO} 'make install'

cd ${DIR}
exit_on_error $? ${LINENO} 'cd ${DIR}'

rm -rf BUILDS/lz4-${VERSION}
exit_on_error $? ${LINENO} "rm -rf BUILDS/openssl-${VERSION}"

