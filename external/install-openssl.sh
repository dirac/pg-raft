#!/bin/bash

source "$(dirname -- "$0")/install-functions.sh"

VERSION=$OPENSSL_VERSION

SOURCE=SOURCES/openssl-$VERSION.tar.gz

download https://www.openssl.org/source/openssl-${VERSION}.tar.gz $SOURCE

cd BUILDS && rm -rf openssl-${VERSION}
exit_on_error $? ${LINENO} "cd BUILDS && rm -rf openssl-${VERSION}"

tar -xvf ../${SOURCE}
exit_on_error $? ${LINENO} "tar -xvf ../${SOURCE} | tar xvf -"

cd openssl-${VERSION}
exit_on_error $? ${LINENO} "cd openssl-${VERSION}"

./Configure --prefix=${prefix} --openssldir=${prefix}/openssl no-shared
exit_on_error $? ${LINENO} "./Configure --prefix=${prefix} --openssldir=${prefix}/openssl no-shared"

make
exit_on_error $? ${LINENO} 'make'

make install
exit_on_error $? ${LINENO} 'make install'

cd ${DIR}
exit_on_error $? ${LINENO} 'cd ${DIR}'

rm -rf BUILDS/openssl-${VERSION}
exit_on_error $? ${LINENO} "rm -rf BUILDS/openssl-${VERSION}"

