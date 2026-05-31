#!/bin/bash

source "$(dirname -- "$0")/install-functions.sh"

VERSION=${BOOST_VERSION}
SOURCE=SOURCES/build-boost-${VERSION}.tar.gz

download https://github.com/boostorg/build/archive/boost-${VERSION}.tar.gz $SOURCE


cd BUILDS && rm -rf build-boost-${VERSION}
exit_on_error $? ${LINENO} "cd BUILDS && rm -rf build-boost-${VERSION}"

gunzip -d < ../${SOURCE} | tar xvf -
exit_on_error $? ${LINENO} "gunzip -d < ../${SOURCE} | tar xvf -"

cd build-boost-${VERSION}
exit_on_error $? ${LINENO} "cd build-boost-${VERSION}"

./bootstrap.sh
exit_on_error $? ${LINENO} './bootstrap.sh'

rm -rf ${prefix}/share/b2
exit_on_error $? ${LINENO} 'rm -rf ${prefix}/share/b2'

./b2 install --prefix=${prefix}
exit_on_error $? ${LINENO} "./b2 install --prefix=${prefix}"

cd ${DIR}
exit_on_error $? ${LINENO} "cd ${DIR}"

rm -r BUILDS/build-boost-${VERSION}
exit_on_error $? ${LINENO} "rm -rf BUILDS/build-boost-${VERSION}"

