#!/bin/bash

source "$(dirname -- "$0")/install-functions.sh"

VERSION=$CXXOPTS_VERSION
SOURCE=SOURCES/cxxopts-${VERSION}.tar.gz

download https://github.com/jarro2783/cxxopts/archive/refs/tags/v${VERSION}.tar.gz $SOURCE

cd BUILDS && rm -rf cxxopts-${VERSION}
exit_on_error $? ${LINENO} "cd BUILDS"

gunzip -d < ../${SOURCE} | tar xvf -
exit_on_error $? ${LINENO} "gunzip -d < ../${SOURCE} | tar xvf -"

cd cxxopts-${VERSION}
exit_on_error $? ${LINENO} "cd cxxopts-${VERSION}"

cp include/cxxopts.hpp ${prefix}/include/.
exit_on_error $? ${LINENO} "cp include/cxxopts.hpp ${prefix}/include/."

cd ${DIR}
exit_on_error $? ${LINENO} 'cd ${DIR}'

rm -r BUILDS/cxxopts-${VERSION}
exit_on_error $? ${LINENO} 'rm -r BUILDS/cxxopts-${VERSION}'

