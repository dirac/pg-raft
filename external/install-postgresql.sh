#!/bin/bash

source "$(dirname -- "$0")/install-functions.sh"

VERSION=$1

MAJOR_VERSION=$(echo $VERSION | sed 's/\..*$//')

SOURCE=SOURCES/postgresql-${VERSION}.tar.gz

download https://ftp.postgresql.org/pub/source/v${VERSION}/postgresql-${VERSION}.tar.gz $SOURCE

cd BUILDS && rm -rf postgresql-${VERSION}
exit_on_error $? ${LINENO} "cd BUILDS"

gunzip -d < ../${SOURCE} | tar xvf -
exit_on_error $? ${LINENO} "gunzip -d < ../${SOURCE} | tar xvf -"

cd postgresql-${VERSION}
exit_on_error $? ${LINENO} "cd postgresql-${VERSION}"

patch src/interfaces/libpq/Makefile < ../../patch_files/postgresql.patch
exit_on_error $? ${LINENO} "patch src/interfaces/libpq/Makefile < ../../postgresql.patch"

u=$(uname)

case $u in
Darwin)
    ICU_LIBS="$ICU_LIBS -lc++"
    ;;
Linux)
    ICU_LIBS="$ICU_LIBS -lstdc++"
    ;;
*)
    exit -1
esac

./configure --prefix=${prefix}/postgresql/${MAJOR_VERSION} --with-ssl=openssl \
             --without-icu \
	          --with-libraries=${prefix}/lib:${prefix}/lib64 \
            --without-readline --without-zlib --with-lz4
exit_on_error $? ${LINENO} "./configure --prefix=${prefix}/postgresql/${MAJOR_VERSION} --with-ssl=openssl --with-includes=${INCLUDES} --with-libraries=${LIBRARIES} --without-readline --without-zlib"

make
exit_on_error $? ${LINENO} "make"

make install
exit_on_error $? ${LINENO} "make install"

cd ${DIR}
exit_on_error $? ${LINENO} 'cd ${DIR}'


