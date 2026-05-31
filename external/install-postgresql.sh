#!/bin/bash

source "$(dirname -- "$0")/install-functions.sh"

VERSION=$1
ICU_VERSION=$2

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

ICU_CFG_PATH=/opt/pg_raft/icu/78/lib/pkgconfig
ICU_CFLAGS=$(PKG_CONFIG_PATH=${ICU_CFG_PATH} pkg-config --cflags icu-uc icu-i18n)
ICU_LIBS=$(PKG_CONFIG_PATH=${ICU_CFG_PATH} pkg-config --libs icu-uc icu-i18n)

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
            --with-icu ICU_LIBS="$ICU_LIBS" ICU_CFLAGS="$ICU_CFLAGS" \
            --with-includes=${prefix}/include \
            --with-libraries=${prefix}/lib \
            --without-readline --without-zlib
exit_on_error $? ${LINENO} "./configure --prefix=${prefix}/postgresql/${MAJOR_VERSION} --with-ssl=openssl --with-includes=${INCLUDES} --with-libraries=${LIBRARIES} --without-readline --without-zlib"

make
exit_on_error $? ${LINENO} "make"

make install
exit_on_error $? ${LINENO} "make install"

cd ${DIR}
exit_on_error $? ${LINENO} 'cd ${DIR}'


