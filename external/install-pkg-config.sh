#!/bin/bash

source "$(dirname -- "$0")/install-functions.sh"

VERSION=${PKG_CONFIG_VERSION}
SOURCE=SOURCES/pkg-config-${VERSION}.tar.gz

download https://pkgconfig.freedesktop.org/releases/pkg-config-${VERSION}.tar.gz $SOURCE

cd BUILDS && rm -rf pkg-config-${VERSION}
exit_on_error $? ${LINENO} "cd BUILDS && rm -rf pkg-config-${VERSION}"

gunzip -d < ../${SOURCE} | tar xvf -
exit_on_error $? ${LINENO} "gunzip -d < ../${SOURCE} | tar xvf -"

cd pkg-config-${VERSION}
exit_on_error $? ${LINENO} "cd pkg-config-${VERSION}"

patch glib/glib/gatomic.h < ../../patch_files/gatomic.h.patch
exit_on_error $? ${LINENO} "patch gatomic.h"

patch glib/glib/gatomic.c < ../../patch_files/gatomic.c.patch
exit_on_error $? ${LINENO} "patch gatomic.c"

patch glib/glib/gdataset.c < ../../patch_files/gdataset.c.patch
exit_on_error $? ${LINENO} "patch gdataset.c"

./configure --disable-compile-warnings --with-internal-glib --prefix=${prefix}
exit_on_error $? ${LINENO} "./configure --prefix=${prefix}"

make
exit_on_error $? ${LINENO} "make"

make install
exit_on_error $? ${LINENO} "make install"

cd ${DIR}
exit_on_error $? ${LINENO} "cd ${DIR}"

rm -rf BUILDS/pkg-config-${VERSION}
exit_on_error $? ${LINENO} "rm -r BUILDS/pkg-config-${VERSION}"

