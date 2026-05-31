#!/bin/bash

source "$(dirname -- "$0")/install-functions.sh"

SOURCE=liblmdb

cd BUILDS && rm -rf liblmdb
exit_on_error $? ${LINENO} "cd BUILDS"

cp -r ../${SOURCE} .
exit_on_error $? ${LINENO} "cp -r ../${SOURCE} ."

cd liblmdb
exit_on_error $? ${LINENO} "cd liblmdb"

sed_prefix=$(echo $prefix | sed 's/\//\\\//g')
sed 's/^prefix[[:space:]]=.*$/prefix = _SET_PREFIX_/' Makefile | sed "s/_SET_PREFIX_/${sed_prefix}/" | sed 's/^ILIBS[[:space:]]*=[[:space:]]*liblmdb.a.*$/ILIBS = liblmdb.a/' >  Makefile.sparrow
exit_on_error $? ${LINENO} "sed makefile"

make -f Makefile.sparrow
exit_on_error $? ${LINENO} "make -f Makefile.sparrow"

make -f Makefile.sparrow install
exit_on_error $? ${LINENO} "make install"

cd ${DIR}
exit_on_error $? ${LINENO} 'cd ${DIR}'

rm -r BUILDS/liblmdb
exit_on_error $? ${LINENO} 'rm -r BUILDS/liblmdb'

