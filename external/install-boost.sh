#!/bin/bash

source "$(dirname -- "$0")/install-functions.sh"

VERSION=$BOOST_VERSION
U_VERSION=$(echo $BOOST_VERSION | sed 's/\./_/g')

SOURCE=SOURCES/boost_${U_VERSION}.tar.bz2

download https://archives.boost.io/release/${VERSION}/source/boost_${U_VERSION}.tar.bz2 ${SOURCE}

cd BUILDS && rm -rf boost_${U_VERSION}
exit_on_error $? ${LINENO} "cd BUILDS && rm -rf boost_${U_VERSION}"

bunzip2 -d < ../${SOURCE} | tar xvf -
exit_on_error $? ${LINENO} "gunzip -d < ../${SOURCE} | tar xvf -"

cd boost_${U_VERSION}
exit_on_error $? ${LINENO} "cd boost_${U_VERSION}"

./bootstrap.sh --prefix=${prefix}
exit_on_error $? ${LINENO} "./bootstrap.sh --prefix=${prefix}"

./b2 --prefix=${prefix} --without-python --no-cmake-config --build-type=minimal define=BOOST_TEST_NO_MAIN define=BOOST_TEST_ALTERNATIVE_INIT_API define=BOOST_LOG_USE_COMPILER_TLS link=static visibility=hidden install
exit_on_error $? ${LINENO} "b2 --prefix=${prefix} install"

cd ${DIR}
exit_on_error $? ${LINENO} "cd ${DIR}"

rm -r BUILDS/boost_${U_VERSION}
exit_on_error $? ${LINENO} "rm -r BUILDS/boost_${U_VERSION}"

