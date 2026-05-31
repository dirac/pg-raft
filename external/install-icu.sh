#!/bin/bash

source "$(dirname -- "$0")/install-functions.sh"

VERSION=$1

MAJOR_VERSION=$(echo $VERSION | sed 's/\..*$//')

SOURCE=SOURCES/icu-release-${VERSION}.tar.gz

download https://github.com/unicode-org/icu/archive/refs/tags/release-${VERSION}.tar.gz $SOURCE

cd BUILDS && rm -rf icu-release-${VERSION}
exit_on_error $? ${LINENO} "cd BUILDS && rm -rf icu"

gunzip -d < ../${SOURCE} | tar xvf -
exit_on_error $? ${LINENO} "gunzip -d < ../${SOURCE} | tar xvf -"

cd icu-release-${VERSION}/icu4c/source
exit_on_error $? ${LINENO} "cd icu/source"

prefix="${prefix}/icu/${MAJOR_VERSION}"

u=$(uname)

case $u in
Darwin)
    CC=clang CXX=clang++ CXXFLAGS="-std=c++2a -stdlib=libc++" ./runConfigureICU macOS --prefix=${prefix} --enable-static --disable-shared
    exit_on_error $? ${LINENO} "CC=clang CXX=clang++ CXXFLAGS=\"-std=c++14 -stdlib=libc++\" ./runConfigureICU MacOSX --prefix=${prefix}"
    ;;
Linux)
    CC=gcc CXX=g++ CXXFLAGS="-O3 -std=c++2a" LDFLAGS=-static-libstdc++ ./runConfigureICU Linux --prefix=${prefix} --enable-static --disable-shared
    exit_on_error $? ${LINENO} "CC=gcc CXX=g++ CXXFLAGS=-std=c++14 LDFLAGS=-static-libstdc++ ./runConfigureICU Linux  --prefix=${prefix}"
    ;;
*)
    exit -1
esac


make
exit_on_error $? ${LINENO} 'make'

make install
exit_on_error $? ${LINENO} 'make install'

cd ${DIR}
exit_on_error $? ${LINENO} 'cd ${DIR}'

rm -rf BUILDS/icu-release-${VERSION}
exit_on_error $? ${LINENO} "rm -rf BUILDS/icu-release-${VERSION}"

