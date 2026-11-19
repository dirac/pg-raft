#!/bin/bash

set -e

#
# Create a directory /opt/pg_raft, with self as the owner with write permissions.
#
source "$(dirname -- "$0")/install-functions.sh"

rm -rf /opt/pg_raft/*

mkdir /opt/pg_raft/lib64
mkdir /opt/pg_raft/lib

rm -rf BUILDS

mkdir -p SOURCES
mkdir -p BUILDS

${DIR}/install-pkg-config.sh
${DIR}/install-b2.sh
${DIR}/install-boost.sh
${DIR}/install-toml.sh
${DIR}/install-cxxoptions.sh


rm -f ${prefix}/lib/*.so.*
rm -f ${prefix}/lib/*.so
rm -f ${prefix}/lib64/*.so.*
rm -f ${prefix}/lib64/*.so

rm -f ${prefix}/lib/*.dylib
rm -f ${prefix}/lib64/*.dylib

${DIR}/install-openssl.sh
${DIR}/install-postgresql.sh 18.7 78

cd ${DIR}

#rm -rf SOURCES
#rm -rf BUILDS

