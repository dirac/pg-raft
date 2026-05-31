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
${DIR}/install-lmdb.sh
${DIR}/install-toml.sh
${DIR}/install-cxxoptions.sh

${DIR}/install-openssl.sh
${DIR}/install-icu.sh 78.1
${DIR}/install-postgresql.sh 17.9 78

rm -f ${sys_prefix}/lib/*.so.*
rm -f ${sys_prefix}/lib/*.so
rm -f ${sys_prefix}/lib64/*.so.*
rm -f ${sys_prefix}/lib64/*.so

rm -f ${sys_prefix}/lib/*.dylib
rm -f ${sys_prefix}/lib64/*.dylib

cd ${DIR}

#rm -rf SOURCES
#rm -rf BUILDS

