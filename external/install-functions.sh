
PKG_CONFIG_VERSION=0.29.1
OPENSSL_VERSION=3.6.0
BOOST_VERSION=1.90.0
CXXOPTS_VERSION=3.3.1
TOML_VERSION=4.4.0

function exit_on_error {
  if [ "$1" -ne "0" ]; then
    echo "FAILED $2: $3 (${PWD})";
    exit $1;
  fi 
}

function download {
  local src=$1
  local dst=$2
  local chk=$(echo "${dst}.sha256" | sed 's/^[^/]*\//checksums\//')

  if shasum -a 256 -c "$chk" 1> /dev/null 2> /dev/null
  then
    return
  fi

  echo "Downloading $src"
  curl --no-progress-meter -L -o "$dst" "$src"
  exit_on_error $? ${LINENO} "curl -L -o $dst $src"

  if [ -f "$chk" ]; then
    shasum -a 256 -c "$chk" > /dev/null
    exit_on_error $? ${LINENO} "shasum -a 256 -c $chk"
  else
    echo "Creating sha256 file: $chk"
    shasum -a 256 "$dst" > "$chk"
  fi
}

DIR=$(dirname -- "$0")
cd "$DIR"
DIR=${PWD}

prefix=/opt/pg_raft

export PATH=/usr/bin:/bin:${prefix}/bin:${PATH}
unset CDPATH

mkdir -p SOURCES
exit_on_error $? ${LINENO} "mkdir -p SOURCES"

mkdir -p BUILDS
exit_on_error $? ${LINENO} "mkdir -p BUILDS"

