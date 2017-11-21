#!/usr/bin/env bash

set -e -u

#work out the script location
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  CIRCUS_TRAIN_BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$SCRIPT_DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
CIRCUS_TRAIN_BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

$CIRCUS_TRAIN_BIN_DIR/circus-train.sh "$@" --modules=housekeeping