#!/usr/bin/env bash

set -e

if [[ -z $CIRCUS_TRAIN_TOOL_HOME ]]; then
  #work out the script location
  SOURCE="${BASH_SOURCE[0]}"
  while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
    SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$SCRIPT_DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
  done
  CIRCUS_TRAIN_TOOL_HOME="$( cd -P "$( dirname "$SOURCE" )" && cd .. && pwd )"
fi
echo "Using Circus Train Tool $CIRCUS_TRAIN_TOOL_HOME"

source $CIRCUS_TRAIN_TOOL_HOME/bin/common.sh
hadoop jar \
  $CIRCUS_TRAIN_TOOL_HOME/lib/circus-train-filter-tool-* \
  com.hotels.bdp.circustrain.tool.filter.FilterTool  \
  "$@"

exit