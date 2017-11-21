#!/usr/bin/env bash

set -e

if [[ -z $CIRCUS_TRAIN_HOME ]]; then
  #work out the script location
  SOURCE="${BASH_SOURCE[0]}"
  while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
    SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$SCRIPT_DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
  done
  CIRCUS_TRAIN_HOME="$( cd -P "$( dirname "$SOURCE" )" && cd .. && pwd )"
fi
echo "Using $CIRCUS_TRAIN_HOME"

if [[ -z ${HIVE_LIB-} ]]; then
  export HIVE_LIB=/usr/hdp/current/hive-client/lib
fi
if [[ -z ${HCAT_LIB-} ]]; then
  export HCAT_LIB=/usr/hdp/current/hive-webhcat/share/hcatalog
fi
if [[ -z ${HIVE_CONF_PATH-} ]]; then
  export HIVE_CONF_PATH=/etc/hive/conf
fi

LIBFB303_JAR=`ls $HIVE_LIB/libfb303-*.jar | tr '\n' ':'`

CIRCUS_TRAIN_LIBS=$CIRCUS_TRAIN_HOME/lib/*\
:$HIVE_LIB/hive-exec.jar\
:$HIVE_LIB/hive-metastore.jar\
:$LIBFB303_JAR\
:$HIVE_CONF_PATH/


if [[ -z ${CIRCUS_TRAIN_CLASSPATH-} ]]; then
  export CIRCUS_TRAIN_CLASSPATH=$CIRCUS_TRAIN_LIBS
else
  export CIRCUS_TRAIN_CLASSPATH=$CIRCUS_TRAIN_CLASSPATH:$CIRCUS_TRAIN_LIBS
fi

if [[ -z ${HADOOP_CLASSPATH-} ]]; then
  export HADOOP_CLASSPATH=$CIRCUS_TRAIN_CLASSPATH
else
  export HADOOP_CLASSPATH=$CIRCUS_TRAIN_CLASSPATH:$HADOOP_CLASSPATH
fi

hadoop jar \
  $CIRCUS_TRAIN_HOME/lib/circus-train-all-latest.jar \
  com.hotels.bdp.circustrain.CircusTrain \
  "$@"

exit
