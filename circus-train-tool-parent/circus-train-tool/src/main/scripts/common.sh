#!/usr/bin/env bash


if [ -z ${CIRCUS_TRAIN_HOME-} ] && [ -d "/opt/circus-train" ] ; then
  export CIRCUS_TRAIN_HOME=/opt/circus-train
fi

if [[ -z ${CIRCUS_TRAIN_HOME-} ]]; then
  echo "Set CIRCUS_TRAIN_HOME then run Tool"
  exit
fi

echo "Using Circus Train Home $CIRCUS_TRAIN_HOME"

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

CIRCUS_TRAIN_LIBS=$CIRCUS_TRAIN_HOME/lib/*:$HIVE_LIB/hive-exec.jar:$HIVE_LIB/hive-metastore.jar:$LIBFB303_JAR:$HIVE_CONF_PATH

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