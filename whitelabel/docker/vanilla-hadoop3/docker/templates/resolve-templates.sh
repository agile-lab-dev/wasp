#!/usr/bin/env bash

set -o errexit # exit when a command fails
set -o nounset # exit when a script tries to use undeclared variables
set -o xtrace # trace what gets executed
set -o pipefail # fail pipeline if a command part of a pipeline fails

cat ${BUILD_TEMPLATES_DIR}/hadoop/core-site.xml | envsubst \$HOSTNAME > $HADOOP_CONF_DIR/core-site.xml 
cat ${BUILD_TEMPLATES_DIR}/hadoop/mapred-site.xml | envsubst \$HOSTNAME > $HADOOP_CONF_DIR/mapred-site.xml 
cat ${BUILD_TEMPLATES_DIR}/hadoop/yarn-site.xml | envsubst \$HOSTNAME > $HADOOP_CONF_DIR/yarn-site.xml

KAFKA_HOME=${KAFKA_HOME:-}

if [ "$KAFKA_HOME" ]; then
  echo "KAFKA SET"
  cat ${BUILD_TEMPLATES_DIR}/kafka/server.properties | envsubst \$HOSTNAME > $KAFKA_HOME/config/server.properties
fi

cat ${BUILD_TEMPLATES_DIR}/mongo/mongod.conf | envsubst \$HOSTNAME > /etc/mongod.conf

HBASE_HOME=${HBASE_HOME:-}

if [ "$HBASE_HOME" ]; then
  echo "HBASE SET"

  cat ${BUILD_TEMPLATES_DIR}/hbase/hbase-site.xml | envsubst \$HOSTNAME > $HBASE_HOME/conf/hbase-site.xml
  cat ${BUILD_TEMPLATES_DIR}/hbase/regionservers | envsubst \$HOSTNAME > $HBASE_HOME/conf/regionservers
fi

SOLR_HOME=${SOLR_HOME:-}

if [ "$SOLR_HOME" ]; then
  echo "SOLR SET"
  cat ${BUILD_TEMPLATES_DIR}/solr/solr.in.sh | envsubst \$HOSTNAME > $SOLR_HOME/bin/solr.in.sh
fi

#cat ${BUILD_TEMPLATES_DIR}/yarn-env.sh | envsubst \$HOSTNAME > $HADOOP_HOME/etc/hadoop/yarn-env.sh

#cat ${BUILD_TEMPLATES_DIR}/spark-defaults.conf | envsubst \$HOSTNAME > $SPARK_HOME/conf/spark-defaults.conf
#cat ${BUILD_TEMPLATES_DIR}/solr.in.sh | envsubst \$HOSTNAME > $SOLR_HOME/bin/solr.in.sh
