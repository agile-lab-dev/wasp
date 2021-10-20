#!/usr/bin/env bash

source $BUILD_INSTALL_SCRIPTS_DIR/common/bash-defaults.sh
bash $BUILD_TEMPLATES_DIR/resolve-templates.sh

hadoop-daemon.sh start namenode
hadoop-daemon.sh start datanode

yarn-daemon.sh start resourcemanager
yarn-daemon.sh start nodemanager

zookeeper-server-start.sh  -daemon $KAFKA_HOME/config/zookeeper.properties
kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties

mongod --fork -f /etc/mongod.conf

hbase-daemon.sh --config $HBASE_HOME/conf/  start  master
hbase-daemon.sh --config $HBASE_HOME/conf/  start  regionserver

solr zk mkroot /solr -z $HOSTNAME:2181
solr start -force -c -z $HOSTNAME:2181/solr -noprompt

export WASP_HOME=/code/single/
ARGS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
ARGS="$ARGS -J-Xmx1g -J-Xms512m"
ARGS="$ARGS -Dlog4j.configuration=file:///log4j-single.properties"
ARGS="$ARGS -Dlog4j.debug=true"
ARGS="$ARGS -Dconfig.file=/wasp.conf"
ARGS="$ARGS -Dwasp.spark-streaming.additional-jars-path=/code/single/lib"
ARGS="$ARGS -Dwasp.process='---one---'"
ARGS="$ARGS -Dwasp.akka.remote.netty.tcp.hostname=${HOSTNAME}"
ARGS="$ARGS -Dwasp.akka.remote.netty.tcp.port=2892"
ARGS="$ARGS -Dwasp.akka.cluster.roles.0=consumers-spark-streaming"
ARGS="$ARGS -Dwasp.akka.cluster.roles.1=master"
ARGS="$ARGS -Dwasp.akka.cluster.roles.2=producers"
ARGS="$ARGS -Dwasp.akka.cluster.roles.3=logger"
ARGS="$ARGS -Dwasp.akka.cluster.roles.4=consumers-spark-streaming-collaborator"
ARGS="$ARGS -Dwasp.akka.cluster.seed-nodes.0='akka.tcp://WASP@${HOSTNAME}:2892'"

if [ "$DROP_WASPDB" = true ] ; then
  ${WASP_HOME}/bin/wasp-whitelabel-singlenode $ARGS -- -d
fi

${WASP_HOME}/bin/wasp-whitelabel-singlenode $ARGS