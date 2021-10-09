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
${WASP_HOME}/bin/wasp-whitelabel-singlenode \
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
-J-Xmx1g -J-Xms512m \
-Dlog4j.configuration=file:///log4j-single.properties \
-Dlog4j.debug=true \
-Dconfig.file=/wasp.conf \
-Dwasp.spark-streaming.additional-jars-path=/code/single/lib \
-Dwasp.process="---one---" \
-Dwasp.akka.remote.netty.tcp.hostname=${HOSTNAME} \
-Dwasp.akka.remote.netty.tcp.port=2892 \
-Dwasp.akka.cluster.roles.0=consumers-spark-streaming \
-Dwasp.akka.cluster.roles.1=master \
-Dwasp.akka.cluster.roles.2=producers \
-Dwasp.akka.cluster.roles.3=logger \
-Dwasp.akka.cluster.roles.4=consumers-spark-streaming-collaborator \
-Dwasp.akka.cluster.seed-nodes.0="akka.tcp://WASP@${HOSTNAME}:2892"