#!/usr/bin/env bash

source $BUILD_INSTALL_SCRIPTS_DIR/common/bash-defaults.sh
bash $BUILD_TEMPLATES_DIR/resolve-templates.sh

hdfs --daemon start namenode
hdfs --daemon start datanode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager

zookeeper-server-start.sh  -daemon $KAFKA_HOME/config/zookeeper.properties
kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties

mongod --fork -f /etc/mongod.conf

hbase-daemon.sh --config $HBASE_HOME/conf/  start  master
hbase-daemon.sh --config $HBASE_HOME/conf/  start  regionserver

solr zk mkroot /solr -z $HOSTNAME:2181
solr start -force -c -z $HOSTNAME:2181/solr -noprompt

bash