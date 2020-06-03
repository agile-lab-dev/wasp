#!/bin/bash
set -ax

bash /usr/bin/resolve-templates.sh

service zookeeper-server start
service hadoop-hdfs-namenode start
service hadoop-hdfs-datanode start
service hbase-master start
service hbase-regionserver start
service hadoop-yarn-resourcemanager start 
service hadoop-yarn-nodemanager start 
service hadoop-mapreduce-historyserver start
service kafka-server start
service solr-server start
service mongod start
service nifi start
service nifi-registry start

tail -f  /var/log/hadoop-hdfs/hadoop-hdfs-namenode-$HOSTNAME.log  /var/log/hadoop-hdfs/hadoop-hdfs-datanode-$HOSTNAME.log  /var/log/hadoop-yarn/yarn-yarn-nodemanager-$HOSTNAME.log  /var/log/hadoop-yarn/yarn-yarn-resourcemanager-$HOSTNAME.log  /var/log/hbase/hbase-hbase-master-$HOSTNAME.log  /var/log/hbase/hbase-hbase-regionserver-$HOSTNAME.log   /var/log/zookeeper/zookeeper.log /var/log/mongodb/mongod.log /var/log/kafka/server.* /nifi/logs/*.log /nifi-registry/logs/*.log