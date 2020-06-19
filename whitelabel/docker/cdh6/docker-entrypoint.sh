#!/usr/bin/env bash
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

hdfs dfs -copyFromLocal /code/consumers-spark/lib/it.agilelab.wasp-spark-telemetry-plugin-*.jar /user/root/spark2/lib
hdfs dfs -copyFromLocal /code/consumers-spark/lib/it.agilelab.wasp-spark-nifi-plugin-*.jar /user/root/spark2/lib
hdfs dfs -copyFromLocal /code/consumers-spark/lib/it.agilelab.wasp-spark-nifi-plugin-bridge-*.jar /user/root/nifi/stateless

sed 's|nifi.web.proxy.context.path=|nifi.web.proxy.context.path=/proxy|g' -i nifi/conf/nifi.properties
sed 's|nifi.web.proxy.host=|nifi.web.proxy.host=localhost:2891|g' -i nifi/conf/nifi.properties

exec /usr/bin/supervisord