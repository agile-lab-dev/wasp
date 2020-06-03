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

echo "WAITING FOR HBASE MASTER TO GO UP"

sleep 20

echo "create_namespace 'AVRO'" | hbase shell -n
echo "create 'AVRO:SCHEMA_REPOSITORY', '0'" | hbase shell -n

hdfs dfs -copyFromLocal /code/consumers-spark/lib/it.agilelab.wasp-spark-telemetry-plugin-*.jar /user/root/spark2/lib
hdfs dfs -copyFromLocal /code/consumers-spark/lib/org.apache.kafka.kafka-clients-0.11.0-kafka-3.0.0.jar /user/root/spark2/lib
hdfs dfs -copyFromLocal /usr/lib/spark/jars/* /user/root/spark2/lib

exec /usr/bin/supervisord