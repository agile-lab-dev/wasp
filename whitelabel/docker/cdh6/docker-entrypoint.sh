#!/usr/bin/env bash
set -ax

bash /usr/bin/resolve-templates.sh

sed 's|nifi.web.proxy.context.path=|nifi.web.proxy.context.path=/proxy|g' -i nifi/conf/nifi.properties
sed 's|nifi.web.proxy.host=|nifi.web.proxy.host=localhost:2891|g' -i nifi/conf/nifi.properties

service nifi start
service nifi-registry start
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

hdfs dfs -copyFromLocal /code/consumers-spark/lib/it.agilelab.wasp-spark-telemetry-plugin-*.jar /user/root/spark2/lib
hdfs dfs -copyFromLocal /code/consumers-spark/lib/it.agilelab.wasp-spark-nifi-plugin-*.jar /user/root/spark2/lib
hdfs dfs -copyFromLocal /code/consumers-spark/lib/it.agilelab.wasp-spark-nifi-plugin-bridge-*.jar /user/root/nifi/stateless

/nifi-toolkit/bin/cli.sh nifi     create-reg-client -u http://localhost:8080/nifi-api  --registryClientName wasp --registryClientUrl http://localhost:18080/nifi-registry
/nifi-toolkit/bin/cli.sh registry create-bucket -u http://localhost:18080 --bucketName wasp-bucket

exec /usr/bin/supervisord