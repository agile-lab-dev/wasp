#!/usr/bin/env bash
set -eax

bash /usr/bin/resolve-templates.sh

if [ -n "$NIFI_PROXY_PATH" ]
  then
    sed "s|nifi.web.proxy.context.path=|nifi.web.proxy.context.path=$NIFI_PROXY_PATH|g" -i nifi/conf/nifi.properties
  else
    sed "s|nifi.web.proxy.context.path=|nifi.web.proxy.context.path=/proxy|g" -i nifi/conf/nifi.properties
fi

if [ -n "$NIFI_PROXY_HOST" ]
  then
    sed "s|nifi.web.proxy.host=|nifi.web.proxy.host=$NIFI_PROXY_HOST|g" -i nifi/conf/nifi.properties
  else
    sed "s|nifi.web.proxy.host=|nifi.web.proxy.host=localhost:2891|g" -i nifi/conf/nifi.properties
fi

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

hdfs dfs -copyFromLocal /code/single/lib/it.agilelab.wasp-spark-telemetry-plugin-*.jar /user/root/spark2/lib
hdfs dfs -copyFromLocal /code/single/lib/it.agilelab.wasp-spark-nifi-plugin-*.jar /user/root/spark2/lib
hdfs dfs -copyFromLocal /code/single/lib/it.agilelab.wasp-spark-nifi-plugin-bridge-*.jar /user/root/nifi/stateless

#/nifi-toolkit/bin/cli.sh nifi     create-reg-client -u http://localhost:8080/nifi-api  --registryClientName wasp --registryClientUrl http://localhost:18080/nifi-registry
#/nifi-toolkit/bin/cli.sh registry create-bucket -u http://localhost:18080 --bucketName wasp-bucket

/confluent-6.0.0/bin/schema-registry-start /confluent-6.0.0/etc/schema-registry/schema-registry.properties &

export HADOOP_CONF_DIR="/etc/hadoop/conf:/etc/hbase/conf",
export HOSTNAME=$HOSTNAME

export WASP_HOME=/code/single/
${WASP_HOME}/bin/wasp-whitelabel-singlenode \
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
-J-Xmx1g -J-Xms512m \
-Dlog4j.configurationFile=file:///log4j2.properties \
-Dconfig.file=/wasp.conf \
-Dwasp.spark-streaming.additional-jars-path=/code/single/lib \
-Dwasp.process="---one---" \
-Dwasp.akka.remote.netty.tcp.hostname=${HOSTNAME} \
-Dwasp.akka.remote.netty.tcp.port=2892 \
-Dwasp.akka.cluster.roles.0=consumers-spark-streaming \
-Dwasp.akka.cluster.roles.1=master \
-Dwasp.akka.cluster.roles.2=producers \
-Dwasp.akka.cluster.roles.3=logger \
-Dwasp.akka.cluster.seed-nodes.0="akka.tcp://WASP@${HOSTNAME}:2892"