#!/usr/bin/env bash
set -eax

source /opt/scripts/retry.sh
mkdir -p /var/run/hadoop-hdfs
chown root /var/run/hadoop-hdfs
bash /opt/resolve-templates.sh

#/opt/scripts/hdfs/initialize-hdfs.sh
/opt/scripts/zookeeper/initialize-zookeeper.sh
/opt/scripts/start-all.sh
echo "WAITING FOR HBASE MASTER TO GO UP"

retry 8 echo "create_namespace 'AVRO'" | hbase shell -n
retry 8 echo "create 'AVRO:SCHEMA_REPOSITORY', '0'" | hbase shell -n

# hdfs dfs -copyFromLocal /code/consumers-spark/lib/it.agilelab.wasp-spark-telemetry-plugin-*.jar /user/root/spark2/lib
# hdfs dfs -copyFromLocal /code/consumers-spark/lib/org.apache.kafka.kafka-clients-0.11.0-kafka-3.0.0.jar /user/root/spark2/lib

#hdfs dfs -copyFromLocal /opt/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/lib/spark/jars/* /user/root/spark2/lib

source /usr/bin/bigtop-detect-javahome

exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf