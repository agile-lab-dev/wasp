#!/usr/bin/env bash
set -ax

bash /usr/bin/resolve-templates.sh

/opt/scripts/start-all.sh

echo "WAITING FOR HBASE MASTER TO GO UP"

sleep 20

echo "create_namespace 'AVRO'" | hbase shell -n
echo "create 'AVRO:SCHEMA_REPOSITORY', '0'" | hbase shell -n

# hdfs dfs -copyFromLocal /code/consumers-spark/lib/it.agilelab.wasp-spark-telemetry-plugin-*.jar /user/root/spark2/lib
# hdfs dfs -copyFromLocal /code/consumers-spark/lib/org.apache.kafka.kafka-clients-0.11.0-kafka-3.0.0.jar /user/root/spark2/lib

source /usr/bin/bigtop-detect-javahome

exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf