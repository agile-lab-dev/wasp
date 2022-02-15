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

rm $SPARK_HOME/jars/avro-1.7.7.jar
cp /code/consumers-spark/lib/org.apache.avro.avro-1.8.2.jar $SPARK_HOME/jars/org.apache.avro.avro-1.8.2.jar

hdfs dfs -mkdir -p /user/root/spark2/lib/
hdfs dfs -put $SPARK_HOME/jars/*.jar /user/root/spark2/lib/

rm -rf /code/consumers-spark/lib/org.apache.loggin*
rm -rf /code/master/lib/org.apache.loggin*
rm -rf /code/producer/lib/org.apache.loggin*

export MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE
export MINIO_ROOT_PASSWORD=wJalrXUtnFEMIaK7MDENGabPxRfiCYEXAMPLEKEY
export MINIO_ENDPOINT=localhost:9123

/minio server --address $MINIO_ENDPOINT /miniodata &

sleep 5

AWS_ACCESS_KEY_ID=$MINIO_ROOT_USER AWS_SECRET_ACCESS_KEY=$MINIO_ROOT_PASSWORD aws --endpoint-url http://$MINIO_ENDPOINT --region eu-central-1 s3 mb s3://entity

exec /usr/bin/supervisord