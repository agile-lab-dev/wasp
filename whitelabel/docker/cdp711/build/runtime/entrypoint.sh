params=$@
source $BUILD_INSTALL_SCRIPTS_DIR/common/bash-defaults.sh
bash $BUILD_TEMPLATES_DIR/resolve-hadoop-templates.sh
bash $BUILD_TEMPLATES_DIR/resolve-spark-templates.sh
bash $BUILD_TEMPLATES_DIR/resolve-kafka-templates.sh
bash $BUILD_TEMPLATES_DIR/resolve-hbase-templates.sh
bash $BUILD_TEMPLATES_DIR/resolve-zookeeper-templates.sh
bash $BUILD_TEMPLATES_DIR/resolve-solr-templates.sh


mkdir -p /var/lib/mongo/data
mkdir -p /var/log/mongo/

mongod --port 27017  --bind_ip $HOSTNAME --dbpath "/var/lib/mongo/data" &> /var/log/mongo/mongod.log &

hdfs --daemon start namenode
hdfs --daemon start datanode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager

zookeeper-server-initialize --conf /etc/zookeeper/conf/zoo.cfg --myid=1 --force

zookeeper-server start /etc/zookeeper/conf/zoo.cfg

kafka-server-start -daemon /etc/kafka/conf/server.properties

mkdir -p /var/log/hbase

set -u nounset

export JAVA_HOME=/usr/lib/jvm/jre-openjdk

/opt/parcel/lib/hbase/bin/hbase-daemon.sh --config /etc/hbase/conf/ start master
/opt/parcel/lib/hbase/bin/hbase-daemon.sh --config /etc/hbase/conf/ start regionserver

zookeeper-client create /solr
/opt/parcel/lib/solr/bin/solr start -c -m 1g -z $HOSTNAME:2181/solr -force -s /etc/solr/conf

exec $params