#!/bin/bash
set -ax

export HOSTNAME=`hostname`
echo "Rendering HOSTNAME $HOSTNAME"
cat /opt/templates/hdfs/core-site.xml | envsubst \$HOSTNAME > /etc/hadoop/conf/core-site.xml 
cat /opt/templates/hdfs/mapred-site.xml | envsubst \$HOSTNAME > /etc/hadoop/conf/mapred-site.xml 
cat /opt/templates/hdfs/yarn-site.xml | envsubst \$HOSTNAME > /etc/hadoop/conf/yarn-site.xml 
cat /opt/templates/hdfs/yarn-env.sh   | envsubst \$HOSTNAME > /etc/hadoop/conf/yarn-env.sh
cat /opt/templates/hdfs/capacity-scheduler.xml   | envsubst \$HOSTNAME > /etc/hadoop/conf/capacity-scheduler.xml

cat /opt/templates/kafka/trogdor.conf | envsubst \$HOSTNAME > /etc/kafka/conf/trogdor.conf 
cat /opt/templates/kafka/server.properties | envsubst \$HOSTNAME > /etc/kafka/conf/server.properties
cat /opt/templates/kafka/connect-distributed.properties | envsubst \$HOSTNAME > /etc/kafka/conf/connect-distributed.properties
cat /opt/templates/kafka/connect-standalone.properties | envsubst \$HOSTNAME > /etc/kafka/conf/connect-standalone.properties

cat /opt/templates/hbase/hbase-site.xml | envsubst \$HOSTNAME > /etc/hbase/conf/hbase-site.xml 
cat /opt/templates/hbase/regionservers | envsubst \$HOSTNAME > /etc/hbase/conf/regionservers 

cat /opt/templates/spark/spark-defaults.conf | envsubst \$HOSTNAME > /etc/spark/conf/spark-defaults.conf



# cat /opt/templates/hbase-env.sh | envsubst \$HOSTNAME > /etc/hbase/conf/hbase-env.sh

# 
# cat /opt/templates/yarn-site.xml | envsubst \$HOSTNAME > /etc/hadoop/conf/yarn-site.xml 
# cat /opt/templates/yarn-env.sh | envsubst \$HOSTNAME > /etc/hadoop/conf/yarn-env.sh 
# cat /opt/templates/server.properties | envsubst \$HOSTNAME > /etc/kafka/conf/server.properties
# cat /opt/templates/spark-defaults.conf | envsubst \$HOSTNAME > /etc/spark/conf/spark-defaults.conf
# cat /opt/templates/solr | envsubst \$HOSTNAME > /etc/default/solr
# 
# cat /opt/templates/spark2/spark2-shell | envsubst \$HOSTNAME > /opt/parcels/SPARK2-2.2.0.cloudera3-1.cdh5.13.3.p0.556753/bin/spark2-shell
# cat /opt/templates/spark2/spark2-submit | envsubst \$HOSTNAME > /opt/parcels/SPARK2-2.2.0.cloudera3-1.cdh5.13.3.p0.556753/bin/spark2-submit
# cat /opt/templates/spark2/spark-env.sh | envsubst \$HOSTNAME > /etc/spark2/conf/spark-env.sh
# cat /opt/templates/spark2/spark-defaults.conf | envsubst \$HOSTNAME > /etc/spark2/conf/spark-defaults.conf
# 
# cat /opt/templates/mongod.conf | envsubst \$HOSTNAME > /etc/mongod.conf
