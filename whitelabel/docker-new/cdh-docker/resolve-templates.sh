#!/bin/bash
set -ax

cat ${TEMPLATE_DIR}/core-site.xml | envsubst \$HOSTNAME > /etc/hadoop/conf/core-site.xml 
cat ${TEMPLATE_DIR}/hbase-site.xml | envsubst \$HOSTNAME > /etc/hbase/conf/hbase-site.xml 
cat ${TEMPLATE_DIR}/hbase-env.sh | envsubst \$HOSTNAME > /etc/hbase/conf/hbase-env.sh
cat ${TEMPLATE_DIR}/regionservers | envsubst \$HOSTNAME > /etc/hbase/conf/regionservers 
cat ${TEMPLATE_DIR}/mapred-site.xml | envsubst \$HOSTNAME > /etc/hadoop/conf/mapred-site.xml 
cat ${TEMPLATE_DIR}/yarn-site.xml | envsubst \$HOSTNAME > /etc/hadoop/conf/yarn-site.xml 
cat ${TEMPLATE_DIR}/yarn-env.sh | envsubst \$HOSTNAME > /etc/hadoop/conf/yarn-env.sh 
cat ${TEMPLATE_DIR}/server.properties | envsubst \$HOSTNAME > /etc/kafka/conf/server.properties
cat ${TEMPLATE_DIR}/spark-defaults.conf | envsubst \$HOSTNAME > /etc/spark/conf/spark-defaults.conf
cat ${TEMPLATE_DIR}/solr | envsubst \$HOSTNAME > /etc/default/solr

cat ${TEMPLATE_DIR}/spark2/spark2-shell | envsubst \$HOSTNAME > /opt/parcels/SPARK2-2.2.0.cloudera3-1.cdh5.13.3.p0.556753/bin/spark2-shell
cat ${TEMPLATE_DIR}/spark2/spark2-submit | envsubst \$HOSTNAME > /opt/parcels/SPARK2-2.2.0.cloudera3-1.cdh5.13.3.p0.556753/bin/spark2-submit
cat ${TEMPLATE_DIR}/spark2/spark-env.sh | envsubst \$HOSTNAME > /etc/spark2/conf/spark-env.sh
cat ${TEMPLATE_DIR}/spark2/spark-defaults.conf | envsubst \$HOSTNAME > /etc/spark2/conf/spark-defaults.conf

cat ${TEMPLATE_DIR}/mongod.conf | envsubst \$HOSTNAME > /etc/mongod.conf