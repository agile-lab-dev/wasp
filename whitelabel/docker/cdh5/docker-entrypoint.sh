#!/usr/bin/env bash
set -ax

apt-get update
apt-get install -y exim4

echo "example.org" > /etc/mailname

cat << 'EOF' > /etc/exim4/update-exim4.conf.conf
# /etc/exim4/update-exim4.conf.conf
#
# Edit this file and /etc/mailname by hand and execute update-exim4.conf
# yourself or use 'dpkg-reconfigure exim4-config'
#
# Please note that this is _not_ a dpkg-conffile and that automatic changes
# to this file might happen. The code handling this will honor your local
# changes, so this is usually fine, but will break local schemes that mess
# around with multiple versions of the file.
#
# update-exim4.conf uses this file to determine variable values to generate
# exim configuration macros for the configuration file.
#
# Most settings found in here do have corresponding questions in the
# Debconf configuration, but not all of them.
#
# This is a Debian specific file

dc_eximconfig_configtype='local'
dc_other_hostnames='example.org'
dc_local_interfaces='127.0.0.1'
dc_readhost=''
dc_relay_domains=''
dc_minimaldns='false'
dc_relay_nets=''
dc_smarthost=''
CFILEMODE='644'
dc_use_split_config='false'
dc_hide_mailname=''
dc_mailname_in_oh='true'
dc_localdelivery='mail_spool'
EOF

update-exim4.conf

mkdir /velocitytemplates

cat << 'EOF' > /velocitytemplates/template1.vm
TEMPLATE 1
eventId = ${eventId}
eventType = ${eventType}
severity = ${severity}
payload = ${payload}
timestamp = ${timestamp}
source = ${source}
sourceId = ${sourceId}
ruleName = ${ruleName}
EOF

cat << 'EOF' > /velocitytemplates/template2.vm
TEMPLATE 2
eventId = ${eventId}
eventType = ${eventType}
severity = ${severity}
payload = ${payload}
timestamp = ${timestamp}
source = ${source}
sourceId = ${sourceId}
ruleName = ${ruleName}
EOF

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
service exim4 start

echo "WAITING FOR HBASE MASTER TO GO UP"

sleep 60 

echo "create_namespace 'AVRO'" | hbase shell -n
echo "create 'AVRO:SCHEMA_REPOSITORY', '0'" | hbase shell -n

# hdfs dfs -copyFromLocal /code/consumers-spark/lib/it.agilelab.wasp-spark-telemetry-plugin-*.jar /user/root/spark2/lib
# hdfs dfs -copyFromLocal /code/consumers-spark/lib/org.apache.kafka.kafka-clients-0.11.0-kafka-3.0.0.jar /user/root/spark2/lib

exec /usr/bin/supervisord
