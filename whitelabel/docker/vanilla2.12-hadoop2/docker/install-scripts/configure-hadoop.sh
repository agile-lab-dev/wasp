#!/usr/bin/env bash

source ./common/bash-defaults.sh

echo "Creating directories needed by hadoop"
mkdir -p /opt/{hadoop/{logs},hdfs/{datanode,namenode},yarn/{logs}

bash $BUILD_TEMPLATES_DIR/resolve-templates.sh 
hdfs namenode -format -nonInteractive -force
hadoop-daemon.sh start namenode
hadoop-daemon.sh start datanode
echo "creating user dir" 
hdfs dfs -mkdir /user 
hdfs dfs -chmod 1777 /user 
hdfs dfs -chown mapred:hadoop /user
echo "creating history dir" 
hdfs dfs -mkdir /user/history 
hdfs dfs -chmod 1777 /user/history 
hdfs dfs -chown mapred:hadoop /user/history 
echo "creating temp directory" 
hdfs dfs -mkdir /tmp 
hdfs dfs -chmod -R 1777 /tmp 
echo "creating yarn log directory" 
hdfs dfs -mkdir -p hdfs:///var/log/hadoop-yarn/apps 
hdfs dfs -chmod 1777 hdfs:///var/log/hadoop-yarn/apps 
hdfs dfs -chown mapred:hadoop hdfs:///var/log/hadoop-yarn/apps 
echo "creating solr directory" 
hdfs dfs -mkdir /solr 
hdfs dfs -chown solr /solr 
echo "creating spark directory" 
hdfs dfs -mkdir -p /user/root/spark2/lib/
hdfs dfs -put $SPARK_HOME/jars/*.jar /user/root/spark2/lib/