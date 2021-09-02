#!/bin/bash
set -axe 

source /opt/scripts/retry.sh
/opt/resolve-templates.sh

hadoop namenode -format -nonInteractive -force 
hdfs --daemon start namenode 
hdfs --daemon start datanode 

retry 5 hdfs dfs -mkdir /user
retry 5 hdfs dfs -chmod 1777 /user  
retry 5 hdfs dfs -mkdir /user/root
retry 5 hdfs dfs -chmod 1777 /user/root      
retry 5 hdfs dfs -chown mapred:hadoop /user         
retry 5 hdfs dfs -mkdir /user/history       
retry 5 hdfs dfs -chmod 1777 /user/history      
retry 5 hdfs dfs -chown mapred:hadoop /user/history         
retry 5 hdfs dfs -mkdir /tmp        
retry 5 hdfs dfs -chmod -R 1777 /tmp        
retry 5 hdfs dfs -mkdir -p hdfs:///var/log/hadoop-yarn/apps         
retry 5 hdfs dfs -chmod 1777 hdfs:///var/log/hadoop-yarn/apps       
retry 5 hdfs dfs -chown mapred:hadoop hdfs:///var/log/hadoop-yarn/apps      
retry 5 hdfs dfs -mkdir /solr       
retry 5 hdfs dfs -chown solr /solr
retry 5 hdfs dfs -mkdir /user/root/spark2
retry 5 hdfs dfs -mkdir /user/root/spark2/lib
retry 5 hdfs dfs -copyFromLocal /opt/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/lib/spark/jars/* /user/root/spark2/lib

hdfs --daemon stop datanode    
hdfs --daemon stop namenode 
