#!/bin/bash
set -axe 
hadoop namenode -format -nonInteractive -force 
hdfs --daemon start namenode 
hdfs --daemon start datanode 
hdfs dfs -mkdir /user       
hdfs dfs -chmod 1777 /user  
hdfs dfs -mkdir /user/root
hdfs dfs -chmod 1777 /user/root      
hdfs dfs -chown mapred:hadoop /user         
hdfs dfs -mkdir /user/history       
hdfs dfs -chmod 1777 /user/history      
hdfs dfs -chown mapred:hadoop /user/history         
hdfs dfs -mkdir /tmp        
hdfs dfs -chmod -R 1777 /tmp        
hdfs dfs -mkdir -p hdfs:///var/log/hadoop-yarn/apps         
hdfs dfs -chmod 1777 hdfs:///var/log/hadoop-yarn/apps       
hdfs dfs -chown mapred:hadoop hdfs:///var/log/hadoop-yarn/apps      
hdfs dfs -mkdir /solr       
hdfs dfs -chown solr /solr      
hdfs --daemon stop datanode    
hdfs --daemon stop namenode 