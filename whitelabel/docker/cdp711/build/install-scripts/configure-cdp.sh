#!/usr/bin/env bash

set -o errexit # exit when a command fails
set -o nounset # exit when a script tries to use undeclared variables

mkdir /libexec
mkdir /share

#for dir in bin etc include jars lib lib64 libexec meta share; do
for dir in etc include jars lib lib64 libexec meta share; do
    for f in $(ls -d /opt/parcel/${dir}/*); do

        actual_file=$f
        without_prefix=${f/\/opt\/parcel\///}  

        if [[ -d $actual_file ]]
        then
            echo "ok"
        else
           d=`dirname $without_prefix` 
           echo "creating dir $d"
           mkdir -p $d
        fi

        echo "linking ${actual_file} to ${without_prefix}"
        ln -s $actual_file $without_prefix 
    done
done

for f in  /opt/parcel/etc/atlas/conf.dist /opt/parcel/etc/bash_completion.d/conf.dist /opt/parcel/etc/cruise_control/conf.dist /opt/parcel/etc/default/conf.dist /opt/parcel/etc/druid/conf.dist /opt/parcel/etc/hadoop/conf.dist /opt/parcel/etc/hadoop-httpfs/conf.dist /opt/parcel/etc/hadoop-kms/conf.dist /opt/parcel/etc/hbase/conf.dist /opt/parcel/etc/hbase-solr/conf.dist /opt/parcel/etc/hive/conf.dist /opt/parcel/etc/hive-hcatalog/conf.dist /opt/parcel/etc/hive-webhcat/conf.dist /opt/parcel/etc/hue/conf.dist /opt/parcel/etc/impala/conf.dist /opt/parcel/etc/init.d/conf.dist /opt/parcel/etc/kafka/conf.dist /opt/parcel/etc/kafka_connect_ext/conf.dist /opt/parcel/etc/kudu/conf.dist /opt/parcel/etc/livy2/conf.dist /opt/parcel/etc/oozie/conf.dist /opt/parcel/etc/rc.d/conf.dist /opt/parcel/etc/schemaregistry/conf.dist /opt/parcel/etc/security/conf.dist /opt/parcel/etc/solr/conf.dist /opt/parcel/etc/spark/conf.dist /opt/parcel/etc/sqoop/conf.dist /opt/parcel/etc/streams_messaging_manager/conf.dist /opt/parcel/etc/streams_messaging_manager_ui/conf.dist /opt/parcel/etc/streams_replication_manager/conf.dist /opt/parcel/etc/tez/conf.dist /opt/parcel/etc/zeppelin/conf.dist /opt/parcel/etc/zookeeper/conf.dist; do
    actual_file=$f
    without_prefix=${f/\/opt\/parcel\///}  
    d=`dirname $without_prefix`
    d=$d/conf
    
    echo "linking ${actual_file} to ${d}"
    ln -s $actual_file $d

done