#!/bin/bash

WASP_CLASSPATH=`cat /wasp.classpath`:/etc/hbase/conf/:/etc/hadoop/conf/

java -cp $WASP_CLASSPATH \
  -Dconfig.file=/docker-environment.conf \
  -Dwasp.process="---streaming" \
  -Dwasp.akka.remote.netty.tcp.hostname=${HOSTNAME} \
  -Dwasp.akka.remote.netty.tcp.port=2892 \
  -Dlog4j.configurationFile=file:///log4j2.properties \
  it.agilelab.bigdata.wasp.consumers.spark.launcher.SparkConsumersStreamingNodeLauncher