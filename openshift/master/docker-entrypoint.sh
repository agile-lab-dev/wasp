#!/bin/bash

WASP_CLASSPATH=`cat /wasp.classpath`:/etc/hbase/conf/

java -cp $WASP_CLASSPATH \
  -Dconfig.file=/docker-environment.conf \
  -Dwasp.process="---master" \
  -Dwasp.akka.remote.netty.tcp.hostname=${HOSTNAME} \
  -Dwasp.akka.remote.netty.tcp.port=2892 \
  -Dlog4j.configurationFile=file:///log4j2.properties \
  it.agilelab.bigdata.wasp.master.launcher.MasterNodeLauncher