#!/bin/bash

WASP_CLASSPATH=`cat /wasp.classpath`:/etc/hbase/conf/

java -cp $WASP_CLASSPATH \
  -Dconfig.file=/docker-environment.conf \
  -Dwasp.process="---producer" \
  -Dwasp.akka.remote.artery.canonical.hostname=${HOSTNAME} \
  -Dwasp.akka.remote.artery.canonical.port=2892 \
  -Dlog4j.configurationFile=file:///log4j2.properties \
  it.agilelab.bigdata.wasp.producers.launcher.ProducersNodeLauncher