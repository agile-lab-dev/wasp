#YARN usage

Add this in standalone applications "consumers-spark/build.sbt":

    scriptClasspath += ":$HADOOP_CONF_DIR:$YARN_CONF_DIR"