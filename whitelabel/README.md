#START-WASP.SH usage

Pay attention to check everything marked as

    # !!! standalone applications have to ... !!! #


#YARN usage

Add this in standalone applications "consumers-spark/build.sbt":

    scriptClasspath += ":$HADOOP_CONF_DIR:$YARN_CONF_DIR"