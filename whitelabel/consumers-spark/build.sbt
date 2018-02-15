mainClass in Compile := Some("it.agilelab.bigdata.wasp.whitelabel.consumers.spark.launcher.SparkConsumersNodeLauncher")

/* !!! YARN usage: Add this in standalone applications !!! */
scriptClasspath += ":$HADOOP_CONF_DIR:$YARN_CONF_DIR"