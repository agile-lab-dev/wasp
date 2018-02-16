mainClass in Compile := Some("thisClassNotExist")
//mainClass in Compile := Some("it.agilelab.bigdata.wasp.whitelabel.consumers.spark.launcher.SparkConsumersStreamingNodeLauncher")
//mainClass in Compile := Some("it.agilelab.bigdata.wasp.whitelabel.consumers.spark.launcher.SparkConsumersBatchNodeLauncher")

// to use within "docker run" in start-wasp.sh using -main FULLY_QUALIFIED_NAME


/* !!! YARN usage: Add this in standalone applications !!! */
scriptClasspath += ":$HADOOP_CONF_DIR:$YARN_CONF_DIR"