mainClass in Compile := Some("thisClassNotExist")
//mainClass in Compile := Some("it.agilelab.bigdata.wasp.consumers.spark.launcher.SparkConsumersStreamingNodeLauncher")
//mainClass in Compile := Some("it.agilelab.bigdata.wasp.consumers.spark.launcher.SparkConsumersBatchNodeLauncher")

// to use within "docker run" in start-wasp.sh using -main FULLY_QUALIFIED_NAME