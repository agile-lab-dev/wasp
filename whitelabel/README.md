# Whitelabel - Standalone application example

## Start scripts usage
(see `docker/start-whitelabel-wasp.sh`, `docker/start-wasp.sh`)

In `docker/start-wasp.sh`, check everything marked as:

    # !!! standalone applications have to ... !!! #

## Custom Node Launcher usage
(see `*/build.sbt`)

Add these in standalone applications:

`master/build.sbt`

    mainClass in Compile := Some("it.agilelab.bigdata.wasp.APPLICATION_NAME.master.launcher.MasterNodeLauncher")

`producers/build.sbt`

    mainClass in Compile := Some("it.agilelab.bigdata.wasp.APPLICATION_NAME.producers.launcher.ProducersNodeLauncher")

`consumers-rt/build.sbt`

    mainClass in Compile := Some("it.agilelab.bigdata.wasp.APPLICATION_NAME.consumers.rt.launcher.RtConsumersNodeLauncher")

`consumers-spark/build.sbt`

    mainClass in Compile := Some("thisClassNotExist")
    //mainClass in Compile := Some("it.agilelab.bigdata.wasp.APPLICATION_NAME.consumers.spark.launcher.SparkConsumersStreamingNodeLauncher")
    //mainClass in Compile := Some("it.agilelab.bigdata.wasp.APPLICATION_NAME.consumers.spark.launcher.SparkConsumersBatchNodeLauncher")

    // to use within "docker run" in start-wasp.sh using -main FULLY_QUALIFIED_NAME

## Spark distributed-mode usage
(see `consumers-spark/build.sbt`)

Add this in standalone applications:
    
    import java.io.File
    
    mappings in Universal += {
      val log = streams.value.log
    
      log.info("Getting jars names to use with additional-jars-lib-path config parameter (used by Wasp Core Framework)")
    
      // get full classpaths of all jars
      val jars = (fullClasspath in Compile).value.map(dep => {
        val moduleOpt = dep.metadata.get(AttributeKey[ModuleID]("moduleID"))
        moduleOpt match {
          case Some(module) =>
            if (module.organization.equalsIgnoreCase("it.agilelab")) {
              //for some reason, the snapshot version is not appended correctly. Must do it manually
              s"${module.organization}.${module.name}-${module.revision}.jar"
            } else
              s"${module.organization}.${dep.data.getName}"
    
          case None =>
            log.warn(s"Dependency $dep does not have a valid ModuleID associated.")
            dep.data.getName
        }
      }).mkString("\n")
    
      val jarsListFileName = "jars.list"
    
      val file = new File(IO.createTemporaryDirectory.getAbsolutePath + File.separator + jarsListFileName)
      IO.write(file, jars)
    
      file -> ("lib" + File.separator + jarsListFileName)
    }


## Hadoop YARN usage
(see `consumers-spark/build.sbt`)

Add this in standalone applications:

    scriptClasspath += ":$HADOOP_CONF_DIR:$YARN_CONF_DIR"