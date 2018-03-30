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

## Spark distributed-mode (Hadoop YARN, Spark Standalone) usage
(see `consumers-spark/build.sbt`)

Add this in standalone applications:
    
    import java.io.File
    
    mappings in Universal += {
      val jarsListFileName = "jars.list"
    
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
    
      val file = new File(IO.createTemporaryDirectory.getAbsolutePath + File.separator + jarsListFileName)
      IO.write(file, jars)
    
      file -> ("lib" + File.separator + jarsListFileName)
    }


## Hadoop YARN usage
(see `consumers-spark/build.sbt`)

Add this in standalone applications:

    scriptClasspath += ":$HADOOP_CONF_DIR:$YARN_CONF_DIR"
    
## Checkpoint and stateful transformation in Spark StructuredStreaming ETL

(see

- `TestCheckpointConsoleWriterStructuredJSONPipegraph` / `TestCheckpointConsoleWriterStructuredAVROPipegraph` in `whitelabel/models/test/TestPipegraphs.scala`

- `whitelabel/consumers/spark/strategies/test/TestCheckpointStrategy.scala`

- `whitelabel/models/test/TestCheckpointState.scala`
)

Inside the *checkpointDir* (default: `/checkpoint` on HDFS) are automatically stored:
 
- the offsets / commits related to the Kafka queue positions already managed by the Spark StructuredStreaming ETL: this should not be a critical point, also changing the transform's behavior.

- the user-defined per-group states related to stateful transformations of the Spark StructuredStreaming ETL (e.g. `flatMapGroupsWithState`).

**"Best-practise"**

- Start creating a State class flat (`TestCheckpointState`) and create an `implicit val` Kryo Encoder based on it (`implicit val testCheckpointStateEncoder`)

- In Strategy's transform (`TestCheckpointStrategy`)
    - Add the import of the implicit val created above

            import TestCheckpointState.implicits._
    
    - Use the created State flat as Encoder `S` of the stateful transformation function (`update`): `GroupState[TestCheckpointState]`

- From now on, create incremental State case-class specializations (`TestCheckpointStateV1`, `TestCheckpointStateV2`, ...) and Strategy specializations (`TestCheckpointJSONStrategyV1`, `TestCheckpointJSONStrategyV2`, ...), one for each new version have to be released

- During the stream coming, in stateful transformation function (`update`), the group will automatically continue to retrieve, if already present, the old State specialization
    - Manage the different State specializations (`pattern matching`), **all old ones and a fallback** (the latter is required to prevent issues with Strategy rollbacks, which should however be avoided).
    
      From this point it is possible, for instance, to simultaneously maintain several (all old ones and the new one) State specializations (`TestCheckpointJSONStrategyV2`) or to manage the state transitions, aligning all old State specializations to the new one (`TestCheckpointJSONStrategyV3`, `TestCheckpointJSONStrategyV4`)
        
- **Note:** In Strategy's transform (`TestCheckpointStrategy`), adding a further stateful transformation (es. `flatMapGroupsWithState`) based on the result of the already existing one, requires to clean-up the *checkpointDir* folders `offsets`, `commits` and `state`.
            
    The reason is that the sub-folder `state/0` exists, related to the already present stateful transformation, whereas the fresh new created sub-folder `state/1`, related to the new stateful transformation,
    still does not exist (i.e. does not contain files `.delta` / `.snapshot`, see [checkpoint-State](../documentation/checkpoint.md#state)) despite the "checkpointing process" of this Spark StructuredStreaming ETL expecting it.

    For sake of clarity:

        // Passing from:
        
        groupedDataset.flatMapGroupsWithState[TestCheckpointState, TestCheckpointDocument](OutputMode.Append, GroupStateTimeout.NoTimeout)(func = update).toDF


        // To:
        
        groupedDataset
          .flatMapGroupsWithState[TestCheckpointState, TestCheckpointDocument](OutputMode.Append, GroupStateTimeout.NoTimeout)(func = update)
          .groupByKey(_.id)
          .flatMapGroupsWithState[TestCheckpointState, TestCheckpointDocument](OutputMode.Append, GroupStateTimeout.NoTimeout)(func = update)
          .toDF
         
        // throws "java.lang.IllegalStateException: Error reading delta file - Caused by: java.io.FileNotFoundException: File does not exist" until checkpointDir sub-folders are cleaned-up