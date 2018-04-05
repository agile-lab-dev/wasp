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
    

## Configuration validation rules
(see `/whitelabel/docker/docker-environment.conf`)

WASP defines a set of configuration validation rules to be validated in order to be launched.

These validation rules are defined at global-level (by `validationRules` in `ConfigManager.scala`) and at plugin-level (by `WaspConsumersSparkPlugin.getValidationRules()` exposed by each plugin).

The following configurations allow to alter the default behavior of WASP related to validation rules:
- `environment.validationRulesToIgnore`: list of validation rules to ignore (through validation rule's keys) (**default: []**)
- For all not ignored validation rules: print VALIDATION-RESULT (validation rule's keys and PASSED/NOT PASSED); if there is at least a validation failure (NOT PASSED):
    - `environment.mode` == "develop": print VALIDATION-WARN and continue
    - `environment.mode` != "develop" (all not "develop" is considered "production" by default): print VALIDATION-ERROR and exit (**default: "production"**)

## Procedure of Strategy version upgrade

(see

- `TestCheckpointConsoleWriterStructuredJSONPipegraph` / `TestCheckpointConsoleWriterStructuredAVROPipegraph` in `whitelabel/models/test/TestPipegraphs.scala`

- `whitelabel/consumers/spark/strategies/test/TestCheckpointStrategy.scala`

- `whitelabel/models/test/TestCheckpointState.scala`
)

Inside the *checkpointDir* (default: `/checkpoint` on HDFS) are automatically stored:
 
- **offsets** / **commits** related to the Kafka queue positions already managed by the Spark StructuredStreaming ETL (e.g. `flatMapGroups`)
   
  Consequently, a Strategy based only on these sub-folders (i.e. Strategy's transform containing only stateless transformations of the Spark StructuredStreaming ETL) should not be really critical, also changing the transform's behavior.

- **user-defined per-group states** related to stateful transformations of the Spark StructuredStreaming ETL (e.g. `flatMapGroupsWithState`)

  Consequently, a Strategy based also on this sub-folder (i.e. Strategy's transform containing stateless transformations of the Spark StructuredStreaming ETL) is obviously critical.


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
    still does not exist (i.e. does not contain files `.delta` / `.snapshot`, see [checkpoint-State](../documentation/spark-structured-streaming-checkpointing.md#state)) despite the "checkpointing process" of this Spark StructuredStreaming ETL expecting it.

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