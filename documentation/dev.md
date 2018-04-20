# Development

## Start scripts usage
(see `docker/start-whitelabel-wasp.sh`, `docker/start-wasp.sh`)

In `docker/start-wasp.sh`, check everything marked as:

    # !!! standalone applications have to ... !!! #

## Custom Node Launcher usage
(see `*/build.sbt`)

Add these in standalone applications:

`master/build.sbt`

    mainClass in Compile := Some("it.agilelab.bigdata.wasp.<APPLICATION_NAME>.master.launcher.MasterNodeLauncher")

`producers/build.sbt`

    mainClass in Compile := Some("it.agilelab.bigdata.wasp.<APPLICATION_NAME>.producers.launcher.ProducersNodeLauncher")

`consumers-rt/build.sbt`

    mainClass in Compile := Some("it.agilelab.bigdata.wasp.<APPLICATION_NAME>.consumers.rt.launcher.RtConsumersNodeLauncher")

`consumers-spark/build.sbt`

    mainClass in Compile := Some("thisClassNotExist")
    //mainClass in Compile := Some("it.agilelab.bigdata.wasp.<APPLICATION_NAME>.consumers.spark.launcher.SparkConsumersStreamingNodeLauncher")
    //mainClass in Compile := Some("it.agilelab.bigdata.wasp.<APPLICATION_NAME>.consumers.spark.launcher.SparkConsumersBatchNodeLauncher")

    // to use within "docker run" in start-wasp.sh using -main FULLY_QUALIFIED_NAME

## Spark distributed-mode (Spark Standalone or Hadoop YARN cluster managers) usage
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
    
- Add this in `consumers-spark/build.sbt`:    
    
        scriptClasspath += ":$HADOOP_CONF_DIR"
    
    - It is required to have in the directory `docker/docker-service-configuration/hadoop` all the configuration files `core-site.xml`, `hdfs-site.xml` and `hbase-site.xml`.

### Hadoop YARN usage
(see

- `consumers-spark/build.sbt`

- `docker/docker-environment.conf`
)

In standalone applications:
- Add this in `consumers-spark/build.sbt`:

        scriptClasspath += ":$HADOOP_CONF_DIR:$YARN_CONF_DIR"
        
    - It is required to copy in the directory `docker/external-cluster-configuration/hadoop` all the configurations of YARN and HBase.
        These can be retrieved in the Cloudera Manager under the section `Download Client Configuration` of the YARN and HBase services.
  
        N.B. Among the configurations, remove:
         - the property related to the topology configuration (`net.topology.script.file.name`) within the file `core-site.xml`
         - the files `topology.py` and `topology.map`
        
In `docker/docker-environment.conf`

- Override `master` configs

        spark-streaming.master.protocol = ""    # default: ""
        spark-streaming.master.host = "yarn"    # default: "local[*]"
        spark-streaming.master.port = 0         # default: 0
        
        spark-batch.master.protocol = ""        # default: ""
        spark-batch.master.host = "yarn"        # default: "local[*]"
        spark-batch.master.port = 0             # default: 0

- Override `driver-hostname` config

    E.g.
        
        spark-streaming.driver-conf.driver-hostname = "server08.cluster01.atscom.it"    # default: "consumers-spark-streaming"

        spark-batch.driver-conf.driver-hostname = "server08.cluster01.atscom.it"        # default: "consumers-spark-batch"
        
        # hostname used by executors to contact the driver


- Override `driver-port` and `block-manager-port` configs different between streaming and batch consumers

    E.g.

        spark-streaming.driver-conf.driver-port = 31441
        spark-streaming.block-manager-port = 31442
        
        spark-batch.driver-conf.driver-port = 31451
        spark-batch.block-manager-port = 31452

- Override with a valid jar-path the `yarn-jar` config

    E.g.
    
        spark-streaming.yarn-jar = "hdfs://server08.cluster01.atscom.it:8020/tmp/spark-jars-yarn-2.2.1/*"   # default: ""

        spark-batch.yarn-jar = "hdfs://server08.cluster01.atscom.it:8020/tmp/spark-jars-yarn-2.2.1/*"       # default: ""
        
    - It is required to put on HDFS the Spark libs that will be used to start the exeutors 
        - Download the tar https://www.apache.org/dyn/closer.lua/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz
        - Untar 
        - Copy the content of the directories `spark-2.2.1-bin-hadoop2.7/jars` and `spark-2.2.1-bin-hadoop2.7/yarn` on HDFS (e.g. hdfs://server08.cluster01.atscom.it:8020/tmp/spark-jars-yarn-2.2.1/*)

- Start all containers (WASP components) using the `docker/start-wasp.sh` specialization (e.g. `docker/start-whitelabel-wasp.sh`), passing `-y` or `--yarn` option to the script

#### Adding Security with Kerberos

**Note:** It is required that Spark is connected to a Kerberized YARN cluster following the istruction above

- Create a keytab in the directory `docker` (follow the instructions contained in the file `docker/keytab-build.sh`)

- Go to the file `docker/security-env.sh` and modify the environment variables with the correct values:
    - KEYTAB_FILE_NAME: name of the created keytab file (e.g. wasp2.keytab)
    - PRINCIPAL_NAME: principal including the realm, used to login (e.g. nome.cognome@CLUSTER01.ATSCOM.IT)
    - ETC_HOSTS: variable used to provide additional configurations to Docker. In this case should be added all the hosts and related IPs
      tp which Spark whould be connected. Ithi is required when the DNS has not the reverse or is not configured to provide informations related to Kerberos
      (e.g. "--add-host=server08.cluster01.atscom.it:192.168.69.223")
    - Check that the options in the autogenerated file `krb5.conf` are correct 

- Kafka security configs:
            
        others = [
          # mandatory
          { "security.protocol" = "SASL_PLAINTEXT" }
          { "sasl.kerberos.service.name" = "kafka" }
          { "sasl.jaas.config" = "com.sun.security.auth.module.Krb5LoginModule required storeKey=true useKeyTab=true useTicketCache=false keyTab=\"./wasp2.keytab\" serviceName=\"kafka\" principal=\"wasp2@REALM\";" }
          { "sasl.mechanism" = "GSSAPI" }
          { "kafka.security.protocol" = "SASL_PLAINTEXT" }
          { "kafka.sasl.kerberos.service.name" = "kafka" }
          { "kafka.sasl.jaas.config" = "com.sun.security.auth.module.Krb5LoginModule required storeKey=true useKeyTab=true useTicketCache=false keyTab=\"./wasp2.keytab\" serviceName=\"kafka\" principal=\"wasp2@REALM\";" }
          { "kafka.sasl.mechanism" = "GSSAPI" }
        
          # optional
          { "sasl.kerberos.kinit.cmd" = "/usr/bin/kinit" }
          { "sasl.kerberos.min.time.before.relogin" = "60000" }
          { "sasl.kerberos.ticket.renew.jitter" = "0.05" }
          { "sasl.kerberos.ticket.renew.window.factor" = "0.8" }
          { "kafka.sasl.kerberos.kinit.cmd" = "/usr/bin/kinit" }
          { "kafka.sasl.kerberos.min.time.before.relogin" = "60000" }
          { "kafka.sasl.kerberos.ticket.renew.jitter" = "0.05" }
          { "kafka.sasl.kerberos.ticket.renew.window.factor" = "0.8" }
        ]

- Spark security configs:

    - `spark streaming.checkpoint-dir` has to be accessible by the user
    
    - Override `spark-streaming` and `spark-batch` configs
    
            others = [
              { "spark.yarn.dist.files" = "file:///root/configurations/wasp2.keytab,file:///root/configurations/sasl.jaas.config" }     # Per impostare la sicurezza bisogna mettere nella directory del executor il keytab dell'utente e il jaas.config per i client zookeeper e kafka 
              { "spark.executor.extraJavaOptions" = "-Djava.security.auth.login.config=./sasl.jaas.config" }                            # Si imposta il percorso del jaas file in modo che un client che supporta sasl sappia dove andarlo a prendere 
              { "spark.authenticate" = "true" }                                                                                         # Dice a spark di abilitare l'autenticazione ai servizi Yarn, HDFS, HBase
            ]
            
        **Note:** use `file://` before the path otherwise the files will be not transferred
        
    - **Important:** Not change the keytab's name otherwise the MD5 is changed and the authentication will not work anymore

- Start all containers (WASP components) using the `docker/start-wasp.sh` specialization (e.g. `docker/start-whitelabel-wasp.sh`), passing `-s` or `--security` option to the script (which also automatically includes the `yarn mode`)

##### Possible problem
The following exception happens when the reverse dns of the cluster addreses on the machine/docker where WASP is executed does not work:

    java.io.IOException: Failed on local exception: java.io.IOException: java.lang.IllegalArgumentException: Server has invalid Kerberos principal: yarn/principal@REALM; Host Details : local host is: ""; destination host is: "SERVER":8032;
      at org.apache.hadoop.net.NetUtils.wrapException(NetUtils.java:772)
      at org.apache.hadoop.ipc.Client.call(Client.java:1474)
      at org.apache.hadoop.ipc.Client.call(Client.java:1401)
      at org.apache.hadoop.ipc.ProtobufRpcEngine$Invoker.invoke(ProtobufRpcEngine.java:232)
      at com.sun.proxy.$Proxy16.getNewApplication(Unknown Source)
      at org.apache.hadoop.yarn.api.impl.pb.client.ApplicationClientProtocolPBClientImpl.getNewApplication(ApplicationClientProtocolPBClientImpl.java:217)
      at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
      at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
      at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
      at java.lang.reflect.Method.invoke(Method.java:498)
      at org.apache.hadoop.io.retry.RetryInvocationHandler.invokeMethod(RetryInvocationHandler.java:187)
      at org.apache.hadoop.io.retry.RetryInvocationHandler.invoke(RetryInvocationHandler.java:102)
      at com.sun.proxy.$Proxy17.getNewApplication(Unknown Source)
      at org.apache.hadoop.yarn.client.api.impl.YarnClientImpl.getNewApplication(YarnClientImpl.java:206)
      at org.apache.hadoop.yarn.client.api.impl.YarnClientImpl.createApplication(YarnClientImpl.java:214)
      at org.apache.spark.deploy.yarn.Client.submitApplication(Client.scala:159)
      at org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend.start(YarnClientSchedulerBackend.scala:56)
      at org.apache.spark.scheduler.TaskSchedulerImpl.start(TaskSchedulerImpl.scala:173)
      at org.apache.spark.SparkContext.<init>(SparkContext.scala:509)
      at org.apache.spark.SparkContext$.getOrCreate(SparkContext.scala:2516)
      at org.apache.spark.sql.SparkSession$Builder$$anonfun$6.apply(SparkSession.scala:918)
      at org.apache.spark.sql.SparkSession$Builder$$anonfun$6.apply(SparkSession.scala:910)
      at scala.Option.getOrElse(Option.scala:121)
      at org.apache.spark.sql.SparkSession$Builder.getOrCreate(SparkSession.scala:910)
      at org.apache.spark.repl.Main$.createSparkSession(Main.scala:101)
      ... 47 elided
    Caused by: java.io.IOException: java.lang.IllegalArgumentException: Server has invalid Kerberos principal: yarn/principal@REALM
      at org.apache.hadoop.ipc.Client$Connection$1.run(Client.java:682)
      at java.security.AccessController.doPrivileged(Native Method)
      at javax.security.auth.Subject.doAs(Subject.java:422)
      at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1692)
      at org.apache.hadoop.ipc.Client$Connection.handleSaslConnectionFailure(Client.java:645)
      at org.apache.hadoop.ipc.Client$Connection.setupIOstreams(Client.java:732)
      at org.apache.hadoop.ipc.Client$Connection.access$2800(Client.java:370)
      at org.apache.hadoop.ipc.Client.getConnection(Client.java:1523)
      at org.apache.hadoop.ipc.Client.call(Client.java:1440)
      ... 70 more
    Caused by: java.lang.IllegalArgumentException: Server has invalid Kerberos principal: yarn/principal@REALM
      at org.apache.hadoop.security.SaslRpcClient.getServerPrincipal(SaslRpcClient.java:334)
      at org.apache.hadoop.security.SaslRpcClient.createSaslClient(SaslRpcClient.java:231)
      at org.apache.hadoop.security.SaslRpcClient.selectSaslClient(SaslRpcClient.java:159)
      at org.apache.hadoop.security.SaslRpcClient.saslConnect(SaslRpcClient.java:396)
      at org.apache.hadoop.ipc.Client$Connection.setupSaslConnection(Client.java:555)
      at org.apache.hadoop.ipc.Client$Connection.access$1800(Client.java:370)
      at org.apache.hadoop.ipc.Client$Connection$2.run(Client.java:724)
      at org.apache.hadoop.ipc.Client$Connection$2.run(Client.java:720)
      at java.security.AccessController.doPrivileged(Native Method)
      at javax.security.auth.Subject.doAs(Subject.java:422)
      at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1692)
      at org.apache.hadoop.ipc.Client$Connection.setupIOstreams(Client.java:719)
      ... 73 more

##### Possibile solutions
- Fix the reverse dns 
- Put the missed hosts in `/etc/hosts`
- https://docs-snaplogic.atlassian.net/wiki/spaces/SD/pages/2015960/How+to+Configure+a+Groundplex+for+CDH+with+Kerberos+Authentication

## Configuration validation rules
(see `/docker/docker-environment.conf`)

In order to be started, WASP defines a set of configuration validation rules to be validated.

These validation rules are defined at global-level (by `globalValidationRules` in `ConfigManager.scala`) and at plugin-level (by `WaspConsumersSparkPlugin.getValidationRules()` exposed by each plugin).

The following configurations allow to alter the default behavior of WASP related to validation rules:
- `environment.validationRulesToIgnore`(default: `[]`): list of validation rules to ignore (through validation rule's keys)
- `environment.mode` (default: `"production"`) - for all not ignored validation rules: print VALIDATION-RESULT (validation rule's keys and PASSED/NOT PASSED); if there is at least a validation failure (NOT PASSED):
    - `environment.mode` == `"develop"`: print VALIDATION-WARN and continue
    - `environment.mode` != `"develop"` (all not "develop" is considered "production" by default): print VALIDATION-ERROR and exit

## Procedure of Strategy version upgrade

(see

- `TestCheckpointConsoleWriterStructuredJSONPipegraph` / `TestCheckpointConsoleWriterStructuredAVROPipegraph` in `models/test/TestPipegraphs.scala`

- `consumers/spark/strategies/test/TestCheckpointStrategy.scala`

- `models/test/TestCheckpointState.scala`
)

Inside the *checkpointDir* (default: `/checkpoint` on HDFS) are automatically stored:
 
- **offsets** / **commits** related to the Kafka queue positions already managed by the Spark StructuredStreaming ETL (e.g. `flatMapGroups`)
   
  Consequently, a Strategy based only on these sub-directories (i.e. Strategy's transform containing only stateless transformations of the Spark StructuredStreaming ETL) should not be really critical, also changing the transform's behavior.

- **user-defined per-group states** related to stateful transformations of the Spark StructuredStreaming ETL (e.g. `flatMapGroupsWithState`)

  Consequently, a Strategy also based on this sub-directory (i.e. Strategy's transform containing stateless transformations of the Spark StructuredStreaming ETL) is obviously critical.


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
        
- **Note:** In Strategy's transform (`TestCheckpointStrategy`), adding a further stateful transformation (es. `flatMapGroupsWithState`) based on the result of the already existing one, requires to clean-up the *checkpointDir* directories `offsets`, `commits` and `state`.
            
    The reason is that the sub-directory `state/0` exists, related to the already present stateful transformation, whereas the fresh new created sub-directory `state/1`, related to the new stateful transformation,
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
         
        // throws "java.lang.IllegalStateException: Error reading delta file - Caused by: java.io.FileNotFoundException: File does not exist" until checkpointDir sub-directories are cleaned-up