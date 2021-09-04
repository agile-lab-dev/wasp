lazy val flavor = {
  val f = Flavor.currentFlavor()
  println(Utils.printWithBorders(s"Building for flavor: ${f}", "*"))
  f
}
lazy val dependencies = flavor.dependencies
lazy val settings     = flavor.settings

//integration tests should extend Test configuration and not Runtime configuration
lazy val IntegrationTest = config("it") extend (Test)

/*
 * Main build definition.
 *
 * See project/Settings.scala for the settings definitions.
 * See project/Dependencies.scala for the dependencies definitions.
 */

/* Libraries */

val generateOpenApi: TaskKey[Unit] = TaskKey("generate-open-api", "Updates the generated open api specification")

/* Framework */

lazy val scala_compiler = Project("wasp-compiler", file("compiler"))
  .settings(settings.commonSettings: _*)
  .dependsOn(model)
  .settings(libraryDependencies ++= dependencies.scalaCompilerDependencies)

lazy val model = Project("wasp-model", file("model"))
  .settings(settings.commonSettings: _*)
  .settings(libraryDependencies ++= dependencies.modelDependencies)

lazy val core = Project("wasp-core", file("core"))
  .settings(settings.commonSettings: _*)
  .dependsOn(scala_compiler)
  .dependsOn(model)
  .dependsOn(repository_core)
  .dependsOn(nifi_client)
  .settings(settings.sbtBuildInfoSettings: _*)
  .settings(libraryDependencies ++= dependencies.coreDependencies)
  .enablePlugins(BuildInfoPlugin)

lazy val repository_core = Project("wasp-repository-core", file("repository/core"))
  .settings(settings.commonSettings: _*)
  .dependsOn(model)
  .settings(libraryDependencies ++= dependencies.repositoryCoreDependencies)

lazy val repository_mongo = Project("wasp-repository-mongo", file("repository/mongo"))
  .settings(settings.commonSettings: _*)
  .dependsOn(repository_core)
  .dependsOn(core)
  .settings(libraryDependencies ++= dependencies.repositoryMongoDependencies)

lazy val repository_postgres = Project("wasp-repository-postgres", file("repository/postgres"))
  .settings(settings.commonSettings: _*)
  .dependsOn(repository_core)
  .dependsOn(core)
  .settings(libraryDependencies ++= dependencies.repositoryPostgresDependencies)

lazy val repository = Project("wasp-repository", file("repository"))
  .settings(settings.commonSettings: _*)
  .aggregate(repository_core, repository_mongo, repository_postgres)

lazy val master = Project("wasp-master", file("master"))
  .settings(settings.commonSettings: _*)
  .dependsOn(core)
  .dependsOn(nifi_client)
  .settings(libraryDependencies ++= dependencies.masterDependencies)

lazy val producers = Project("wasp-producers", file("producers"))
  .settings(settings.commonSettings: _*)
  .dependsOn(core)
  .settings(libraryDependencies ++= dependencies.producersDependencies)

lazy val consumers_spark = Project("wasp-consumers-spark", file("consumers-spark"))
  .settings(settings.commonSettings: _*)
  .dependsOn(core)
  .settings(libraryDependencies ++= dependencies.consumersSparkDependencies)
  .settings(settings.disableParallelTests: _*)

lazy val consumers_rt = Project("wasp-consumers-rt", file("consumers-rt"))
  .settings(settings.commonSettings: _*)
  .dependsOn(core)
  .settings(libraryDependencies ++= dependencies.consumersRtDependencies)

/* Plugins */

lazy val plugin_console_spark = Project("wasp-plugin-console-spark", file("plugin-console-spark"))
  .settings(settings.commonSettings: _*)
  .settings(libraryDependencies ++= dependencies.sparkPluginBasicDependencies)
  .dependsOn(consumers_spark)

lazy val plugin_elastic_spark = Project("wasp-plugin-elastic-spark", file("plugin-elastic-spark"))
  .settings(settings.commonSettings: _*)
  .dependsOn(consumers_spark)
  .settings(libraryDependencies ++= dependencies.pluginElasticSparkDependencies)

lazy val plugin_hbase_spark = Project("wasp-plugin-hbase-spark", file("plugin-hbase-spark"))
  .settings(settings.commonSettings: _*)
  .dependsOn(consumers_spark)
  .settings(libraryDependencies ++= dependencies.pluginHbaseSparkDependencies)

lazy val plugin_jdbc_spark = Project("wasp-plugin-jdbc-spark", file("plugin-jdbc-spark"))
  .settings(settings.commonSettings: _*)
  .settings(libraryDependencies ++= dependencies.sparkPluginBasicDependencies)
  .dependsOn(consumers_spark)

lazy val plugin_kafka_spark = Project("wasp-plugin-kafka-spark", file("plugin-kafka-spark"))
  .settings(settings.commonSettings: _*)
  .dependsOn(consumers_spark % "compile->compile;test->test")
  .settings(libraryDependencies ++= dependencies.pluginKafkaSparkDependencies)

lazy val plugin_kafka_spark_new = Project("wasp-plugin-kafka-spark-new", file("plugin-kafka-spark-new"))
  .settings(settings.commonSettings: _*)
  .dependsOn(consumers_spark % "compile->compile;test->test")
  .settings(libraryDependencies ++= dependencies.pluginKafkaSparkNewDependencies)

lazy val plugin_kafka_spark_old = Project("wasp-plugin-kafka-spark-old", file("plugin-kafka-spark-old"))
  .settings(settings.commonSettings: _*)
  .dependsOn(consumers_spark % "compile->compile;test->test")
  .settings(libraryDependencies ++= dependencies.pluginKafkaSparkOldDependencies)

lazy val plugin_raw_spark = Project("wasp-plugin-raw-spark", file("plugin-raw-spark"))
  .settings(settings.commonSettings: _*)
  .settings(libraryDependencies ++= dependencies.sparkPluginBasicDependencies)
  .dependsOn(consumers_spark % "compile->compile;test->test")

lazy val plugin_solr_spark = Project("wasp-plugin-solr-spark", file("plugin-solr-spark"))
  .settings(settings.commonSettings: _*)
  .dependsOn(consumers_spark)
  .settings(libraryDependencies ++= dependencies.pluginSolrSparkDependencies)

lazy val plugin_mongo_spark = Project("wasp-plugin-mongo-spark", file("plugin-mongo-spark"))
  .settings(settings.commonSettings: _*)
  .dependsOn(consumers_spark)
  .settings(libraryDependencies ++= dependencies.pluginMongoSparkDependencies)

lazy val plugin_mailer_spark = Project("wasp-plugin-mailer-spark", file("plugin-mailer-spark"))
  .settings(settings.commonSettings: _*)
  .dependsOn(consumers_spark)
  .settings(libraryDependencies ++= dependencies.pluginMailerSparkDependencies)

lazy val plugin_http_spark = Project("wasp-plugin-http-spark", file("plugin-http-spark"))
  .settings(settings.commonSettings: _*)
  .dependsOn(consumers_spark % "compile->compile;test->test")
  .settings(libraryDependencies ++= dependencies.pluginHttpSparkDependencies)

lazy val plugin_cdc_spark = Project("wasp-plugin-cdc-spark", file("plugin-cdc-spark"))
  .settings(settings.commonSettings: _*)
  .dependsOn(consumers_spark % "compile->compile;test->test")
  .settings(libraryDependencies ++= dependencies.pluginCdcSparkDependencies)

lazy val microservice_catalog = Project("wasp-microservice-catalog", file("microservice-catalog"))
  .settings(settings.commonSettings: _*)
  .settings(libraryDependencies ++= dependencies.microserviceCatalogDependencies)
  .dependsOn(consumers_spark % "compile->compile;test->test")

lazy val plugin_parallel_write_spark = Project("wasp-plugin-parallel-write-spark", file("plugin-parallel-write-spark"))
  .settings(Defaults.itSettings)
  .settings(settings.commonSettings: _*)
  .settings(settings.disableParallelTests)
  .settings(libraryDependencies ++= dependencies.pluginParallelWriteSparkDependencies)
  .dependsOn(microservice_catalog % "compile->compile;test->test")

/* Yarn  */

lazy val yarn_auth_hdfs = Project("wasp-yarn-auth-hdfs", file("yarn/auth/hdfs"))
  .settings(settings.commonSettings: _*)
  .settings(dependencies.kmsTest: _*)
  .settings(libraryDependencies ++= dependencies.yarnAuthHdfsDependencies)

lazy val yarn_auth_hbase = Project("wasp-yarn-auth-hbase", file("yarn/auth/hbase"))
  .settings(settings.commonSettings: _*)
  .settings(libraryDependencies ++= dependencies.yarnAuthHBaseDependencies)

lazy val yarn_auth = Project("wasp-yarn-auth", file("yarn/auth"))
  .settings(settings.commonSettings: _*)
  .aggregate(yarn_auth_hbase, yarn_auth_hdfs)

lazy val yarn = Project("wasp-yarn", file("yarn"))
  .settings(settings.commonSettings: _*)
  .aggregate(yarn_auth)

lazy val spark_telemetry_plugin = Project("wasp-spark-telemetry-plugin", file("spark/telemetry-plugin"))
  .settings(settings.commonSettings: _*)
  .settings(libraryDependencies ++= dependencies.sparkTelemetryPluginDependencies)

lazy val spark_nifi_plugin = Project("wasp-spark-nifi-plugin", file("spark/nifi-plugin"))
  .settings(settings.commonSettings: _*)
  .settings(libraryDependencies ++= dependencies.sparkNifiPluginDependencies)
  .dependsOn(consumers_spark)
  .dependsOn(spark_nifi_plugin_bridge)

lazy val spark_nifi_plugin_bridge = Project("wasp-spark-nifi-plugin-bridge", file("spark/nifi-plugin-bridge"))
  .settings(settings.commonSettings: _*)
  .settings(libraryDependencies ++= dependencies.nifiStatelessDependencies)

lazy val spark = Project("wasp-spark", file("spark"))
  .settings(settings.commonSettings: _*)
  .aggregate(spark_telemetry_plugin, spark_nifi_plugin)

/* nifi */

lazy val nifi_client = Project("wasp-nifi-client", file("nifi-client"))
  .settings(settings.commonSettings: _*)
  .settings(libraryDependencies ++= dependencies.nifiClientDependencies)

lazy val kernel = project
  .withId("wasp-kernel")
  .settings(settings.commonSettings: _*)
  .aggregate(
    model,
    scala_compiler,
    repository_core,
    core,
    master,
    producers,
    consumers_spark,
    consumers_rt,
    openapi,
    nifi_client,
    yarn,
    spark
  )

lazy val plugin = project
  .withId("wasp-plugin")
  .settings(settings.commonSettings: _*)
  .aggregate(
    plugin_console_spark,
    plugin_hbase_spark,
    plugin_jdbc_spark,
    plugin_kafka_spark,
    plugin_kafka_spark_old,
    plugin_kafka_spark_new,
    plugin_raw_spark,
    plugin_solr_spark,
    plugin_cdc_spark,
    plugin_parallel_write_spark,
    plugin_mailer_spark,
    plugin_http_spark,
    plugin_mongo_spark,
    microservice_catalog
  )

/* Framework + Plugins */
lazy val wasp = Project("wasp", file("."))
  .settings(settings.commonSettings: _*)
  .aggregate(
    kernel,
    repository,
    plugin
  )

/* WhiteLabel */

lazy val whiteLabelModels = Project("wasp-whitelabel-models", file("whitelabel/models"))
  .settings(settings.commonSettings: _*)
  .dependsOn(core)
  .settings(libraryDependencies ++= dependencies.whitelabelModelsDependencies)

lazy val whiteLabelMaster = Project("wasp-whitelabel-master", file("whitelabel/master"))
  .settings(settings.commonSettings: _*)
  .dependsOn(whiteLabelModels)
  .dependsOn(repository_mongo)
  .dependsOn(whiteLabelConsumersSpark)
  .dependsOn(master)
  .settings(libraryDependencies ++= dependencies.whitelabelMasterDependencies)
  .enablePlugins(JavaAppPackaging)
  .settings(dependencies.whitelabelMasterScriptClasspath)

lazy val whiteLabelProducers = Project("wasp-whitelabel-producers", file("whitelabel/producers"))
  .settings(settings.commonSettings: _*)
  .dependsOn(whiteLabelModels)
  .dependsOn(repository_mongo)
  .dependsOn(producers)
  .settings(libraryDependencies ++= dependencies.whitelabelProducerDependencies)
  .settings(dependencies.whitelabelProducerScriptClasspath)
  .enablePlugins(JavaAppPackaging)

lazy val whiteLabelConsumersSpark = Project("wasp-whitelabel-consumers-spark", file("whitelabel/consumers-spark"))
  .settings(settings.commonSettings: _*)
  .dependsOn(whiteLabelModels)
  .dependsOn(consumers_spark)
  .dependsOn(repository_mongo)
  .dependsOn(plugin_console_spark)
  .dependsOn(plugin_hbase_spark)
  .dependsOn(plugin_jdbc_spark)
  .dependsOn(plugin_kafka_spark_new)
  .dependsOn(plugin_mailer_spark)
  .dependsOn(plugin_raw_spark)
  .dependsOn(plugin_solr_spark)
  .dependsOn(plugin_mongo_spark)
  .dependsOn(plugin_http_spark)
  .dependsOn(plugin_cdc_spark)
  .dependsOn(spark_telemetry_plugin)
  .dependsOn(spark_nifi_plugin)
  .dependsOn(plugin_parallel_write_spark)
  .settings(libraryDependencies ++= dependencies.whitelabelSparkConsumerDependencies)
  .enablePlugins(JavaAppPackaging)
  .settings(dependencies.whitelabelSparkConsumerScriptClasspath)

lazy val whiteLabelConsumersRt = Project("wasp-whitelabel-consumers-rt", file("whitelabel/consumers-rt"))
  .settings(settings.commonSettings: _*)
  .dependsOn(whiteLabelModels)
  .dependsOn(consumers_rt)
  .dependsOn(plugin_hbase_spark)
  .settings(libraryDependencies ++= dependencies.whiteLabelConsumersRtDependencies)
  .enablePlugins(JavaAppPackaging)
  .settings(dependencies.whiteLabelConsumersRtScriptClasspath)

lazy val whiteLabelSingleNode = project
  .withId("wasp-whitelabel-singlenode")
  .in(file("whitelabel/single-node"))
  .settings(settings.commonSettings: _*)
  .dependsOn(whiteLabelMaster)
  .dependsOn(whiteLabelConsumersSpark)
  .dependsOn(whiteLabelProducers)
  .enablePlugins(JavaAppPackaging)
  .settings(dependencies.whiteLabelSingleNodeScriptClasspath)

lazy val whiteLabel = Project("wasp-whitelabel", file("whitelabel"))
  .settings(settings.commonSettings: _*)
  .aggregate(
    whiteLabelModels,
    whiteLabelMaster,
    whiteLabelProducers,
    whiteLabelConsumersSpark,
    whiteLabelConsumersRt,
    whiteLabelSingleNode
  )

lazy val openapi = Project("wasp-openapi", file("openapi"))
  .settings(settings.commonSettings: _*)
  .settings(libraryDependencies ++= dependencies.openapiDependencies)
  .settings(
    generateOpenApi := {
      (Compile / runMain)
        .toTask(" it.agilelab.bigdata.wasp.master.web.openapi.GenerateOpenApi documentation/wasp-openapi.yaml")
        .value
    },
    generateOpenApi := (generateOpenApi dependsOn (Compile / compile)).value
  )
  .dependsOn(core)
