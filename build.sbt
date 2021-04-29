import Dependencies.scalaTest

//integration tests should extend Test configuration and not Runtime configuration
lazy val IntegrationTest = config("it") extend (Test)

/*
 * Main build definition.
 *
 * See project/Settings.scala for the settings definitions.
 * See project/Dependencies.scala for the dependencies definitions.
 * See project/Versions.scala for the versions definitions.
 */

/* Libraries */

val generateOpenApi: TaskKey[Unit] = TaskKey("generate-open-api", "Updates the generated open api specification")

lazy val spark_sql_kafka_0_11 = Project("wasp-spark-sql-kafka-0-11", file("spark-sql-kafka-0-11"))
  .configs(IntegrationTest)
  .settings(Settings.commonSettings: _*)
  .settings(Defaults.itSettings)
  .settings(Settings.disableParallelTests: _*)
  .settings(libraryDependencies ++= Dependencies.spark_sql_kafka_0_11)

lazy val spark_sql_kafka_0_11_new = Project("wasp-spark-sql-kafka-0-11-new", file("spark-sql-kafka-0-11-new"))
  .configs(IntegrationTest)
  .settings(Settings.commonSettings: _*)
  .settings(Defaults.itSettings)
  .settings(Settings.disableParallelTests: _*)
  .settings(libraryDependencies ++= Dependencies.spark_sql_kafka_0_11)

lazy val spark_sql_kafka_0_11_old = Project("wasp-spark-sql-kafka-0-11-old", file("spark-sql-kafka-0-11-old"))
  .configs(IntegrationTest)
  .settings(Settings.commonSettings: _*)
  .settings(Defaults.itSettings)
  .settings(Settings.disableParallelTests: _*)
  .settings(libraryDependencies ++= Dependencies.spark_sql_kafka_0_11)

/* Framework */

lazy val scala_compiler = Project("wasp-compiler", file("compiler"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(model)
  .settings(libraryDependencies ++= Dependencies.test :+ Dependencies.scalaCompiler :+ Dependencies.scalaPool)

lazy val model = Project("wasp-model", file("model"))
  .settings(Settings.commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.model ++ Dependencies.test :+ Dependencies.darwinCore)

lazy val core = Project("wasp-core", file("core"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(scala_compiler)
  .dependsOn(model)
  .dependsOn(repository_core)
  .dependsOn(nifi_client)
  .settings(Settings.sbtBuildInfoSettings: _*)
  .settings(libraryDependencies ++= Dependencies.core ++ Dependencies.test :+ Dependencies.darwinCore)
  .enablePlugins(BuildInfoPlugin)

lazy val repository_core = Project("wasp-repository-core", file("repository/core"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(model)
  .settings(libraryDependencies ++= Dependencies.test)

lazy val repository_mongo = Project("wasp-repository-mongo", file("repository/mongo"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(repository_core)
  .dependsOn(core)
  .settings(libraryDependencies ++= Dependencies.repository_mongo ++ Dependencies.test)

lazy val repository_postgres = Project("wasp-repository-postgres", file("repository/postgres"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(repository_core)
  .dependsOn(core)
  .settings(libraryDependencies ++= Dependencies.repository_postgres ++ Dependencies.test)

lazy val repository = Project("wasp-repository", file("repository"))
  .settings(Settings.commonSettings: _*)
  .aggregate(repository_core, repository_mongo, repository_postgres)

lazy val master = Project("wasp-master", file("master"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(core)
  .dependsOn(nifi_client)
  .settings(libraryDependencies ++= Dependencies.master)
  .enablePlugins(JavaAppPackaging)

lazy val producers = Project("wasp-producers", file("producers"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(core)
  .settings(libraryDependencies ++= Dependencies.producers)
  .enablePlugins(JavaAppPackaging)

lazy val consumers_spark = Project("wasp-consumers-spark", file("consumers-spark"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(core)
  .settings(libraryDependencies ++= Dependencies.consumers_spark ++ Dependencies.wireMock)
  .settings(Settings.disableParallelTests: _*)
  .enablePlugins(JavaAppPackaging)

lazy val consumers_rt = Project("wasp-consumers-rt", file("consumers-rt"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(core)
  .settings(libraryDependencies ++= Dependencies.consumers_rt)
  .enablePlugins(JavaAppPackaging)

/* Plugins */

lazy val plugin_console_spark = Project("wasp-plugin-console-spark", file("plugin-console-spark"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(consumers_spark)

lazy val plugin_elastic_spark = Project("wasp-plugin-elastic-spark", file("plugin-elastic-spark"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(consumers_spark)
  .settings(libraryDependencies ++= Dependencies.plugin_elastic_spark)

lazy val plugin_hbase_spark = Project("wasp-plugin-hbase-spark", file("plugin-hbase-spark"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(consumers_spark)
  .settings(libraryDependencies ++= Dependencies.plugin_hbase_spark)

lazy val plugin_jdbc_spark = Project("wasp-plugin-jdbc-spark", file("plugin-jdbc-spark"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(consumers_spark)

lazy val plugin_kafka_spark = Project("wasp-plugin-kafka-spark", file("plugin-kafka-spark"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(consumers_spark % "compile->compile;test->test")
  .dependsOn(spark_sql_kafka_0_11)
  .settings(libraryDependencies ++= Dependencies.plugin_kafka_spark)

lazy val plugin_kafka_spark_new = Project("wasp-plugin-kafka-spark-new", file("plugin-kafka-spark-new"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(consumers_spark % "compile->compile;test->test")
  .dependsOn(spark_sql_kafka_0_11_new)
  .settings(libraryDependencies ++= Dependencies.plugin_kafka_spark)

lazy val plugin_kafka_spark_old = Project("wasp-plugin-kafka-spark-old", file("plugin-kafka-spark-old"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(consumers_spark % "compile->compile;test->test")
  .dependsOn(spark_sql_kafka_0_11_old)
  .settings(libraryDependencies ++= Dependencies.plugin_kafka_spark)

lazy val plugin_raw_spark = Project("wasp-plugin-raw-spark", file("plugin-raw-spark"))
  .settings(Settings.commonSettings: _*)
  .settings(libraryDependencies += Dependencies.scalaTest)
  .dependsOn(consumers_spark % "compile->compile;test->test")

lazy val plugin_solr_spark = Project("wasp-plugin-solr-spark", file("plugin-solr-spark"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(consumers_spark)
  .settings(libraryDependencies ++= Dependencies.plugin_solr_spark)

lazy val plugin_mongo_spark = Project("wasp-plugin-mongo-spark", file("plugin-mongo-spark"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(consumers_spark)
  .settings(libraryDependencies ++= Dependencies.plugin_mongo_spark)

lazy val plugin_mailer_spark = Project("wasp-plugin-mailer-spark", file("plugin-mailer-spark"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(consumers_spark)
  .settings(libraryDependencies ++= Dependencies.plugin_mailer_spark)

lazy val plugin_http_spark = Project("wasp-plugin-http-spark", file("plugin-http-spark"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(consumers_spark % "compile->compile;test->test")
  .settings(libraryDependencies ++= Dependencies.plugin_http_spark)

lazy val plugin_cdc_spark = Project("wasp-plugin-cdc-spark", file("plugin-cdc-spark"))
	.settings(Settings.commonSettings: _*)
  .dependsOn(consumers_spark % "compile->compile;test->test")
  .dependsOn(delta_lake)
	.settings(libraryDependencies ++= Dependencies.plugin_cdc_spark)
  .enablePlugins(JavaAppPackaging)

lazy val microservice_catalog = Project("wasp-microservice-catalog", file("microservice-catalog"))
  .settings(Settings.commonSettings: _*)
  .settings(libraryDependencies += Dependencies.scalaTest)
  .settings(libraryDependencies += "net.liftweb" %% "lift-json" % "3.4.1")
  .settings(libraryDependencies ++= Dependencies.plugin_http_spark)
  .dependsOn(consumers_spark % "compile->compile;test->test")
  // https://mvnrepository.com/artifact/net.liftweb/lift-json


lazy val plugin_parallel_write_spark = Project("wasp-plugin-parallel-write-spark", file("plugin-parallel-write-spark"))
  .settings(Settings.commonSettings: _*)
  .settings(libraryDependencies += Dependencies.scalaTest)
// https://mvnrepository.com/artifact/net.liftweb/lift-json
  .settings(libraryDependencies += "net.liftweb" %% "lift-json" % "3.4.1")
  .settings(libraryDependencies ++= Dependencies.plugin_http_spark)
  .settings(libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.0.0")
  .dependsOn(microservice_catalog % "compile->compile;test->test")
lazy val plugin_continuous_update_spark = Project("wasp-plugin-continuous-update-spark", file("plugin-continuous-update-spark"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(Settings.commonSettings: _*)
  .settings(libraryDependencies += Dependencies.scalaTest)
  // https://mvnrepository.com/artifact/net.liftweb/lift-json
  .settings(libraryDependencies += "net.liftweb" %% "lift-json" % "3.4.1")
  .settings(libraryDependencies ++= Dependencies.plugin_http_spark)
  .settings(libraryDependencies += "cloud.localstack" % "localstack-utils" % "0.2.10" % Test)
  .settings(libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.0.0")
  .dependsOn(microservice_catalog % "compile->compile;test->test")
  .dependsOn(delta_lake)
/* Yarn  */

lazy val yarn_auth_hdfs = Project("wasp-yarn-auth-hdfs", file("yarn/auth/hdfs"))
  .settings(Settings.commonSettings: _*)
  .settings(Dependencies.kmsTest: _*)
  .settings(libraryDependencies += scalaTest)
  .settings(libraryDependencies += Dependencies.sparkYarn)

lazy val yarn_auth_hbase = Project("wasp-yarn-auth-hbase", file("yarn/auth/hbase"))
  .settings(Settings.commonSettings: _*)
  .settings(libraryDependencies += Dependencies.sparkYarn)
  .settings(libraryDependencies += Dependencies.hbaseServer)
  .settings(libraryDependencies += Dependencies.hbaseCommon)

lazy val yarn_auth = Project("wasp-yarn-auth", file("yarn/auth"))
  .settings(Settings.commonSettings: _*)
  .aggregate(yarn_auth_hbase, yarn_auth_hdfs)

lazy val yarn = Project("wasp-yarn", file("yarn"))
  .settings(Settings.commonSettings: _*)
  .aggregate(yarn_auth)

lazy val spark_telemetry_plugin = Project("wasp-spark-telemetry-plugin", file("spark/telemetry-plugin"))
  .settings(Settings.commonSettings: _*)
  .settings(libraryDependencies += Dependencies.sparkCore)
  .settings(libraryDependencies += Dependencies.kafkaClients)
  .settings(libraryDependencies += Dependencies.scalaParserAndCombinators)

lazy val spark_nifi_plugin = Project("wasp-spark-nifi-plugin", file("spark/nifi-plugin"))
  .settings(Settings.commonSettings: _*)
  .settings(libraryDependencies += Dependencies.sparkCore)
  .dependsOn(consumers_spark)
  .dependsOn(spark_nifi_plugin_bridge)

lazy val spark_nifi_plugin_bridge = Project("wasp-spark-nifi-plugin-bridge", file("spark/nifi-plugin-bridge"))
  .settings(Settings.commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.nifiStateless)

lazy val spark = Project("wasp-spark", file("spark"))
  .settings(Settings.commonSettings: _*)
  .aggregate(spark_telemetry_plugin, spark_nifi_plugin)

/* nifi */

lazy val nifi_client = Project("wasp-nifi-client", file("nifi-client"))
  .settings(Settings.commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.nifiClient)

/* delta lake */
lazy val delta_lake = Project("wasp-delta-lake", file("delta-lake"))
  .settings(Settings.commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.delta_lake)
  .settings(resolvers ++= Resolvers.resolvers)
  .settings(scalaVersion := Versions.scala)

/* Framework + Plugins */
lazy val wasp = Project("wasp", file("."))
  .settings(Settings.commonSettings: _*)
  .aggregate(
    model,
    scala_compiler,
    repository_core,
    core,
    repository_mongo,
    repository_postgres,
    master,
    producers,
    consumers_spark,
    consumers_rt,
    plugin_console_spark,
    plugin_hbase_spark,
    plugin_jdbc_spark,
    plugin_kafka_spark,
    plugin_kafka_spark_old,
    plugin_kafka_spark_new,
    plugin_raw_spark,
    plugin_solr_spark,
    plugin_cdc_spark,
    microservice_catalog,
    plugin_continuous_update_spark,
    plugin_parallel_write_spark,
    yarn,
    spark,
    plugin_mailer_spark,
    plugin_http_spark,
    spark_sql_kafka_0_11,
    spark_sql_kafka_0_11_old,
    spark_sql_kafka_0_11_new,
    openapi,
    nifi_client,
    plugin_mongo_spark,
    delta_lake,
  )

/* WhiteLabel */

lazy val whiteLabelModels = Project("wasp-whitelabel-models", file("whitelabel/models"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(core)
  .settings(libraryDependencies ++= Dependencies.log4j ++ Dependencies.avro4s)

lazy val whiteLabelMaster = Project("wasp-whitelabel-master", file("whitelabel/master"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(whiteLabelModels)
  .dependsOn(repository_mongo)
  .dependsOn(whiteLabelConsumersSpark)
  .dependsOn(master)
  .settings(libraryDependencies ++= Dependencies.plugin_hbase_spark)
  .settings(libraryDependencies += Dependencies.log4j1)
  .settings(libraryDependencies ++= Dependencies.log4j :+ Dependencies.darwinConfluentConnector)
  .enablePlugins(JavaAppPackaging)

lazy val whiteLabelProducers = Project("wasp-whitelabel-producers", file("whitelabel/producers"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(whiteLabelModels)
  .dependsOn(repository_mongo)
  .dependsOn(producers)
  .settings(libraryDependencies ++= Dependencies.plugin_hbase_spark)
  .settings(libraryDependencies += Dependencies.log4j1)
  .settings(libraryDependencies ++= Dependencies.log4j :+ Dependencies.darwinConfluentConnector)
  .enablePlugins(JavaAppPackaging)

lazy val whiteLabelConsumersSpark = Project("wasp-whitelabel-consumers-spark", file("whitelabel/consumers-spark"))
  .settings(Settings.commonSettings: _*)
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
  .dependsOn(plugin_continuous_update_spark)
  .settings(
    libraryDependencies ++= Dependencies.log4j :+ Dependencies.darwinConfluentConnector
      :+ "mysql" % "mysql-connector-java" % "5.1.6"
      :+ Dependencies.scalaTest
      :+ Dependencies.darwinMockConnector
  )
  .enablePlugins(JavaAppPackaging)

lazy val whiteLabelConsumersRt = Project("wasp-whitelabel-consumers-rt", file("whitelabel/consumers-rt"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(whiteLabelModels)
  .dependsOn(consumers_rt)
  .dependsOn(plugin_hbase_spark)
  .settings(libraryDependencies ++= Dependencies.log4j)
  .enablePlugins(JavaAppPackaging)

lazy val whiteLabelSingleNode = project.withId("wasp-whitelabel-singlenode").in(file("whitelabel/single-node"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(whiteLabelMaster)
  .dependsOn(whiteLabelConsumersSpark)
  .dependsOn(whiteLabelProducers)
  .enablePlugins(JavaAppPackaging)

lazy val whiteLabel = Project("wasp-whitelabel", file("whitelabel"))
  .settings(Settings.commonSettings: _*)
  .aggregate(whiteLabelModels, whiteLabelMaster, whiteLabelProducers, whiteLabelConsumersSpark, whiteLabelConsumersRt, whiteLabelSingleNode)

lazy val openapi = Project("wasp-openapi", file("openapi"))
  .settings(Settings.commonSettings: _*)
  .settings(libraryDependencies += Dependencies.swaggerCore)
  .settings(
    generateOpenApi := {
      (runMain in Compile)
        .toTask(" it.agilelab.bigdata.wasp.master.web.openapi.GenerateOpenApi documentation/wasp-openapi.yaml")
        .value
    },
    generateOpenApi := (generateOpenApi dependsOn (compile in Compile)).value
  )
  .dependsOn(core)
