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

/* Framework */

lazy val scala_compiler = Project("wasp-compiler", file("compiler"))
  .settings(Settings.commonSettings: _*)
  .settings(Settings.sbtBuildInfoSettings: _*)
  .settings(libraryDependencies ++= Dependencies.test :+ Dependencies.scalaCompiler)
  .enablePlugins(BuildInfoPlugin)

lazy val core = Project("wasp-core", file("core"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(scala_compiler)
  .settings(Settings.sbtBuildInfoSettings: _*)
  .settings(libraryDependencies ++= Dependencies.core ++ Dependencies.test :+ Dependencies.darwinCore)
  .enablePlugins(BuildInfoPlugin)

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
  .settings(libraryDependencies ++= Dependencies.consumers_spark)
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
  .dependsOn(consumers_spark)
  .dependsOn(spark_sql_kafka_0_11)
  .settings(libraryDependencies ++= Dependencies.plugin_kafka_spark)

lazy val plugin_raw_spark = Project("wasp-plugin-raw-spark", file("plugin-raw-spark"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(consumers_spark)

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

lazy val spark = Project("wasp-spark", file("spark"))
  .settings(Settings.commonSettings: _*)
  .aggregate(spark_telemetry_plugin)

/* nifi */

lazy val nifi_client = Project("wasp-nifi-client", file("nifi-client"))
  .settings(Settings.commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.nifiClient)


/* Framework + Plugins */

lazy val wasp = Project("wasp", file("."))
  .settings(Settings.commonSettings: _*)
  .aggregate(scala_compiler,
    core,
    master,
    producers,
    consumers_spark,
    consumers_rt,
    plugin_console_spark,
    plugin_hbase_spark,
    plugin_jdbc_spark,
    plugin_kafka_spark,
    plugin_raw_spark,
    plugin_solr_spark,
    yarn,
    spark,
    plugin_mailer_spark,
    spark_sql_kafka_0_11,
    openapi,
    nifi_client)

/* WhiteLabel */

lazy val whiteLabelModels = Project("wasp-whitelabel-models", file("whitelabel/models"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(core)
  .settings(libraryDependencies ++= Dependencies.log4j ++ Dependencies.avro4s)

lazy val whiteLabelMaster = Project("wasp-whitelabel-master", file("whitelabel/master"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(whiteLabelModels)
  .dependsOn(master)
  .dependsOn(plugin_hbase_spark)
  .settings(libraryDependencies ++= Dependencies.log4j :+ Dependencies.darwinHBaseConnector)
  .enablePlugins(JavaAppPackaging)

lazy val whiteLabelProducers = Project("wasp-whitelabel-producers", file("whitelabel/producers"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(whiteLabelModels)
  .dependsOn(producers)
  .dependsOn(plugin_hbase_spark)
  .settings(libraryDependencies ++= Dependencies.log4j :+ Dependencies.darwinHBaseConnector)
  .enablePlugins(JavaAppPackaging)

lazy val whiteLabelConsumersSpark = Project("wasp-whitelabel-consumers-spark", file("whitelabel/consumers-spark"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(whiteLabelModels)
  .dependsOn(consumers_spark)
  .dependsOn(plugin_console_spark)
  .dependsOn(plugin_hbase_spark)
  .dependsOn(plugin_jdbc_spark)
  .dependsOn(plugin_kafka_spark)
  .dependsOn(plugin_mailer_spark)
  .dependsOn(plugin_raw_spark)
  .dependsOn(plugin_solr_spark)
  .dependsOn(plugin_mongo_spark)
  .dependsOn(spark_telemetry_plugin)
  .settings(libraryDependencies ++= Dependencies.log4j :+ Dependencies.darwinHBaseConnector
    :+ "mysql" % "mysql-connector-java" % "5.1.6"
    :+ Dependencies.scalaTest
    :+ Dependencies.darwinMockConnector)
  .enablePlugins(JavaAppPackaging)

lazy val whiteLabelConsumersRt = Project("wasp-whitelabel-consumers-rt", file("whitelabel/consumers-rt"))
  .settings(Settings.commonSettings: _*)
  .dependsOn(whiteLabelModels)
  .dependsOn(consumers_rt)
  .dependsOn(plugin_hbase_spark)
  .settings(libraryDependencies ++= Dependencies.log4j)
  .enablePlugins(JavaAppPackaging)

lazy val whiteLabel = Project("wasp-whitelabel", file("whitelabel"))
  .settings(Settings.commonSettings: _*)
  .aggregate(whiteLabelModels, whiteLabelMaster, whiteLabelProducers, whiteLabelConsumersSpark, whiteLabelConsumersRt)

lazy val openapi = Project("wasp-openapi", file("openapi"))
  .settings(Settings.commonSettings: _*)
  .settings(libraryDependencies += Dependencies.swaggerCore)
  .settings(

    generateOpenApi := {
      (runMain in Compile).toTask(" it.agilelab.bigdata.wasp.master.web.openapi.GenerateOpenApi documentation/wasp-openapi.yaml").value
    },

    generateOpenApi := (generateOpenApi dependsOn (compile in Compile)).value
  )
  .dependsOn(core)
