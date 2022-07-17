import sbt.Keys.{libraryDependencies, transitiveClassifiers}
import com.typesafe.sbt.packager.Keys.scriptClasspath
import sbt._

class Cdh6Dependencies(versions: Cdh6Versions) extends Dependencies {

  val jacksonDependencies = Seq(
    "com.fasterxml.jackson.core"     % "jackson-annotations"             % "2.10.1" force (),
    "com.fasterxml.jackson.core"     % "jackson-core"                    % "2.10.1" force (),
    "com.fasterxml.jackson.core"     % "jackson-databind"                % "2.10.1" force (),
    "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8"           % "2.10.1" force (),
    "com.fasterxml.jackson.jaxrs"    % "jackson-jaxrs-base"              % "2.10.1" force (),
    "com.fasterxml.jackson.jaxrs"    % "jackson-jaxrs-json-provider"     % "2.10.1" force (),
    "com.fasterxml.jackson.module"   % "jackson-module-jaxb-annotations" % "2.10.1" force (),
    "com.fasterxml.jackson.module"   % "jackson-module-paranamer"        % "2.10.1" force (),
    "com.fasterxml.jackson.module"   % "jackson-module-scala_2.11"       % "2.10.1" force ()
  )

  lazy val mongoTest = "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "3.4.6" % Test

  implicit class Exclude(module: ModuleID) {
    def log4jExclude: ModuleID =
      module excludeAll ExclusionRule("log4j")

    def logbackExclude: ModuleID = module excludeAll ExclusionRule("ch.qos.logback")

    def json4sExclude = module excludeAll ExclusionRule(
      "org.json4s"
    )

    def embeddedExclusions: ModuleID =
      module.log4jExclude
        .excludeAll(ExclusionRule("org.apache.spark"))
        .excludeAll(ExclusionRule("com.typesafe"))

    def driverExclusions: ModuleID =
      module.log4jExclude
        .exclude("com.google.guava", "guava")
        .excludeAll(ExclusionRule("org.slf4j"))

    def hbaseExclusion: ModuleID =
      module.log4jExclude
        .exclude("io.netty", "netty")
        .exclude("io.netty", "netty-all")
        .exclude("com.google.guava", "guava")
        .exclude("org.mortbay.jetty", "jetty-util")
        .exclude("org.mortbay.jetty", "jetty")
        .exclude("org.apache.zookeeper", "zookeeper")

    def solrExclusion: ModuleID =
      module.log4jExclude
        .excludeAll("org.apache.spark")
        .exclude("org.objenesis", "objenesis")
        .exclude("org.apache.zookeeper", "zookeeper")

    def sparkExclusions: ModuleID =
      module.log4jExclude
        .exclude("io.netty", "netty")
        .exclude("com.google.guava", "guava")
        .exclude("org.apache.spark", "spark-core")
        .exclude("org.apache.kafka", "kafka-clients")
        .exclude("org.slf4j", "slf4j-log4j12")
        .exclude("org.apache.logging.log4j", "log4j-api")
        .exclude("org.apache.logging.log4j", "log4j-core")
        .exclude("org.apache.logging.log4j", "log4j-slf4j-impl")
        .exclude("org.apache.solr", "solr-solrj")
        .exclude("org.apache.solr", "solr-core")
        .exclude("org.apache.solr", "solr-test-framework")

    def sttpExclusions: ModuleID = json4sExclude

    def kafkaExclusions: ModuleID =
      module
        .excludeAll(ExclusionRule("org.slf4j"))
        .exclude("com.sun.jmx", "jmxri")
        .exclude("com.sun.jdmk", "jmxtools")
        .exclude("net.sf.jopt-simple", "jopt-simple")
        .exclude("net.jpountz.lz4", "lz4")

    def kafka8Exclusions: ModuleID =
      module
        .exclude("org.apache.kafka", "kafka_2.11")

    def kryoExclusions: ModuleID =
      module
        .exclude("net.jpountz.lz4", "lz4")

    // these are needed because kafka brings in jackson-core/databind 2.8.5, which are incompatible with Spark
    // and otherwise cause a jackson compatibility exception
    def kafkaJacksonExclusions: ModuleID =
      module
        .exclude("com.fasterxml.jackson.core", "jackson-core")
        .exclude("com.fasterxml.jackson.core", "jackson-databind")
        .exclude("com.fasterxml.jackson.core", "jackson-annotations")
  }

  val excludeLog4j: (sbt.ModuleID) => ModuleID = (module: ModuleID) =>
    module.excludeAll(
      sbt.ExclusionRule(organization = "org.apache.logging.log4j", name = "log4j-api"),
      sbt.ExclusionRule(organization = "org.apache.logging.log4j", name = "log4j-core"),
      sbt.ExclusionRule(organization = "org.apache.logging.log4j", name = "log4j-slf4j-impl"),
      sbt.ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12")
    )

  val excludeJavaxRs: (sbt.ModuleID) => ModuleID = (module: ModuleID) =>
    module.excludeAll(
      sbt.ExclusionRule(organization = "javax.ws.rs", name = "javax.was.rs-api")
    )

  val excludeNetty: (sbt.ModuleID) => ModuleID = (module: ModuleID) =>
    module.excludeAll(
      sbt.ExclusionRule(organization = "io.netty", name = "netty"),
      sbt.ExclusionRule(organization = "io.netty", name = "netty-all")
      //sbt.ExclusionRule(organization = "com.google.guava", name = "guava")
    )

  val excludeHive: (sbt.ModuleID) => ModuleID = (module: ModuleID) =>
    module.excludeAll(
      sbt.ExclusionRule(organization = "org.apache.spark", name = "spark-core")
    )

  // ===================================================================================================================
  // Compile dependencies
  // ===================================================================================================================
  val akkaActor          = "com.typesafe.akka" %% "akka-actor" % versions.akka
  val akkaCluster        = "com.typesafe.akka" %% "akka-cluster" % versions.akka
  val akkaClusterTools   = "com.typesafe.akka" %% "akka-cluster-tools" % versions.akka
  val akkaContrib        = "com.typesafe.akka" %% "akka-contrib" % versions.akka
  val akkaHttp           = "com.typesafe.akka" %% "akka-http" % versions.akkaHttp
  val akkaHttpSpray      = "com.typesafe.akka" %% "akka-http-spray-json" % versions.akkaHttp
  val akkaKryo           = "com.github.romix.akka" %% "akka-kryo-serialization" % "0.5.0"
  val akkaRemote         = "com.typesafe.akka" %% "akka-remote" % versions.akka
  val akkaSlf4j          = "com.typesafe.akka" %% "akka-slf4j" % versions.akka
  val akkaStream         = "com.typesafe.akka" %% "akka-stream" % versions.akka
  val akkaStreamTestkit  = "com.typesafe.akka" %% "akka-stream-testkit" % versions.akka % Test
  val akkaHttpTestKit    = "com.typesafe.akka" %% "akka-http-testkit" % versions.akkaHttp % Test
  val apacheCommonsLang3 = "org.apache.commons" % "commons-lang3" % versions.apacheCommonsLang3Version // remove?
  val avro               = "org.apache.avro" % "avro" % versions.avro
  val delta              = ("it.agilelab" %% "wasp-delta-lake" % versions.delta).log4jExclude.logbackExclude

  // avro4s requires a json4s version incompatible with wasp, downstream projects confirmed that this exclusion does
  // not create issues with darwin and avro parsing
  val avro4sCore = "com.sksamuel.avro4s" % "avro4s-core_2.11" % versions.avro4sVersion excludeAll ExclusionRule(
    "org.json4s"
  )
  val avro4sJson = "com.sksamuel.avro4s" % "avro4s-json_2.11" % versions.avro4sVersion excludeAll ExclusionRule(
    "org.json4s"
  )
  val darwinCore                = "it.agilelab" %% "darwin-core" % versions.darwin
  val darwinConfluentConnector  = excludeLog4j("it.agilelab" %% "darwin-confluent-connector" % versions.darwin)
  val darwinHBaseConnector      = "it.agilelab" %% "darwin-hbase-connector" % versions.darwin
  val darwinMockConnector       = "it.agilelab" %% "darwin-mock-connector" % versions.darwin
  val commonsCli                = "commons-cli" % "commons-cli" % versions.commonsCli
  val elasticSearch             = "org.elasticsearch" % "elasticsearch" % versions.elasticSearch
  val elasticSearchSpark        = "org.elasticsearch" %% "elasticsearch-spark-20" % versions.elasticSearchSpark
  val guava                     = "com.google.guava" % "guava" % versions.guava
  val hbaseClient               = "org.apache.hbase" % "hbase-client" % versions.hbase hbaseExclusion
  val hbaseCommon               = "org.apache.hbase" % "hbase-common" % versions.hbase hbaseExclusion
  val hbaseServer               = "org.apache.hbase" % "hbase-server" % versions.hbase hbaseExclusion
  val hbaseMapreduce            = "org.apache.hbase" % "hbase-mapreduce" % versions.hbase hbaseExclusion
  val hbaseTestingUtils         = "org.apache.hbase" % "hbase-testing-util" % versions.hbase % Test
  val httpClient                = "org.apache.httpcomponents" % "httpclient" % versions.httpcomponents
  val httpCore                  = "org.apache.httpcomponents" % "httpcore" % versions.httpcomponents
  val httpmime                  = "org.apache.httpcomponents" % "httpmime" % "4.3.1" // TODO remove?
  val javaxMail                 = "javax.mail" % "mail" % "1.4"
  val json4sCore                = "org.json4s" %% "json4s-core" % versions.json4s
  val json4sJackson             = "org.json4s" %% "json4s-jackson" % versions.json4s
  val json4sNative              = "org.json4s" %% "json4s-native" % versions.json4s
  val kafka                     = ("org.apache.kafka" %% "kafka" % versions.kafka).kafkaExclusions.kafkaJacksonExclusions
  val kafkaClients              = ("org.apache.kafka" % "kafka-clients" % versions.kafka).kafkaExclusions.kafkaJacksonExclusions
  val kafkaSchemaRegistryClient = "io.confluent" % "kafka-schema-registry-client" % "3.3.3"
  val log4jApi                  = "org.apache.logging.log4j" % "log4j-api" % versions.log4j % "optional,test"
  val log4jCore                 = "org.apache.logging.log4j" % "log4j-core" % versions.log4j % "optional,test"
  val log4jSlf4jImpl            = "org.apache.logging.log4j" % "log4j-slf4j-impl" % versions.log4j % "optional,test"
  val log4j1                    = "log4j" % "log4j" % versions.log4j1
  val metrics                   = "com.yammer.metrics" % "metrics-core" % "2.2.0" // TODO upgrade?
  val mongodbScala              = "org.mongodb.scala" %% "mongo-scala-driver" % versions.mongodbScala
  val netty                     = "io.netty" % "netty" % versions.netty
  val nettySpark                = "io.netty" % "netty" % versions.nettySpark
  val nettyAll                  = "io.netty" % "netty-all" % versions.nettyAllSpark
  val quartz                    = "org.quartz-scheduler" % "quartz" % versions.quartz
  val scalaj                    = "org.scalaj" %% "scalaj-http" % "1.1.4" // TODO remove?
  val scaldi                    = "org.scaldi" %% "scaldi-akka" % "0.3.3" // TODO remove?
  val slf4jApi                  = "org.slf4j" % "slf4j-api" % versions.slf4j
  val solrCore                  = "org.apache.solr" % "solr-core" % versions.solr solrExclusion
  val solrj                     = "org.apache.solr" % "solr-solrj" % versions.solr solrExclusion
  val sparkCatalyst             = "org.apache.spark" %% "spark-catalyst" % versions.spark
  val sparkCore                 = "org.apache.spark" %% "spark-core" % versions.spark sparkExclusions
  val sparkMLlib                = "org.apache.spark" %% "spark-mllib" % versions.spark sparkExclusions
  val sparkSolr = versions.scala.take(4) match {
    case "2.11" => ("it.agilelab.bigdata.spark" % "spark-solr"  % versions.sparkSolr).sparkExclusions.solrExclusion
    case "2.12" => ("it.agilelab.bigdata.spark" %% "spark-solr" % versions.sparkSolr).sparkExclusions.solrExclusion
  }
  val sparkTags                 = "org.apache.spark" %% "spark-tags" % versions.spark sparkExclusions
  val sparkSQL                  = "org.apache.spark" %% "spark-sql" % versions.spark sparkExclusions
  val sparkYarn                 = "org.apache.spark" %% "spark-yarn" % versions.spark sparkExclusions
  val sparkHive                 = "org.apache.spark" %% "spark-hive" % versions.spark sparkExclusions
  val swaggerCore               = "io.swagger.core.v3" % "swagger-core" % "2.1.2"
  val typesafeConfig            = "com.typesafe" % "config" % "1.3.0"
  val velocity                  = "org.apache.velocity" % "velocity" % "1.7"
  val zkclient                  = "com.101tec" % "zkclient" % "0.3"
  val scalaParserAndCombinators = "org.scala-lang.modules" %% "scala-parser-combinators" % versions.scalaParserAndCombinators
  val scalaCompiler             = "org.scala-lang" % "scala-compiler" % versions.scala
  val scalaPool                 = "io.github.andrebeat" %% "scala-pool" % "0.4.3"
  val mySql                     = "mysql" % "mysql-connector-java" % "5.1.6"
  val nameOf                    = "com.github.dwickern" %% "scala-nameof" % "1.0.3" % "provided"
  val solrjMasterClient         = "org.apache.solr" % "solr-solrj" % versions.solr solrExclusion
  val sttpCore                  = "com.softwaremill.sttp.client" %% "core" % versions.sttpVersion sttpExclusions
  val sttpJson4s                = "com.softwaremill.sttp.client" %% "json4s" % versions.sttpVersion sttpExclusions
  val reflections               = "org.reflections" % "reflections" % versions.reflectionsVersion
  val postgres                  = "org.postgresql" % "postgresql" % versions.postgresqlVersion
  val dpcp2                     = "org.apache.commons" % "commons-dbcp2" % versions.dbcp2Version
  val postgresqlEmbedded        = "com.opentable.components" % "otj-pg-embedded" % versions.postgresqlEmbeddedVersion % Test
  val mockOkHttp2               = "com.squareup.okhttp" % "mockwebserver" % "2.7.5" % Test // in sync with cdh6
  // why these are the only dependencies with snack case?
  val spark_sql_kafka     = "it.agilelab"      %% "wasp-spark-sql-kafka"     % ("0.0.2" + "-" + versions.kafka_ + "-" + versions.spark)
  val spark_sql_kafka_old = "it.agilelab"      %% "wasp-spark-sql-kafka-old" % ("0.0.2" + "-" + versions.kafka_ + "-" + versions.spark)
  val prettyPrint         = "com.lihaoyi"      %% "pprint"                   % "0.6.6"
  val sparkAvro           = "org.apache.spark" %% "spark-avro"               % versions.spark
  val shapeless           = "com.chuusai"      %% "shapeless"                % "2.3.3"

  // grouped dependencies, for convenience =============================================================================
  val akka = Seq(
    akkaActor,
    akkaCluster,
    akkaClusterTools,
    akkaContrib,
    akkaRemote,
    akkaSlf4j,
    akkaKryo.kryoExclusions
  )

  val hbase = Seq(hbaseClient, hbaseCommon, hbaseServer, hbaseMapreduce)

  val json = Seq(json4sCore, json4sJackson, json4sNative)

  val logging = Seq(slf4jApi)

  val log4j = Seq(log4jApi, log4jCore, log4jSlf4jImpl)

  val spark = Seq(sparkCore, sparkMLlib, sparkSQL, sparkYarn)

  val schemaRegistry = Seq(darwinCore, darwinHBaseConnector)

  val avro4s = Seq(avro4sCore, avro4sJson)

  val avro4sTest          = avro4s.map(_                          % Test)
  val avro4sTestAndDarwin = avro4sTest ++ Seq(darwinMockConnector % Test)

  val jaxRs = "jakarta.ws.rs" % "jakarta.ws.rs-api" % "2.1.5"

  val wireMock =
    Seq("com.github.tomakehurst" % "wiremock-standalone" % "2.25.0" % Test, "xmlunit" % "xmlunit" % "1.6" % Test)

  // ===================================================================================================================
  // Test dependencies
  // ===================================================================================================================
  val akkaClusterTestKit = "com.typesafe.akka" %% "akka-multi-node-testkit" % versions.akka % Test
  val akkaTestKit        = "com.typesafe.akka" %% "akka-testkit" % versions.akka % Test
  val joptSimpleTests    = "net.sf.jopt-simple" % "jopt-simple" % versions.jopt % Test
  val kafkaTests         = kafka % Test kafkaJacksonExclusions
  val scalaCheck         = "org.scalacheck" %% "scalacheck" % versions.scalaCheck % Test
  val scalaTest          = "org.scalatest" %% "scalatest" % versions.scalaTest % Test
  val sparkCatalystTests = sparkCatalyst % Test classifier "tests"
  val sparkCoreTests     = sparkCore % Test classifier "tests"
  val sparkSQLTests      = sparkSQL % Test classifier "tests"
  val sparkTagsTests     = sparkTags % Test classifier "tests"

  // grouped dependencies, for convenience =============================================================================

  val spark_sql_kafka_0_11 = (Seq( // normal dependencies
    guava,
    kafkaClients,
    sparkSQL,
    sparkTags
  ) ++ Seq( // test dependencies
    joptSimpleTests,
    kafkaTests,
    scalaCheck,
    sparkCatalystTests,
    sparkCoreTests,
    sparkSQLTests,
    sparkTagsTests,
    jaxRs,
    scalaTest
  )).map(excludeLog4j) ++ log4j :+ log4j1

  // ===================================================================================================================
  // Module dependencies
  // ===================================================================================================================

  val scalaTestDependencies = Seq(scalaTest, mongoTest)

  val testDependencies = Seq(akkaTestKit, akkaClusterTestKit, scalaTest, mongoTest)

  val modelDependencies = (
    json :+
      akkaHttpSpray :+
      sparkSQL :+
      mongodbScala
  ).map(excludeLog4j).map(excludeNetty) ++ testDependencies :+ darwinCore

  val coreDependencies = (akka ++
    avro4sTest ++
    logging ++
    jacksonDependencies ++
    testDependencies :+
    akkaHttp :+
    akkaHttpSpray :+
    avro :+
    commonsCli :+
    kafka :+ // TODO remove when switching to plugins
    sparkSQL :+
    typesafeConfig :+
    reflections).map(excludeLog4j).map(excludeNetty) ++ testDependencies :+ darwinCore

  val repositoryMongoDependencies = Seq(
    mongodbScala,
    nameOf,
    shapeless
  ).map(excludeLog4j).map(excludeNetty) ++ testDependencies

  val repositoryPostgresDependencies = Seq(
    postgres,
    dpcp2,
    postgresqlEmbedded
  ).map(excludeLog4j).map(excludeNetty) ++ testDependencies

  val producersDependencies = (
    akka ++
      testDependencies :+
      akkaHttp :+
      akkaStream :+
      netty
  ).map(excludeLog4j) ++ log4j :+ log4j1

  val consumersSparkDependencies = schemaRegistry ++ (
    akka ++
      testDependencies ++
      avro4sTestAndDarwin ++
      hbase ++
      spark :+
      quartz :+
      nameOf :+
      velocity :+ //TODO: evaluate this is legal
      scalaCompiler :+
      sparkAvro :+
      sparkHive   % Test :+
      prettyPrint % Test
  ).map(excludeNetty).map(excludeLog4j) ++
    wireMock ++
    log4j :+
    log4j1 :+
    nettySpark :+
    nettyAll :+
    guava

  val masterDependencies = (
    akka :+
      akkaHttp :+
      akkaHttpSpray :+
      netty :+
      scalaTest :+
      akkaHttpTestKit :+
      akkaStreamTestkit :+
      solrjMasterClient :+
      httpClient
  ).map(excludeLog4j) ++ log4j

  val pluginElasticSparkDependencies: Seq[ModuleID] = Seq(
    elasticSearchSpark
  )

  val pluginHttpSparkDependencies = Seq(mockOkHttp2, scalaTest)

  val pluginHbaseSparkDependencies = (
    hbase :+
      scalaTest
  ).map(excludeNetty)

  val pluginPlainHbaseWriterSparkDependencies = (
    hbase :+
      scalaTest :+ hbaseTestingUtils
  ).map(excludeNetty)

  val _plugin_kafka_spark = Seq(
    guava,
    kafkaClients,
    sparkSQL,
    sparkTags,
    scalaTest
  ).map(excludeLog4j).map(excludeNetty)

  val pluginKafkaSparkDependencies    = _plugin_kafka_spark :+ spark_sql_kafka
  val pluginKafkaSparkOldDependencies = _plugin_kafka_spark :+ spark_sql_kafka_old

  val pluginSolrSparkDependencies = Seq(
    httpClient,
    httpCore,
    solrj,
    sparkSolr
  ).map(excludeNetty)

  val pluginMongoSparkDependencies = Seq(
    ("org.mongodb.spark" %% "mongo-spark-connector" % "2.4.3").exclude("org.mongodb", "mongo-java-driver"),
    "org.mongodb" % "mongo-java-driver" % "3.12.2"
  ).map(excludeNetty)

  val pluginMailerSparkDependencies = Seq(
    javaxMail,
    scalaTest
  )

  val openapi = swaggerCore

  val nifiClientDependencies: Seq[sbt.ModuleID] = (
    akka :+
      akkaHttp :+
      akkaHttpSpray :+
      sttpCore :+
      sttpJson4s :+
      json4sJackson
  )

  val nifiStatelessDependencies: Seq[ModuleID] = Seq(
    "org.apache.nifi" % "nifi-stateless" % versions.nifi % "provided"
  )
  val delta_lake: Seq[ModuleID] = jacksonDependencies :+ sparkHive

  val pluginCdcSparkDependencies = Seq(
    delta,
    scalaTest
  ) ++ log4j

  val kmsTest: Seq[Def.Setting[_]] = Seq(
    (Test / transitiveClassifiers) := Seq(Artifact.TestsClassifier, Artifact.SourceClassifier),
    libraryDependencies ++= Seq(
      ("org.codehaus.jackson" % "jackson-core-asl"   % "1.9.13")          % "test",
      ("org.codehaus.jackson" % "jackson-jaxrs"      % "1.9.13")          % "test",
      ("org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13")          % "test",
      ("org.eclipse.jetty"    % "jetty-security"     % "9.3.25.v20180904" % "test"),
      ("org.apache.hadoop"    % "hadoop-common"      % versions.kms)      % "test",
      ("org.apache.hadoop"    % "hadoop-kms"         % versions.kms)      % "test",
      ("org.apache.hadoop" % "hadoop-kms" % versions.kms).classifier("tests") % "test"
    )
  )

  val scalaCompilerDependencies: Seq[ModuleID] = testDependencies :+ scalaCompiler :+ scalaPool

  val microserviceCatalogDependencies: Seq[ModuleID] =
    Seq(scalaTest) ++ pluginHttpSparkDependencies

  val pluginParallelWriteSparkDependencies: Seq[ModuleID] =
    Seq(scalaTest) ++ pluginHttpSparkDependencies ++ Seq(delta, "org.apache.hadoop" % "hadoop-aws" % "3.0.0")

  val yarnAuthHdfsDependencies: Seq[ModuleID]  = Seq(scalaTest, sparkYarn)
  val yarnAuthHBaseDependencies: Seq[ModuleID] = Seq(sparkYarn, hbaseServer, hbaseCommon)
  val sparkTelemetryPluginDependencies: Seq[ModuleID] =
    Seq(sparkCore, kafkaClients, scalaParserAndCombinators)
  val sparkNifiPluginDependencies: Seq[ModuleID]  = Seq(sparkCore)
  val whitelabelModelsDependencies: Seq[ModuleID] = log4j ++ avro4s
  val whitelabelMasterDependencies: Seq[ModuleID] =
    pluginHbaseSparkDependencies ++ Seq(log4j1) ++ log4j ++ Seq(darwinHBaseConnector)
  val whitelabelProducerDependencies: Seq[ModuleID] =
    pluginHbaseSparkDependencies ++ Seq(log4j1) ++ log4j ++ Seq(darwinHBaseConnector)

  val whitelabelSparkConsumerDependencies: Seq[ModuleID] = log4j :+
    darwinHBaseConnector :+
    "mysql" % "mysql-connector-java" % "5.1.6" :+
    scalaTest :+
    darwinMockConnector

  val openapiDependencies: Seq[ModuleID]                   = Seq(swaggerCore)
  val repositoryCoreDependencies: Seq[ModuleID]            = testDependencies ++ Seq(shapeless)
  override val sparkPluginBasicDependencies: Seq[ModuleID] = scalaTestDependencies
  override val awsAuth: Seq[ModuleID] = Seq(
    "org.apache.hadoop" % "hadoop-aws"          % versions.hadoop,
    "org.apache.hadoop" % "hadoop-common"       % versions.hadoop,
    "com.amazonaws"     % "aws-java-sdk-bundle" % versions.awsBundle force ()
  )

  override val whitelabelMasterScriptClasspath        = scriptClasspath += ""
  override val whitelabelProducerScriptClasspath      = scriptClasspath += ""
  override val whitelabelSparkConsumerScriptClasspath = scriptClasspath += ":$HADOOP_CONF_DIR:$YARN_CONF_DIR"
  override val whiteLabelConsumersRtScriptClasspath   = scriptClasspath += ""
  override val whiteLabelSingleNodeScriptClasspath    = scriptClasspath += ""
}
