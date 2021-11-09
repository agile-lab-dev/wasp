import com.typesafe.sbt.packager.Keys.scriptClasspath
import sbt._
import sbt.Keys.transitiveClassifiers

class Vanilla2Dependencies(val versions: Vanilla2Versions)
    extends Dependencies
    with AkkaDependencies
    with DarwinDependencies
    with HBaseDependencies
    with SparkDependencies
    with LoggingDependencies
    with KafkaDependencies
    with MongoDependencies
    with CamelDependencies
    with Json4sDependencies
    with NettyDependencies
    with TestFrameworkDependencies
    with ScalaCoreDependencies
    with AvroDependencies
    with ApacheCommonsDependencies
    with SolrDependencies
    with CodehausJacksonDependencies
    with OkHttpDependencies
    with PostgresDependencies
    with SttpDependencies {
  val exclusions: VanillaExclusions.type = VanillaExclusions

  lazy val delta               = "io.delta" %% "delta-core" % versions.delta exclude exclusions.log4jExclude
  lazy val elasticSearchSpark  = "org.elasticsearch" %% "elasticsearch-spark-20" % versions.elasticSearchSpark
  lazy val guava               = "com.google.guava" % "guava" % versions.guava
  lazy val javaxMail           = "javax.mail" % "mail" % versions.javaxMail
  lazy val metrics             = "com.yammer.metrics" % "metrics-core" % versions.yammerMetrics // TODO upgrade?
  lazy val quartz              = "org.quartz-scheduler" % "quartz" % versions.quartz
  lazy val swaggerCore         = "io.swagger.core.v3" % "swagger-core" % versions.swagger
  lazy val velocity            = "org.apache.velocity" % "velocity" % versions.velocity
  lazy val kryo                = "com.esotericsoftware" % "kryo-shaded" % versions.kryo
  lazy val reflections         = "org.reflections" % "reflections" % versions.reflectionsVersion
  lazy val mySqlJavaConnector  = "mysql" % "mysql-connector-java" % versions.mySqlConnector
  lazy val jaxRs               = "jakarta.ws.rs" % "jakarta.ws.rs-api" % versions.jakartaRsApi
  lazy val nifiStateless       = "org.apache.nifi" % "nifi-stateless" % versions.nifi % Provided exclude exclusions.javaxRsExclude
  lazy val joptSimpleTests     = "net.sf.jopt-simple" % "jopt-simple" % versions.jopt % Test
  lazy val jettySecurity       = "org.eclipse.jetty" % "jetty-security" % versions.jettySecurity
  lazy val avro4sTestAndDarwin = avro4sTest ++ Seq(darwinMockConnector % Test)
  lazy val mongoTest           = "com.github.simplyscala" %% "scalatest-embedmongo" % "0.2.4" % Test

  lazy val _pluginKafkaSparkDependencies: Seq[ModuleID] = spark ++ Seq(
    guava,
    kafkaClients,
    scalaTest
  )

  override val scalaTestDependencies: Seq[ModuleID] = Seq(scalaTest, mongoTest)

  override val testDependencies: Seq[ModuleID] = Seq(akkaTestKit, akkaClusterTestKit, scalaTest, mongoTest)

  override val modelDependencies: Seq[ModuleID] = (json ++ Seq(
    akkaHttpSpray,
    sparkSQL,
    mongoBsonScala
  )).map(_.exclude(exclusions.log4jExclude ++ exclusions.nettyExclude)) ++ scalaTestDependencies

  override val coreDependencies: Seq[ModuleID] = (akka ++
    avro4sTest ++
    logging ++
    testDependencies ++ Seq(
    akkaHttp,
    akkaHttpSpray,
    avro,
    commonsCli,
    kafka, // TODO remove when switching to plugins
    sparkSQL,
    typesafeConfig,
    scalaCompiler,
    apacheCommonsLang3,
    darwinCore,
    reflections
  ) ++ spark).map(_.exclude(exclusions.log4jExclude ++ exclusions.nettyExclude))

  override val repositoryMongoDependencies: Seq[ModuleID] = Seq(
    mongodbScala,
    nameOf,
    sparkSQL
  ).map(_.exclude(exclusions.log4jExclude ++ exclusions.nettyExclude)) ++ scalaTestDependencies

  override val repositoryPostgresDependencies: Seq[ModuleID] = Seq(
    postgres,
    dpcp2,
    postgresqlEmbedded,
    sparkSQL
  ).map(_.exclude(exclusions.log4jExclude ++ exclusions.nettyExclude)) ++ scalaTestDependencies

  override val scalaCompilerDependencies: Seq[ModuleID] = (testDependencies ++ Seq(scalaCompiler, scalaPool))
    .map(_.exclude(exclusions.log4jExclude ++ exclusions.nettyExclude))

  override val producersDependencies: Seq[ModuleID] = (
    akka ++ testDependencies ++ Seq(commonsIO, akkaHttp, akkaStream, netty, commonsCli)
  ).map(_.exclude(exclusions.log4jExclude))

  override val consumersSparkDependencies: Seq[ModuleID] = schemaRegistry ++ (
    akka ++
      testDependencies ++
      avro4sTestAndDarwin ++
      hbase2 ++ // maybe remove this, we need to refactor the gdpr part for hbase
      wireMock ++
      spark ++
      Seq(
        quartz,
        nameOf,
        velocity, //TODO: evaluate this is legal
        scalaCompiler,
        sparkAvro
      )
  ).map(_.exclude(exclusions.nettyExclude)).map(_.exclude(exclusions.log4jExclude)) ++
    Seq(nettySpark, nettyAll, guava) ++ logging

  override val consumersRtDependencies: Seq[ModuleID] = (
    akka ++
      Seq(akkaCamel, camelKafka, camelWebsocket, kafka, netty, commonsCli)
  ).map(_.exclude(exclusions.log4jExclude))

  override val masterDependencies: Seq[ModuleID] = (
    json ++
      akka ++
      Seq(
        sparkSQL,
        akkaHttp,
        akkaHttpSpray,
        netty,
        scalaTest,
        akkaHttpTestKit,
        akkaStreamTestkit,
        solrjMasterClient,
        httpClient
      )
  ).map(_.exclude(exclusions.log4jExclude))

  override val pluginElasticSparkDependencies: Seq[ModuleID] = spark ++ Seq(elasticSearchSpark)

  override val pluginHttpSparkDependencies: Seq[ModuleID] =
    spark ++ Seq(okHttp2, mockOkHttp2, scalaTest).map(_.exclude(exclusions.hiveExclude))

  // here we need to create 2 plugins instead

  override val pluginHbaseSparkDependencies: Seq[ModuleID] =
    (spark ++ hbase2 ++ Seq(scalaTest)).map(_.exclude(exclusions.nettyExclude))

  override val pluginKafkaSparkDependencies: Seq[ModuleID] =
    (Seq(sparkSqlKafka) ++ _pluginKafkaSparkDependencies)
      .map(_.exclude(exclusions.log4jExclude ++ exclusions.nettyExclude)) ++ logging ++ Seq(nettyAll)

  override val pluginKafkaSparkOldDependencies: Seq[ModuleID] =
    (Seq(sparkSqlKafkaOld) ++ _pluginKafkaSparkDependencies)
      .map(_.exclude(exclusions.log4jExclude ++ exclusions.nettyExclude)) ++ logging ++ Seq(nettyAll)

  override val pluginSolrSparkDependencies: Seq[ModuleID] = spark ++ Seq(
    httpClient,
    httpCore,
    solrj,
    sparkSolr
  ).map(_.exclude(exclusions.nettyExclude))

  override val pluginMongoSparkDependencies: Seq[ModuleID] = spark ++ Seq(
    mongoSparkConnector,
    mongoJavaDriver
  ).map(_.exclude(exclusions.nettyExclude))

  override val pluginMailerSparkDependencies: Seq[ModuleID] = spark ++ Seq(javaxMail, scalaTest)

  override val openapiDependencies: Seq[ModuleID] = coreDependencies ++ testDependencies ++ Seq(
    swaggerCore,
    kryo,
    darwinCore
  )

  override val nifiClientDependencies: Seq[ModuleID] = akka ++ Seq(
    akkaHttp,
    akkaHttpSpray,
    sttpCore,
    sttpJson4s,
    json4sJackson
  )

  override val nifiStatelessDependencies: Seq[ModuleID] = Seq(jaxRs, nifiStateless, commonsCli)

  override val pluginCdcSparkDependencies: Seq[ModuleID] = spark ++ Seq(delta, scalaTest)

  override val kmsTest: Seq[Def.Setting[_]] = Seq(
    Test / transitiveClassifiers := Seq(Artifact.TestsClassifier, Artifact.SourceClassifier),
    Keys.libraryDependencies ++= Seq(
      codeHausJacksonCoreAsl    % Test,
      codeHausJacksonJaxRS      % Test,
      codeHausJacksonMapperAsl  % Test,
      jettySecurity             % Test,
      hadoopCommonNoScope       % Test,
      kms.classifier("tests")   % Test,
      kms.classifier("classes") % Test
    )
  )
  override val pluginParallelWriteSparkDependencies: Seq[ModuleID] =
    Seq(scalaTest) ++ pluginHttpSparkDependencies ++ Seq(delta, hadoopAWS)

  override val microserviceCatalogDependencies: Seq[ModuleID] =
    Seq(scalaTest) ++ pluginHttpSparkDependencies

  override val yarnAuthHdfsDependencies: Seq[ModuleID] = Seq(scalaTest, sparkYarn, hadoopCommon)

  override val yarnAuthHBaseDependencies: Seq[ModuleID] = Seq(sparkYarn, hbaseServer2, hbaseCommon2)

  override val sparkTelemetryPluginDependencies: Seq[ModuleID] =
    Seq(sparkCore, kafkaClients, scalaParserAndCombinators)

  override val sparkNifiPluginDependencies: Seq[ModuleID] = spark

  override val repositoryCoreDependencies: Seq[ModuleID] = testDependencies ++ Seq(apacheCommonsLang3)

  override val sparkPluginBasicDependencies: Seq[ModuleID] = spark ++ scalaTestDependencies

  override val whitelabelModelsDependencies: Seq[ModuleID] = avro4s ++ spark

  override val whitelabelMasterDependencies: Seq[ModuleID] =
    pluginHbaseSparkDependencies ++ Seq(darwinHBaseConnector, hbaseClient2Shaded, slf4jLog4j1Binding)

  override val whitelabelProducerDependencies: Seq[ModuleID] =
    pluginHbaseSparkDependencies ++ Seq(darwinHBaseConnector, hbaseClient2Shaded, slf4jLog4j1Binding)

  override val whitelabelSparkConsumerDependencies: Seq[ModuleID] = Seq(
    darwinHBaseConnector,
    mySqlJavaConnector,
    scalaTest,
    darwinMockConnector % Test
  ) ++ spark ++ Seq(hbaseClient2Shaded, slf4jLog4j1Binding)

  override val whiteLabelConsumersRtDependencies: Seq[ModuleID] = Seq(commonsCli)

  override val whitelabelMasterScriptClasspath =
    scriptClasspath := Seq(":$SPARK_HOME/jars/*") ++
      scriptClasspath.value ++
      Seq(":$HADOOP_CONF_DIR:$YARN_CONF_DIR:/$HBASE_CONF_DIR")

  override val whitelabelProducerScriptClasspath =
    scriptClasspath := Seq(":$SPARK_HOME/jars/*") ++
      scriptClasspath.value ++
      Seq(":$HADOOP_CONF_DIR:$YARN_CONF_DIR:/$HBASE_CONF_DIR")

  override val whitelabelSparkConsumerScriptClasspath =
    scriptClasspath := Seq(":$SPARK_HOME/jars/*") ++
      scriptClasspath.value ++
      Seq(":$HADOOP_CONF_DIR:$YARN_CONF_DIR:/$HBASE_CONF_DIR")
  override val whiteLabelConsumersRtScriptClasspath =
    scriptClasspath := Seq(":$SPARK_HOME/jars/*") ++
      scriptClasspath.value ++
      Seq(":$HADOOP_CONF_DIR:$YARN_CONF_DIR:/$HBASE_CONF_DIR")
  override val whiteLabelSingleNodeScriptClasspath =
    scriptClasspath := Seq(":$SPARK_HOME/jars/*") ++
      scriptClasspath.value ++
      Seq(":$HADOOP_CONF_DIR:$YARN_CONF_DIR:/$HBASE_CONF_DIR")
}

trait AkkaDependencies {
  val versions: Vanilla2Versions
  val exclusions: VanillaExclusions.type
  lazy val akkaActor          = "com.typesafe.akka"     %% "akka-actor"              % versions.akka
  lazy val akkaCamel          = "com.typesafe.akka"     %% "akka-camel"              % versions.akka
  lazy val akkaCluster        = "com.typesafe.akka"     %% "akka-cluster"            % versions.akka
  lazy val akkaClusterTools   = "com.typesafe.akka"     %% "akka-cluster-tools"      % versions.akka
  lazy val akkaContrib        = "com.typesafe.akka"     %% "akka-contrib"            % versions.akka
  lazy val akkaHttp           = "com.typesafe.akka"     %% "akka-http"               % versions.akkaHttp
  lazy val akkaHttpSpray      = "com.typesafe.akka"     %% "akka-http-spray-json"    % versions.akkaHttp
  lazy val akkaKryo           = "com.github.romix.akka" %% "akka-kryo-serialization" % versions.akkaKryo exclude exclusions.akkaKryoExclude
  lazy val akkaRemote         = "com.typesafe.akka"     %% "akka-remote"             % versions.akka
  lazy val akkaSlf4j          = "com.typesafe.akka"     %% "akka-slf4j"              % versions.akka
  lazy val akkaStream         = "com.typesafe.akka"     %% "akka-stream"             % versions.akka
  lazy val akkaStreamTestkit  = "com.typesafe.akka"     %% "akka-stream-testkit"     % versions.akka % Test
  lazy val akkaHttpTestKit    = "com.typesafe.akka"     %% "akka-http-testkit"       % versions.akkaHttp % Test
  lazy val akkaClusterTestKit = "com.typesafe.akka"     %% "akka-multi-node-testkit" % versions.akka % Test
  lazy val akkaTestKit        = "com.typesafe.akka"     %% "akka-testkit"            % versions.akka % Test
  lazy val akka = Seq(
    akkaActor,
    akkaCluster,
    akkaClusterTools,
    akkaContrib,
    akkaRemote,
    akkaSlf4j,
    akkaKryo
  )
}

trait DarwinDependencies {
  val versions: Vanilla2Versions
  val exclusions: VanillaExclusions.type
  lazy val darwinCore           = "it.agilelab" %% "darwin-core"             % versions.darwin
  lazy val darwinHBaseConnector = "it.agilelab" %% "darwin-hbase2-connector" % versions.darwin
  lazy val darwinMockConnector  = "it.agilelab" %% "darwin-mock-connector"   % versions.darwin
  lazy val darwinConfluentConnector =
    ("it.agilelab" %% "darwin-confluent-connector" % versions.darwin)
      .exclude(exclusions.log4jExclude ++ exclusions.jacksonExclude)

  lazy val schemaRegistry = Seq(darwinCore)
}

trait HBaseDependencies {
  val versions: Vanilla2Versions
  val exclusions: VanillaExclusions.type
  lazy val hbaseClient2NoScope    = "org.apache.hbase" % "hbase-client" % versions.hbase2 exclude exclusions.hbaseExclusion
  lazy val hbaseCommon2NoScope    = "org.apache.hbase" % "hbase-common" % versions.hbase2 exclude exclusions.hbaseExclusion
  lazy val hbaseServer2NoScope    = "org.apache.hbase" % "hbase-server" % versions.hbase2 exclude exclusions.hbaseExclusion
  lazy val hbaseMapreduce2NoScope = "org.apache.hbase" % "hbase-mapreduce" % versions.hbase2 exclude exclusions.hbaseExclusion
  lazy val hbaseClient2Shaded     = "org.apache.hbase" % "hbase-shaded-client" % versions.hbase2 exclude exclusions.hbaseExclusion
  lazy val hbaseClient2           = hbaseClient2NoScope % Provided
  lazy val hbaseCommon2           = hbaseCommon2NoScope % Provided
  lazy val hbaseServer2           = hbaseServer2NoScope % Provided
  lazy val hbaseMapreduce2        = hbaseMapreduce2NoScope % Provided
  lazy val hbase2                 = Seq(hbaseClient2, hbaseCommon2, hbaseServer2, hbaseMapreduce2)
}

trait SparkDependencies extends HadoopDependencies {
  lazy val sparkCatalystTests = "org.apache.spark" %% "spark-catalyst" % versions.spark % Test classifier "tests"
  lazy val sparkCore          = "org.apache.spark" %% "spark-core" % versions.spark % Provided
  lazy val sparkTagsTests     = "org.apache.spark" %% "spark-tags" % versions.spark % Test classifier "tests"
  lazy val sparkMLlib         = "org.apache.spark" %% "spark-mllib" % versions.spark % Provided
  lazy val sparkSQL           = "org.apache.spark" %% "spark-sql" % versions.spark % Provided
  lazy val sparkYarn          = "org.apache.spark" %% "spark-yarn" % versions.spark % Provided
  lazy val sparkStreaming     = "org.apache.spark" %% "spark-streaming" % versions.spark % Provided
  lazy val sparkHive          = "org.apache.spark" %% "spark-hive" % versions.spark % Provided
  lazy val sparkCoreTests     = sparkCore classifier "tests"
  lazy val sparkSQLTests      = "org.apache.spark" %% "spark-sql" % versions.spark % "provided,test" classifier "tests"
  lazy val sparkAvro          = "org.apache.spark" %% "spark-avro" % versions.spark
  lazy val spark              = Seq(sparkMLlib, sparkYarn, sparkStreaming, hadoopCommon, sparkHive)
}

trait HadoopDependencies {
  val versions: Vanilla2Versions
  val exclusions: VanillaExclusions.type
  lazy val hadoopCommonNoScope = "org.apache.hadoop" % "hadoop-common" % versions.hadoop
  lazy val hadoopCommon        = hadoopCommonNoScope % Provided
  lazy val kms                 = "org.apache.hadoop" % "hadoop-kms" % versions.hadoop
  lazy val hadoopAWS           = "org.apache.hadoop" % "hadoop-aws" % versions.hadoop % Provided
}

trait LoggingDependencies {
  val versions: Vanilla2Versions
  val exclusions: VanillaExclusions.type
  lazy val slf4jApi           = "org.slf4j" % "slf4j-api" % versions.slf4j
  lazy val slf4jLog4j1Binding = "org.slf4j" % "slf4j-log4j12" % versions.slf4j
  lazy val log4j1             = "log4j" % "log4j" % versions.log4j1
  lazy val logging            = Seq(slf4jApi, slf4jLog4j1Binding % Test, log4j1 % Test)
}

trait KafkaDependencies {
  val versions: Vanilla2Versions
  val exclusions: VanillaExclusions.type
  lazy val kafka            = "org.apache.kafka" %% "kafka" % versions.kafka exclude (exclusions.kafkaExclusions ++ exclusions.jacksonExclude) // TODO remove jersey?
  lazy val kafkaClients     = "org.apache.kafka" % "kafka-clients" % versions.kafka exclude (exclusions.kafkaExclusions ++ exclusions.jacksonExclude) // TODO remove jersey?
  lazy val kafkaStreaming   = "org.apache.spark" %% "spark-streaming-kafka-0-8" % versions.spark exclude (exclusions.sparkExclusions ++ exclusions.kafka08Exclude) // TODO remove jersey?
  lazy val kafkaTests       = kafka              % Test exclude (exclusions.jacksonExclude)
  lazy val sparkSqlKafka    = "it.agilelab"      %% "wasp-spark-sql-kafka" % versions.sparkSqlKafka
  lazy val sparkSqlKafkaOld = "it.agilelab"      %% "wasp-spark-sql-kafka-old" % versions.sparkSqlKafka
}

trait MongoDependencies {
  val versions: Vanilla2Versions
  val exclusions: VanillaExclusions.type
  lazy val mongodbScala        = "org.mongodb.scala" %% "mongo-scala-driver"    % versions.mongodbScala
  lazy val mongoBsonScala      = "org.mongodb.scala" %% "mongo-scala-bson"      % versions.mongodbScala
  lazy val mongoSparkConnector = "org.mongodb.spark" %% "mongo-spark-connector" % versions.mongoSparkConnector exclude (exclusions.mongoJavaDriverExclude)
  lazy val mongoJavaDriver     = "org.mongodb"       % "mongo-java-driver"      % versions.mongoJavaDriver
}

trait CamelDependencies {
  val versions: Vanilla2Versions
  val exclusions: VanillaExclusions.type
  lazy val camelKafka     = "org.apache.camel" % "camel-kafka"     % versions.camel exclude (exclusions.kafkaExclusions ++ exclusions.camelKafkaExclusions)
  lazy val camelWebsocket = "org.apache.camel" % "camel-websocket" % versions.camel
}

trait Json4sDependencies {
  val versions: Vanilla2Versions
  val exclusions: VanillaExclusions.type
  lazy val json4sCore    = "org.json4s" %% "json4s-core" % versions.json4s % Provided exclude exclusions.jacksonExclude
  lazy val json4sJackson = "org.json4s" %% "json4s-jackson" % versions.json4s % Provided exclude exclusions.jacksonExclude
  lazy val json4sNative  = "org.json4s" %% "json4s-native" % versions.json4s exclude exclusions.jacksonExclude
  lazy val json          = Seq(json4sCore, json4sJackson, json4sNative)
}

trait NettyDependencies {
  val versions: Vanilla2Versions
  lazy val netty      = "io.netty" % "netty"     % versions.nettySpark    % Provided
  lazy val nettySpark = "io.netty" % "netty"     % versions.nettySpark    % Provided
  lazy val nettyAll   = "io.netty" % "netty-all" % versions.nettyAllSpark % Provided
}

trait TestFrameworkDependencies {
  val versions: Vanilla2Versions
  val exclusions: VanillaExclusions.type
  lazy val scalaTest  = "org.scalatest"  %% "scalatest"  % versions.scalaTest  % Test
  lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % versions.scalaCheck % Test
  lazy val wireMock: Seq[ModuleID] = Seq(
    "com.github.tomakehurst" % "wiremock-jre8" % versions.wireMock % Test,
    "xmlunit"                % "xmlunit"       % versions.xmlUnit  % Test
  ).map(_ exclude exclusions.jacksonExclude)

}

trait ScalaCoreDependencies {
  val versions: Vanilla2Versions
  lazy val typesafeConfig            = "com.typesafe"           % "config"                    % versions.typesafeConfig
  lazy val scalaParserAndCombinators = "org.scala-lang.modules" %% "scala-parser-combinators" % versions.scalaParserAndCombinators
  lazy val nameOf                    = "com.github.dwickern"    %% "scala-nameof"             % versions.nameOf
  lazy val scalaPool                 = "io.github.andrebeat"    %% "scala-pool"               % versions.scalaPool
  lazy val scalaCompiler             = "org.scala-lang"         % "scala-compiler"            % versions.scala
}

trait AvroDependencies {
  val versions: Vanilla2Versions
  val exclusions: VanillaExclusions.type
  lazy val avro       = "org.apache.avro"     % "avro"         % versions.avro % Provided
  lazy val avro4sCore = "com.sksamuel.avro4s" %% "avro4s-core" % versions.avro4sVersion exclude exclusions.json4sExclude
  lazy val avro4sJson = "com.sksamuel.avro4s" %% "avro4s-json" % versions.avro4sVersion exclude exclusions.json4sExclude

  lazy val avro4s     = Seq(avro4sCore, avro4sJson)
  lazy val avro4sTest = avro4s.map(_ % Test)
}

trait ApacheCommonsDependencies {
  val versions: Vanilla2Versions
  lazy val apacheCommonsLang3 = "org.apache.commons"        % "commons-lang3" % versions.apacheCommonsLang3Version // remove?
  lazy val commonsCli         = "commons-cli"               % "commons-cli"   % versions.commonsCli % Provided
  lazy val httpClient         = "org.apache.httpcomponents" % "httpclient"    % versions.httpcomponents
  lazy val httpCore           = "org.apache.httpcomponents" % "httpcore"      % versions.httpcomponents
  lazy val commonsIO          = "commons-io"                % "commons-io"    % versions.commonsIO
  lazy val dpcp2              = "org.apache.commons"        % "commons-dbcp2" % versions.dbcp2Version

}

trait SolrDependencies {
  val versions: Vanilla2Versions
  val exclusions: VanillaExclusions.type
  lazy val solrj = "org.apache.solr" % "solr-solrj" % versions.solr exclude exclusions.solrExclusion
  lazy val sparkSolr = versions.scala.take(4) match {
    case "2.11" =>
      ("it.agilelab.bigdata.spark" % "spark-solr" % versions.sparkSolr)
        .exclude(exclusions.sparkSolrExclusion)
    case "2.12" =>
      ("it.agilelab.bigdata.spark" %% "spark-solr" % versions.sparkSolr)
        .exclude(exclusions.sparkSolrExclusion)
  }
  lazy val solrjMasterClient = "org.apache.solr" % "solr-solrj" % versions.solr exclude exclusions.solrExclusion
}

trait SttpDependencies {
  val versions: Vanilla2Versions
  val exclusions: VanillaExclusions.type
  lazy val sttpCore   = "com.softwaremill.sttp.client" %% "core"   % versions.sttpVersion exclude exclusions.json4sExclude
  lazy val sttpJson4s = "com.softwaremill.sttp.client" %% "json4s" % versions.sttpVersion exclude exclusions.json4sExclude
}

trait CodehausJacksonDependencies {
  val versions: Vanilla2Versions

  lazy val codeHausJacksonCoreAsl   = "org.codehaus.jackson" % "jackson-core-asl"   % versions.codeHausJackson
  lazy val codeHausJacksonJaxRS     = "org.codehaus.jackson" % "jackson-jaxrs"      % versions.codeHausJackson
  lazy val codeHausJacksonMapperAsl = "org.codehaus.jackson" % "jackson-mapper-asl" % versions.codeHausJackson
}

trait OkHttpDependencies {
  val versions: Vanilla2Versions

  lazy val mockOkHttp2 = "com.squareup.okhttp" % "mockwebserver" % versions.okHttp % Test // in sync with cdh6
  lazy val okHttp2     = "com.squareup.okhttp" % "okhttp"        % versions.okHttp // in sync with cdh6
}

trait PostgresDependencies {
  val versions: Vanilla2Versions

  lazy val postgres           = "org.postgresql"           % "postgresql"      % versions.postgresqlVersion
  lazy val postgresqlEmbedded = "com.opentable.components" % "otj-pg-embedded" % versions.postgresqlEmbeddedVersion % Test

}
