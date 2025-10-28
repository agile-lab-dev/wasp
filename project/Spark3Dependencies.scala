import com.typesafe.sbt.packager.Keys.scriptClasspath
import sbt._

object Spark33Dependencies extends Spark3Dependencies(Spark33Versions) {
  val delta = "io.delta" %% "delta-core" % versions.delta exclude exclusions.log4jExclude

}
object Spark34Dependencies extends Spark3Dependencies(Spark34Versions) {
  val delta = "io.delta" %% "delta-core" % versions.delta exclude exclusions.log4jExclude

}
object Spark35Dependencies extends Spark3Dependencies(Spark35Versions) {
  val delta = "io.delta" %% "delta-spark" % versions.delta exclude exclusions.log4jExclude

}
object Spark35Emr770Dependencies extends Spark3Dependencies(Spark35Emr770Versions) {
  val delta = "io.delta" %% "delta-spark" % versions.delta exclude exclusions.log4jExclude

}

abstract class Spark3Dependencies(val versions: Spark3Versions)
    extends Dependencies
    with Spark3AkkaDependencies
    with Spark3DarwinDependencies
    with Spark3HBaseDependencies
    with Spark3SparkDependencies
    with Spark3LoggingDependencies
    with Spark3KafkaDependencies
    with Spark3MongoDependencies
    with Spark3Json4sDependencies
    with Spark3TestFrameworkDependencies
    with Spark3ScalaCoreDependencies
    with Spark3AvroDependencies
    with Spark3ApacheCommonsDependencies
    with Spark3SolrDependencies
    with Spark3CodehausJacksonDependencies
    with Spark3OkHttpDependencies
    with Spark3PostgresDependencies
    with Spark3SttpDependencies {
  val delta: ModuleID
  val exclusions: Spark3Exclusions.type = Spark3Exclusions
  val removeShims: Seq[ExclusionRule] = Seq(
    ExclusionRule("org.spark-project.hive")
  )

  lazy val parquet = "org.apache.parquet" % "parquet-column" % "1.12.3" exclude exclusions.log4jExclude
  lazy val elasticSearchSpark = "org.elasticsearch"   %% "elasticsearch-spark-20" % versions.elasticSearchSpark
  lazy val guava              = "com.google.guava"     % "guava"                  % versions.guava
  lazy val javaxMail          = "javax.mail"           % "mail"                   % versions.javaxMail
  lazy val metrics            = "com.codahale.metrics" % "metrics-core"           % versions.codahaleMetrics
  lazy val quartz             = "org.quartz-scheduler" % "quartz"                 % versions.quartz
  lazy val swaggerCore        = "io.swagger.core.v3"   % "swagger-core"           % versions.swagger
  lazy val velocity           = "org.apache.velocity"  % "velocity"               % versions.velocity
  lazy val kryo               = "com.esotericsoftware" % "kryo-shaded"            % versions.kryo
  lazy val reflections        = "org.reflections"      % "reflections"            % versions.reflectionsVersion
  lazy val mySqlJavaConnector = "mysql"                % "mysql-connector-java"   % versions.mySqlConnector
  lazy val jaxRs              = "jakarta.ws.rs"        % "jakarta.ws.rs-api"      % versions.jakartaRsApi
  lazy val nifiStateless =
    "org.apache.nifi" % "nifi-stateless" % versions.nifi % Provided exclude exclusions.javaxRsExclude
  lazy val joptSimpleTests = "net.sf.jopt-simple"  % "jopt-simple"               % versions.jopt % Test
  lazy val mongoTest       = "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "3.5.4"       % Test
  lazy val shapeless       = "com.chuusai"        %% "shapeless"                 % "2.3.3"
  lazy val hadoopClientApi = "org.apache.hadoop"   % "hadoop-client-api"         % versions.hadoop

  val jacksonTestDependencies = Seq(
    "com.fasterxml.jackson.core"     % "jackson-annotations"             % "2.10.1" % Test force (),
    "com.fasterxml.jackson.core"     % "jackson-core"                    % "2.10.1" % Test force (),
    "com.fasterxml.jackson.core"     % "jackson-databind"                % "2.10.1" % Test force (),
    "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8"           % "2.10.1" % Test force (),
    "com.fasterxml.jackson.jaxrs"    % "jackson-jaxrs-base"              % "2.10.1" % Test force (),
    "com.fasterxml.jackson.jaxrs"    % "jackson-jaxrs-json-provider"     % "2.10.1" % Test force (),
    "com.fasterxml.jackson.module"   % "jackson-module-jaxb-annotations" % "2.10.1" % Test force (),
    "com.fasterxml.jackson.module"   % "jackson-module-paranamer"        % "2.10.1" % Test force (),
    "com.fasterxml.jackson.module"  %% "jackson-module-scala"            % "2.10.1" % Test force ()
  )

  lazy val _pluginKafkaSparkDependencies: Seq[ModuleID] = spark ++ Seq(
    guava,
    kafkaClients,
    scalaTest
  )

  override val scalaTestDependencies: Seq[ModuleID] = Seq(scalaTest, mongoTest)

  override val testDependencies: Seq[ModuleID] = Seq(akkaTestKit, akkaClusterTestKit, scalaTest, mongoTest)

  override val modelDependencies: Seq[ModuleID] = (json ++ Seq(
    typesafeConfig,
    akkaStream,
    akkaHttpSpray,
    sparkSQL,
    mongoBsonScala
  )).map(_.exclude(exclusions.log4jExclude)) ++ scalaTestDependencies

  override val coreDependencies: Seq[ModuleID] = (akka ++
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
    ) ++ spark)

  override val repositoryMongoDependencies: Seq[ModuleID] = Seq(
    mongodbScala,
    nameOf,
    sparkSQL,
    shapeless
  ) ++ scalaTestDependencies

  override val repositoryPostgresDependencies: Seq[ModuleID] = Seq(
    postgres,
    dpcp2,
    postgresqlEmbedded,
    postgresqlEmbeddedArm64,
    postgresqlEmbeddedArm64Linux,
    sparkSQL
  ) ++ scalaTestDependencies

  override val scalaCompilerDependencies: Seq[ModuleID] = (testDependencies ++ Seq(scalaCompiler, scalaPool))
    .map(_.exclude(exclusions.log4jExclude))

  override val producersDependencies: Seq[ModuleID] = (
    akka ++ testDependencies ++ Seq(commonsIO, akkaHttp, akkaStream, commonsCli)
  ).map(_.exclude(exclusions.log4jExclude))

  override val consumersSparkDependencies: Seq[ModuleID] = schemaRegistry ++ (
    akka ++
      testDependencies ++
      hbase2 ++ // maybe remove this, we need to refactor the gdpr part for hbase
      wireMock ++
      spark ++
      Seq(
        quartz,
        nameOf,
        velocity, // TODO: evaluate this is legal
        scalaCompiler,
        sparkAvro,
        darwinMockConnector % Test
      )
  )

  override val masterDependencies: Seq[ModuleID] = (
    json ++
      akka ++
      Seq(
        sparkSQL,
        akkaHttp,
        akkaHttpSpray,
        commonsCli,
        scalaTest,
        akkaHttpTestKit,
        akkaStreamTestkit,
        solrjMasterClient,
        httpClient
      )
  )

  override val pluginElasticSparkDependencies: Seq[ModuleID] = spark ++ Seq(elasticSearchSpark)

  override val pluginHttpSparkDependencies: Seq[ModuleID] =
    spark ++ Seq(okHttp2, mockOkHttp2, scalaTest).map(_.exclude(exclusions.hiveExclude))

  // here we need to create 2 plugins instead

  override val pluginHbaseSparkDependencies: Seq[ModuleID] =
    (spark ++ hbase2 ++ Seq(scalaTest))

  override val pluginPlainHbaseWriterSparkDependencies: Seq[ModuleID] =
    (spark ++
      hbase2 ++
      jacksonTestDependencies ++
      Seq(scalaTest, scalaTestMockito, hbaseTestingUtils))

  override val pluginPostgreSQLSparkDependencies = Seq(
    sparkSQL,
    sparkHive,
    sparkStreaming,
    postgres,
    dpcp2,
    postgresqlEmbedded,
    postgresqlEmbeddedArm64,
    postgresqlEmbeddedArm64Linux,
    scalaTest
  )

  override val pluginKafkaSparkDependencies: Seq[ModuleID] =
    (Seq(sparkSqlKafka) ++ _pluginKafkaSparkDependencies) ++ logging

  override val pluginSolrSparkDependencies: Seq[ModuleID] = spark ++ Seq(
    httpClient,
    httpCore,
    solrj,
    sparkSolr
  )

  override val pluginMongoSparkDependencies: Seq[ModuleID] = spark ++ Seq(
    mongoSparkConnector,
    mongoJavaDriver
  )

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

  // it's lazy because it depends on delta which is initialized by a subclass
  override lazy val pluginCdcSparkDependencies: Seq[ModuleID] = spark ++ Seq(delta, scalaTest)

  override val awsAuth: Seq[ModuleID] = Seq(
    "org.apache.hadoop" % "hadoop-aws"          % versions.hadoop,
    "org.apache.hadoop" % "hadoop-common"       % versions.hadoop,
    "com.amazonaws"     % "aws-java-sdk-bundle" % versions.awsBundle force ()
  )

  // it's lazy because it depends on delta which is initialized by a subclass
  override lazy val pluginParallelWriteSparkDependencies: Seq[ModuleID] =
    Seq(scalaTest) ++ pluginHttpSparkDependencies ++ Seq(
      /*
       Hive-exec shades a lot of things we need to take
      care of overriding the classpath by prepending libraries
      that are shaded by hive-exec, notable examples are guava and
      commons lang, actual implementation of hive-exec
      on an EMR cluster do the right thing because they are patched
      by aws with proper support for hadoop3
       */
      apacheCommonsLang3,
      guava % Provided,
      delta,
      "org.apache.hive" % "hive-exec"      % "2.3.9" % Provided classifier "core",
      "org.apache.hive" % "hive-metastore" % "2.3.9" % Provided,
      parquet
    ).map(_ exclude exclusions.hiveExclude) ++ logging

  override val microserviceCatalogDependencies: Seq[ModuleID] =
    Seq(scalaTest) ++ pluginHttpSparkDependencies

  override val sparkTelemetryPluginDependencies: Seq[ModuleID] =
    Seq(sparkCore, kafkaClients, scalaParserAndCombinators)

  override val sparkNifiPluginDependencies: Seq[ModuleID] = spark

  override val repositoryCoreDependencies: Seq[ModuleID] = testDependencies ++ Seq(apacheCommonsLang3, shapeless)

  override val sparkPluginBasicDependencies: Seq[ModuleID] = spark ++ scalaTestDependencies

  override val whitelabelModelsDependencies: Seq[ModuleID] = spark

  override val whitelabelMasterDependencies: Seq[ModuleID] =
    pluginHbaseSparkDependencies ++ Seq(darwinHBaseConnector, hbaseClient2Shaded)

  override val whitelabelProducerDependencies: Seq[ModuleID] =
    pluginHbaseSparkDependencies ++ Seq(darwinHBaseConnector, hbaseClient2Shaded)

  override val whitelabelSparkConsumerDependencies: Seq[ModuleID] = Seq(
    darwinHBaseConnector,
    mySqlJavaConnector,
    scalaTest,
    hadoopAuth          % Test,
    darwinMockConnector % Test
  ) ++ spark ++ Seq(hbaseClient2Shaded)

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

  override val whiteLabelSingleNodeScriptClasspath =
    scriptClasspath := Seq(":$SPARK_HOME/jars/*") ++
      scriptClasspath.value ++
      Seq(":$HADOOP_CONF_DIR:$YARN_CONF_DIR:/$HBASE_CONF_DIR")
}

trait Spark3AkkaDependencies {
  val versions: Spark3Versions
  val exclusions: Spark3Exclusions.type
  lazy val akkaActor          = "com.typesafe.akka" %% "akka-actor"           % versions.akka
  lazy val akkaCluster        = "com.typesafe.akka" %% "akka-cluster"         % versions.akka
  lazy val akkaClusterMetrics = "com.typesafe.akka" %% "akka-cluster-metrics" % versions.akka
  lazy val akkaClusterTools   = "com.typesafe.akka" %% "akka-cluster-tools"   % versions.akka
  lazy val akkaHttp           = "com.typesafe.akka" %% "akka-http"            % versions.akkaHttp
  lazy val akkaHttpSpray      = "com.typesafe.akka" %% "akka-http-spray-json" % versions.akkaHttp
  lazy val akkaKryo = "io.altoo" %% "akka-kryo-serialization" % versions.akkaKryo exclude exclusions.akkaKryoExclude
  lazy val akkaRemote         = "com.typesafe.akka" %% "akka-remote"             % versions.akka
  lazy val akkaSlf4j          = "com.typesafe.akka" %% "akka-slf4j"              % versions.akka
  lazy val akkaStream         = "com.typesafe.akka" %% "akka-stream"             % versions.akka
  lazy val akkaStreamTestkit  = "com.typesafe.akka" %% "akka-stream-testkit"     % versions.akka     % Test
  lazy val akkaHttpTestKit    = "com.typesafe.akka" %% "akka-http-testkit"       % versions.akkaHttp % Test
  lazy val akkaClusterTestKit = "com.typesafe.akka" %% "akka-multi-node-testkit" % versions.akka     % Test
  lazy val akkaTestKit        = "com.typesafe.akka" %% "akka-testkit"            % versions.akka     % Test
  lazy val akka = Seq(
    akkaActor,
    akkaCluster,
    akkaClusterTools,
    akkaClusterMetrics,
    akkaRemote,
    akkaSlf4j,
    akkaKryo
  )
}

trait Spark3DarwinDependencies {
  val versions: Spark3Versions
  val exclusions: Spark3Exclusions.type
  lazy val darwinCore           = "it.agilelab" %% "darwin-core"             % versions.darwin
  lazy val darwinHBaseConnector = "it.agilelab" %% "darwin-hbase2-connector" % versions.darwin
  lazy val darwinMockConnector  = "it.agilelab" %% "darwin-mock-connector"   % versions.darwin
  lazy val darwinConfluentConnector =
    ("it.agilelab" %% "darwin-confluent-connector" % versions.darwin)
      .exclude(exclusions.log4jExclude ++ exclusions.jacksonExclude)

  lazy val schemaRegistry = Seq(darwinCore)
}

trait Spark3HBaseDependencies {
  val versions: Spark3Versions
  val exclusions: Spark3Exclusions.type
  lazy val hbaseClient2NoScope = "org.apache.hbase" % "hbase-client" % versions.hbase2 exclude exclusions.hbaseExclusion
  lazy val hbaseCommon2NoScope = "org.apache.hbase" % "hbase-common" % versions.hbase2 exclude exclusions.hbaseExclusion
  lazy val hbaseServer2NoScope = "org.apache.hbase" % "hbase-server" % versions.hbase2 exclude exclusions.hbaseExclusion
  lazy val hbaseMapreduce2NoScope =
    "org.apache.hbase" % "hbase-mapreduce" % versions.hbase2 exclude exclusions.hbaseExclusion
  lazy val hbaseClient2Shaded =
    "org.apache.hbase" % "hbase-shaded-client" % versions.hbase2 exclude exclusions.hbaseExclusion
  lazy val hbaseTestingUtils = "org.apache.hbase"     % "hbase-testing-util" % versions.hbase2
  lazy val hbaseClient2      = hbaseClient2NoScope    % Provided
  lazy val hbaseCommon2      = hbaseCommon2NoScope    % Provided
  lazy val hbaseServer2      = hbaseServer2NoScope    % Provided
  lazy val hbaseMapreduce2   = hbaseMapreduce2NoScope % Provided
  lazy val hbase2            = Seq(hbaseClient2, hbaseCommon2, hbaseServer2, hbaseMapreduce2)
}

trait Spark3SparkDependencies extends Spark3HadoopDependencies {
  lazy val sparkCatalystTests = "org.apache.spark" %% "spark-catalyst"  % versions.spark % Test classifier "tests"
  lazy val sparkCore          = "org.apache.spark" %% "spark-core"      % versions.spark % Provided
  lazy val sparkTagsTests     = "org.apache.spark" %% "spark-tags"      % versions.spark % Test classifier "tests"
  lazy val sparkMLlib         = "org.apache.spark" %% "spark-mllib"     % versions.spark % Provided
  lazy val sparkSQL           = "org.apache.spark" %% "spark-sql"       % versions.spark % Provided
  lazy val sparkYarn          = "org.apache.spark" %% "spark-yarn"      % versions.spark % Provided
  lazy val sparkStreaming     = "org.apache.spark" %% "spark-streaming" % versions.spark % Provided
  lazy val sparkHive          = "org.apache.spark" %% "spark-hive"      % versions.spark % Provided
  lazy val sparkCoreTests     = sparkCore classifier "tests"
  lazy val sparkSQLTests = "org.apache.spark" %% "spark-sql"   % versions.spark % "provided,test" classifier "tests"
  lazy val sparkAvro     = "org.apache.spark" %% "spark-avro"  % versions.spark
  lazy val spark         = Seq(sparkMLlib, sparkYarn, hadoopCommon, sparkHive)
  lazy val hadoopAuth    = "org.apache.hadoop" % "hadoop-auth" % versions.hadoop
}

trait Spark3HadoopDependencies {
  val versions: Spark3Versions
  val exclusions: Spark3Exclusions.type
  lazy val hadoopCommonNoScope = "org.apache.hadoop" % "hadoop-common" % versions.hadoop
  lazy val hadoopCommon        = hadoopCommonNoScope % Provided
  lazy val hadoopAWS           = "org.apache.hadoop" % "hadoop-aws"    % versions.hadoop % Provided
}

trait Spark3LoggingDependencies {
  val versions: Spark3Versions
  val exclusions: Spark3Exclusions.type

  lazy val slf4jApi           = "org.slf4j"                % "slf4j-api"         % versions.slf4j % Provided
  lazy val slf4jLog4j2Binding = "org.apache.logging.log4j" % "log4j-slf4j2-impl" % versions.log4j % Provided
  lazy val log4j2Api          = "org.apache.logging.log4j" % "log4j-api"         % versions.log4j % Provided
  lazy val log4jCore          = "org.apache.logging.log4j" % "log4j-core"        % versions.log4j % Provided
  lazy val log4j1Api          = "org.apache.logging.log4j" % "log4j-1.2-api"     % versions.log4j % Provided
  val logging                 = Seq(slf4jApi, slf4jLog4j2Binding, log4j2Api, log4jCore, log4j1Api)
}

trait Spark3KafkaDependencies {
  val versions: Spark3Versions
  val exclusions: Spark3Exclusions.type
  lazy val kafka =
    "org.apache.kafka" %% "kafka" % versions.kafka exclude (exclusions.kafkaExclusions ++ exclusions.jacksonExclude) // TODO remove jersey?
  lazy val kafkaClients =
    "org.apache.kafka" % "kafka-clients" % versions.kafka exclude (exclusions.kafkaExclusions ++ exclusions.jacksonExclude) // TODO remove jersey?
  lazy val kafkaTests    = kafka               % Test exclude (exclusions.jacksonExclude)
  lazy val sparkSqlKafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % versions.spark
}

trait Spark3MongoDependencies {
  val versions: Spark3Versions
  val exclusions: Spark3Exclusions.type
  lazy val mongodbScala   = "org.mongodb.scala" %% "mongo-scala-driver" % versions.mongodbScala
  lazy val mongoBsonScala = "org.mongodb.scala" %% "mongo-scala-bson"   % versions.mongodbScala
  lazy val mongoSparkConnector =
    "org.mongodb.spark" %% "mongo-spark-connector" % versions.mongoSparkConnector exclude (exclusions.mongoJavaDriverExclude)
  lazy val mongoJavaDriver = "org.mongodb" % "mongo-java-driver" % versions.mongoJavaDriver
}

trait Spark3Json4sDependencies {
  val versions: Spark3Versions
  val exclusions: Spark3Exclusions.type
  lazy val json4sCore = "org.json4s" %% "json4s-core" % versions.json4s % Provided exclude exclusions.jacksonExclude
  lazy val json4sJackson =
    "org.json4s" %% "json4s-jackson" % versions.json4s % Provided exclude exclusions.jacksonExclude
  lazy val json4sNative = "org.json4s" %% "json4s-native" % versions.json4s exclude exclusions.jacksonExclude
  lazy val json         = Seq(json4sCore, json4sJackson, json4sNative)
}

trait Spark3TestFrameworkDependencies {
  val versions: Spark3Versions
  val exclusions: Spark3Exclusions.type
  lazy val scalaTest        = "org.scalatest"  %% "scalatest"     % versions.scalaTest        % Test
  lazy val scalaCheck       = "org.scalacheck" %% "scalacheck"    % versions.scalaCheck       % Test
  lazy val scalaTestMockito = "org.mockito"    %% "mockito-scala" % versions.scalaTestMockito % Test
  lazy val wireMock: Seq[ModuleID] = Seq(
    "com.github.tomakehurst" % "wiremock-jre8" % versions.wireMock % Test,
    "xmlunit"                % "xmlunit"       % versions.xmlUnit  % Test
  ).map(_ exclude exclusions.jacksonExclude)
}

trait Spark3ScalaCoreDependencies {
  val versions: Spark3Versions
  lazy val typesafeConfig = "com.typesafe" % "config" % versions.typesafeConfig
  lazy val scalaParserAndCombinators =
    "org.scala-lang.modules" %% "scala-parser-combinators" % versions.scalaParserAndCombinators
  lazy val nameOf        = "com.github.dwickern" %% "scala-nameof"   % versions.nameOf
  lazy val scalaPool     = "io.github.andrebeat" %% "scala-pool"     % versions.scalaPool
  lazy val scalaCompiler = "org.scala-lang"       % "scala-compiler" % versions.scala
}

trait Spark3AvroDependencies {
  val versions: Spark3Versions
  val exclusions: Spark3Exclusions.type
  lazy val avro = "org.apache.avro" % "avro" % versions.avro % Provided
}

trait Spark3ApacheCommonsDependencies {
  val versions: Spark3Versions
  lazy val apacheCommonsLang3 = "org.apache.commons" % "commons-lang3" % versions.apacheCommonsLang3Version // remove?
  lazy val commonsCli         = "commons-cli"        % "commons-cli"   % versions.commonsCli % Provided
  lazy val httpClient = "org.apache.httpcomponents" % "httpclient"    % versions.httpcomponents
  lazy val httpCore   = "org.apache.httpcomponents" % "httpcore"      % versions.httpcomponents
  lazy val commonsIO  = "commons-io"                % "commons-io"    % versions.commonsIO
  lazy val dpcp2      = "org.apache.commons"        % "commons-dbcp2" % versions.dbcp2Version

}

trait Spark3SolrDependencies {
  val versions: Spark3Versions
  val exclusions: Spark3Exclusions.type
  lazy val solrj = "org.apache.solr" % "solr-solrj" % versions.solr exclude exclusions.solrExclusion
  lazy val sparkSolr =
    "it.agilelab.bigdata.spark" %% "spark-solr" % versions.sparkSolr exclude exclusions.sparkSolrExclusion
  lazy val solrjMasterClient = "org.apache.solr" % "solr-solrj" % versions.solr exclude exclusions.solrExclusion
}

trait Spark3SttpDependencies {
  val versions: Spark3Versions
  val exclusions: Spark3Exclusions.type
  lazy val sttpCore = "com.softwaremill.sttp.client" %% "core" % versions.sttpVersion exclude exclusions.json4sExclude
  lazy val sttpJson4s =
    "com.softwaremill.sttp.client" %% "json4s" % versions.sttpVersion exclude exclusions.json4sExclude
}

trait Spark3CodehausJacksonDependencies {
  val versions: Spark3Versions
  lazy val jacksonDatabind          = "com.fasterxml.jackson.core" % "jackson-databind"   % versions.fasterxmlJackson
  lazy val jacksonCore              = "com.fasterxml.jackson.core" % "jackson-core"       % versions.fasterxmlJackson
  lazy val codeHausJacksonMapperAsl = "org.codehaus.jackson"       % "jackson-mapper-asl" % versions.codeHausJackson
  lazy val codeHausJacksonCoreAsl   = "org.codehaus.jackson"       % "jackson-core-asl"   % versions.codeHausJackson
}

trait Spark3OkHttpDependencies {
  val versions: Spark3Versions

  lazy val mockOkHttp2 = "com.squareup.okhttp" % "mockwebserver" % versions.okHttp % Test // in sync with cdh6
  lazy val okHttp2     = "com.squareup.okhttp" % "okhttp"        % versions.okHttp // in sync with cdh6
}

trait Spark3PostgresDependencies {
  val versions: Spark3Versions

  lazy val postgres           = "org.postgresql" % "postgresql"        % versions.postgresqlVersion
  lazy val postgresqlEmbedded = "io.zonky.test"  % "embedded-postgres" % versions.postgresqlEmbeddedVersion % Test
  lazy val postgresqlEmbeddedArm64 =
    "io.zonky.test.postgres" % "embedded-postgres-binaries-darwin-arm64v8" % "17.2.0" % Test
  lazy val postgresqlEmbeddedArm64Linux =
    "io.zonky.test.postgres" % "embedded-postgres-binaries-linux-arm64v8" % "17.2.0" % Test

}
