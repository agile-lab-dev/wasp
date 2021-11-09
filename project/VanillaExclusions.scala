import sbt._
object VanillaExclusions {
  // I'm switching from .excludeAll to exclude API because excludeAll does not reflect in pom.xml therefore
  // when the artifacts are published, they're not excluded by dependant projects
  implicit class ModuleIdPower(val moduleID: ModuleID) extends AnyVal {
    def exclude(orgAndModule: Iterable[MavenCoordinate]): ModuleID = {
      orgAndModule.foldLeft(moduleID) {
        case (m, MavenCoordinate(org, mod)) =>
          m.exclude(org, mod)
      }
    }
  }

  case class MavenCoordinate(org: String, module: String) {
    def toExclusionRule = ExclusionRule(org, module)
  }

  // <something>Exclusion -> exclusions to be applied to <something>
  // <something>Exclude   -> artifacts of <something> that will be excluded

  // curated list of all dependencies under log4j organization
  lazy val log4jExclude: Vector[MavenCoordinate] = Vector(
    MavenCoordinate("log4j", "log4j"),
    MavenCoordinate("log4j", "apache-log4j-extras"),
    MavenCoordinate("log4j", "log4j-patched"),
    MavenCoordinate("org.apache.logging.log4j", "log4j-api"),
    MavenCoordinate("org.apache.logging.log4j", "log4j-core"),
    MavenCoordinate("org.apache.logging.log4j", "log4j-slf4j-impl"),
    MavenCoordinate("org.slf4j", "slf4j-log4j12")
  )

  lazy val akkaKryoExclude: Vector[MavenCoordinate] =
    Vector(MavenCoordinate("net.jpountz.lz4", "lz4"), MavenCoordinate("org.lz4", "lz4-java"))

  lazy val hbaseExclusion: Vector[MavenCoordinate] =
    log4jExclude ++ jacksonExclude ++ Vector(
      MavenCoordinate("io.netty", "netty"),
      MavenCoordinate("io.netty", "netty-all"),
      MavenCoordinate("com.google.guava", "guava"),
      MavenCoordinate("org.mortbay.jetty", "jetty-util"),
      MavenCoordinate("org.mortbay.jetty", "jetty"),
      MavenCoordinate("org.apache.zookeeper", "zookeeper")
    )

  lazy val solrExclusion: Vector[MavenCoordinate] =
    log4jExclude ++ sparkExclude ++ Vector(
      MavenCoordinate("org.objenesis", "objenesis"),
      MavenCoordinate("org.apache.zookeeper", "zookeeper"),
      MavenCoordinate("org.apache.hadoop", "hadoop-client")
    )

  lazy val sparkExclusions: Vector[MavenCoordinate] =
    log4jExclude ++ sparkExclude ++ Vector(
      MavenCoordinate("io.netty", "netty"),
      MavenCoordinate("com.google.guava", "guava"),
      MavenCoordinate("org.apache.spark", "spark-core"),
      MavenCoordinate("org.apache.kafka", "kafka-clients"),
      MavenCoordinate("org.slf4j", "slf4j-log4j12"),
      MavenCoordinate("org.apache.logging.log4j", "log4j-api"),
      MavenCoordinate("org.apache.logging.log4j", "log4j-core"),
      MavenCoordinate("org.apache.logging.log4j", "log4j-slf4j-impl"),
      MavenCoordinate("org.apache.solr", "solr-solrj"),
      MavenCoordinate("org.apache.solr", "solr-core"),
      MavenCoordinate("org.apache.solr", "solr-test-framework")
    )

  lazy val mongoJavaDriverExclude: Vector[MavenCoordinate] = Vector(
    MavenCoordinate("org.mongodb", "mongo-java-driver")
  )

  lazy val kafkaExclusions: Vector[MavenCoordinate] =
    slf4jExclude ++
      Vector(
        MavenCoordinate("com.sun.jmx", "jmxri"),
        MavenCoordinate("com.sun.jdmk", "jmxtools"),
        MavenCoordinate("net.sf.jopt-simple", "jopt-simple"),
        MavenCoordinate("net.jpountz.lz4", "lz4"),
        MavenCoordinate("org.lz4", "lz4-java")
      )

  lazy val kafka08Exclude: Vector[MavenCoordinate] =
    Vector(MavenCoordinate("org.apache.kafka", "kafka_2.11"), MavenCoordinate("org.apache.kafka", "kafka_2.12")) // todo chec

  lazy val hiveExclude = Vector(
    MavenCoordinate("org.apache.spark", "spark-core_2.11"),
    MavenCoordinate("org.apache.spark", "spark-core_2.12")
  )

  // these are needed because kafka brings in jackson-core/databind 2.8.5, which are incompatible with Spark
  // and otherwise cause a jackson compatibility exception
  lazy val jacksonExclude: Vector[MavenCoordinate] =
    Vector(
      MavenCoordinate("com.fasterxml.jackson.core", "jackson-core"),
      MavenCoordinate("com.fasterxml.jackson.core", "jackson-databind"),
      MavenCoordinate("com.fasterxml.jackson.core", "jackson-annotations"),
      MavenCoordinate("com.fasterxml.jackson.module", "jackson-module-scala_2.11"),
      MavenCoordinate("com.fasterxml.jackson.module", "jackson-module-scala_2.12"),
      MavenCoordinate("com.fasterxml.jackson.module", "jackson-module-paranamer"),
      MavenCoordinate("com.fasterxml.jackson.datatype", "jackson-datatype-jdk8"),
      MavenCoordinate("org.codehaus.jackson", "jackson-core-asl")
    )

  lazy val camelKafkaExclusions: Vector[MavenCoordinate] = Vector(MavenCoordinate("org.apache.kafka", "kafka-clients"))

  lazy val javaxRsExclude: Vector[MavenCoordinate] = Vector(
    MavenCoordinate("javax.ws.rs", "javax.ws.rs-api")
  )

  lazy val nettyExclude: Vector[MavenCoordinate] = Vector(
    MavenCoordinate("io.netty", "netty"),
    MavenCoordinate("io.netty", "netty-all")
  )

  lazy val hadoopExclude: Vector[MavenCoordinate] = Vector(
    MavenCoordinate("org.apache.hadoop", "hadoop-client"),
    MavenCoordinate("org.apache.hadoop", "hadoop-common")
  )

  lazy val kryoExclude =
    Vector(MavenCoordinate("com.esotericsoftware", "kryo-shaded"), MavenCoordinate("com.esotericsoftware", "minlog"))

  lazy val sparkSolrExclusion = sparkExclusions ++ solrExclusion ++ jacksonExclude ++ hadoopExclude ++ kryoExclude

  // curated list of all org.slf4j available artifacts, update when needed
  lazy val slf4jExclude = Vector(
    "integration",
    "jcl-over-slf4j",
    "jcl104-over-slf4j",
    "jul-to-slf4j",
    "log4j-over-slf4j",
    "nlog4j",
    "osgi-over-slf4j",
    "slf4j-android",
    "slf4j-api",
    "slf4j-archetype",
    "slf4j-converter",
    "slf4j-ext",
    "slf4j-jcl",
    "slf4j-jdk14",
    "slf4j-log4j12",
    "slf4j-log4j13",
    "slf4j-migrator",
    "slf4j-nop",
    "slf4j-parent",
    "slf4j-simple",
    "slf4j-site",
    "slf4j-skin",
    "taglib"
  ).map(MavenCoordinate("org.slf4j", _))

  // curated list of all org.apache.spark available artifacts for scala 2.11 and 2.12
  lazy val sparkExclude = Vector(
    "spark-avro_2.11",
    "spark-avro_2.12",
    "spark-bagel_2.11",
    "spark-catalyst_2.11",
    "spark-catalyst_2.12",
    "spark-core_2.11",
    "spark-core_2.12",
    "spark-cypher_2.12",
    "spark-docker-integration-tests_2.11",
    "spark-ganglia-lgpl_2.11",
    "spark-ganglia-lgpl_2.12",
    "spark-graph-api_2.12",
    "spark-graph_2.12",
    "spark-graphx_2.11",
    "spark-graphx_2.12",
    "spark-hive-thriftserver_2.11",
    "spark-hive-thriftserver_2.12",
    "spark-hive_2.11",
    "spark-hive_2.12",
    "spark-hivecontext-compatibility_2.11",
    "spark-kubernetes_2.11",
    "spark-kubernetes_2.12",
    "spark-kvstore_2.11",
    "spark-kvstore_2.12",
    "spark-launcher_2.11",
    "spark-launcher_2.12",
    "spark-mesos_2.11",
    "spark-mesos_2.12",
    "spark-mllib-local_2.11",
    "spark-mllib-local_2.12",
    "spark-mllib_2.11",
    "spark-mllib_2.12",
    "spark-network-common_2.11",
    "spark-network-common_2.12",
    "spark-network-shuffle_2.11",
    "spark-network-shuffle_2.12",
    "spark-network-yarn_2.11",
    "spark-network-yarn_2.12",
    "spark-pa",
    "spark-parent_2.11",
    "spark-parent_2.12",
    "spark-repl_2.11",
    "spark-repl_2.12",
    "spark-sketch_2.11",
    "spark-sketch_2.12",
    "spark-sql-kafka-0-10_2.11",
    "spark-sql-kafka-0-10_2.12",
    "spark-sql_2.11",
    "spark-sql_2.12",
    "spark-streaming-flume-assembly_2.11",
    "spark-streaming-flume-assembly_2.12",
    "spark-streaming-flume-sink_2.11",
    "spark-streaming-flume-sink_2.12",
    "spark-streaming-flume_2.11",
    "spark-streaming-flume_2.12",
    "spark-streaming-kafka-0-10-assembly_2.11",
    "spark-streaming-kafka-0-10-assembly_2.12",
    "spark-streaming-kafka-0-10_2.11",
    "spark-streaming-kafka-0-10_2.12",
    "spark-streaming-kafka-0-8-assembly_2.11",
    "spark-streaming-kafka-0-8_2.11",
    "spark-streaming-kafka-assembly_2.11",
    "spark-streaming-kafka_2.11",
    "spark-streaming-kinesis-asl-assembly_2.11",
    "spark-streaming-kinesis-asl_2.11",
    "spark-streaming-kinesis-asl_2.12",
    "spark-streaming-mqtt-assembly_2.11",
    "spark-streaming-mqtt_2.11",
    "spark-streaming-twitter_2.11",
    "spark-streaming-zeromq_2.11",
    "spark-streaming_2.11",
    "spark-streaming_2.12",
    "spark-tags_2.11",
    "spark-tags_2.12",
    "spark-test-tags_2.11",
    "spark-token-provider-kafka-0-10_2.12",
    "spark-unsafe_2.11",
    "spark-unsafe_2.12",
    "spark-yarn_2.11",
    "spark-yarn_2.12"
  ).map(MavenCoordinate("org.apache.spark", _))

  // curated list of all org.json4s available artifacts for scala 2.11 and 2.12
  lazy val json4sExclude: Vector[MavenCoordinate] = Vector(
    "json4s-ast_2.11",
    "json4s-ast_2.12",
    "json4s-ast_2.12.0-M3",
    "json4s-ast_2.12.0-M4",
    "json4s-ast_2.12.0-RC1",
    "json4s-ast_2.12.0-RC2",
    "json4s-ast_sjs0.6_2.11",
    "json4s-core_2.11",
    "json4s-core_2.12",
    "json4s-core_2.12.0-M3",
    "json4s-core_2.12.0-M4",
    "json4s-core_2.12.0-RC1",
    "json4s-core_2.12.0-RC2",
    "json4s-ext_2.11",
    "json4s-ext_2.12",
    "json4s-ext_2.12.0-M3",
    "json4s-ext_2.12.0-M4",
    "json4s-ext_2.12.0-RC1",
    "json4s-ext_2.12.0-RC2",
    "json4s-jackson_2.11",
    "json4s-jackson_2.12",
    "json4s-jackson_2.12.0-M3",
    "json4s-jackson_2.12.0-M4",
    "json4s-jackson_2.12.0-RC1",
    "json4s-jackson_2.12.0-RC2",
    "json4s-mongo_2.11",
    "json4s-mongo_2.12",
    "json4s-mongo_2.12.0-M3",
    "json4s-mongo_2.12.0-M4",
    "json4s-mongo_2.12.0-RC1",
    "json4s-mongo_2.12.0-RC2",
    "json4s-native_2.11",
    "json4s-native_2.12",
    "json4s-native_2.12.0-M3",
    "json4s-native_2.12.0-M4",
    "json4s-native_2.12.0-RC1",
    "json4s-native_2.12.0-RC2",
    "json4s-scalap_2.11",
    "json4s-scalap_2.12",
    "json4s-scalap_2.12.0-M3",
    "json4s-scalap_2.12.0-M4",
    "json4s-scalap_2.12.0-RC1",
    "json4s-scalap_2.12.0-RC2",
    "json4s-scalaz_2.11",
    "json4s-scalaz_2.12",
    "json4s-scalaz_2.12.0-M3",
    "json4s-scalaz_2.12.0-M4",
    "json4s-scalaz_2.12.0-RC1",
    "json4s-scalaz_2.12.0-RC2",
    "json4s-tests_2.11",
    "json4s-xml_2.11",
    "json4s-xml_2.12",
    "json4s_2.11",
    "muster-codec-argonaut_2.11",
    "muster-codec-jackson_2.11",
    "muster-codec-jawn_2.11",
    "muster-codec-json4s_2.11",
    "muster-codec-json_2.11",
    "muster-codec-play-json_2.11",
    "muster-codec-string_2.11",
    "muster-core_2.11"
  ).map(MavenCoordinate("org.json4s", _))

}
