trait Spark3Versions {
  val spark: String
  val delta: String
  val akka                      = "2.6.21"
  val akkaHttp                  = "10.2.10"
  val apacheCommonsLang3Version = "3.4"
  val avro                      = "1.8.2"
  val codahaleMetrics           = "3.0.2"
  val commonsCli                = "1.2"
  val darwin                    = "1.2.1"
  val guava                     = "14.0.1"
  val hbase2                    = "2.1.10"
  val httpcomponents            = "4.3.3"
  val httpcomponentsMime        = "4.3.1"
  val javaxMail                 = "1.4"
  val jdk                       = "8"
  val jetty                     = "9.3.20.v20170531"
  val jopt                      = "3.2"
  val json4s                    = "3.5.3"
  val kryo                      = "4.0.2"
  val log4j                     = "2.19.0"
  val parquet                   = "1.12.3"
  val quartz                    = "2.3.0"
  val scalaCheck                = "1.13.5"
  val scalaTest                 = "3.0.4"
  val scalaTestMockito          = "1.17.31"
  val slf4j                     = "2.0.6"
  val solr                      = "8.4.1" // solr8 client works also with solr 7 server
  val sparkSolr                 = "3.8.1" // solr8 client works also with solr 7 server
  val hadoop                    = "3.3.4" // 2.8.5 vs 3.3.3 (emr) vs 3.1.1 (cdp)
  val awsBundle                 = "1.11.375"
  val scalaParserAndCombinators = "1.0.4"
  val nifi                      = "1.11.4"
  val dbcp2Version              = "2.4.0"
  val postgresqlEmbeddedVersion = "2.1.0"
  val sttpVersion               = "2.1.2"
  val reflectionsVersion        = "0.9.11"
  val postgresqlVersion         = "42.2.5"
  val kafka_                    = "2.2.1"
  val kafka: String             = kafka_
  val yammerMetrics             = "2.2.0"
  val swagger                   = "2.1.2"
  val typesafeConfig            = "1.4.2"
  val velocity                  = "1.7"
  val mySqlConnector            = "5.1.6"
  val nameOf                    = "1.0.3"
  val scalaPool                 = "0.4.3"
  val commonsIO                 = "2.6"
  val okHttp                    = "2.7.5"
  val jakartaRsApi              = "2.1.5"
  val wireMock                  = "2.21.0"
  val xmlUnit                   = "1.6"
  val fasterxmlJackson          = "2.14.2"
  val codeHausJackson           = "1.9.13"
  val mongoSparkConnector       = "2.4.3"
  val mongoJavaDriver           = "3.12.2"
  val mongodbScala              = "2.9.0"
  val jettySecurity             = "9.3.25.v20180904"
  val akkaKryo                  = "1.1.5"
  val scala                     = "2.12.17"
  val elasticSearchSpark        = "7.15.0"
}

object Spark33Versions extends Spark3Versions {
  override val spark: String = "3.3.4"
  override val delta: String = "2.3.0"
}

object Spark34Versions extends Spark3Versions {
  override val spark: String = "3.4.4"
  override val delta: String = "2.4.0"
}

object Spark35Versions extends Spark3Versions {
  override val spark: String = "3.5.5"
  override val delta: String = "3.3.0"
}

object Spark35Emr770Versions extends Spark3Versions {
  override val spark: String = "3.5.5"
  override val delta: String = "3.3.0"
  override val json4s: String = "3.7.0-M11"
}
