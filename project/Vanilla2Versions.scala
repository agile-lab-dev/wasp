class Vanilla2Versions {
  val akka                      = "2.4.20" // do not use akka 2.5+ until spark has removed their dependency on akka 2.3, otherwise master & consumer won't be able to communicate
  val akkaHttp                  = "10.0.9" // keep in sync with akka
  val akkaKryo                  = "0.5.0"
  val apacheCommonsLang3Version = "3.4"
  val avro                      = "1.8.2"
  val avro4sVersion             = "1.8.3"
  val commonsCli                = "1.2"
  val darwin                    = "1.2.1"
  val elasticSearchSpark        = "6.1.2"
  val guava                     = "14.0.1"
  val hbase2                    = "2.1.10"
  val httpcomponents            = "4.3.3"
  val httpcomponentsMime        = "4.3.1"
  val javaxMail                 = "1.4"
  val jdk                       = "1.8"
  val jetty                     = "9.3.20.v20170531"
  val jopt                      = "3.2"
  val json4s                    = "3.5.3"
  val kryo                      = "4.0.2"
  val log4j                     = "2.17.0" // keep compatible with elastic
  val log4j1                    = "1.2.17"
  val nettySpark                = "3.9.9.Final"
  val nettyAllSpark             = "4.1.47.Final"
  val quartz                    = "2.3.0"
  val scala                     = "2.11.12"
  val scalaCheck                = "1.13.5"
  val scalaTest                 = "3.0.4"
  val slf4j                     = "1.7.12"
  val solr                      = "8.4.1" // solr8 client works also with solr 7 server
  val spark                     = "2.4.8"
  val sparkSolr                 = "3.8.1" // solr8 client works also with solr 7 server
  val hadoop                    = "2.8.5"
  val awsBundle                 = "1.11.375"
  val scalaParserAndCombinators = "1.0.4"
  val nifi                      = "1.11.4"
  val dbcp2Version              = "2.4.0"
  val postgresqlEmbeddedVersion = "0.13.1"
  val sttpVersion               = "2.1.2"
  val reflectionsVersion        = "0.9.11"
  val postgresqlVersion         = "42.2.5"
  val delta                     = "0.6.1" // + "-" + spark
  val kafka_                    = "2.2.1"
  val kafka: String             = kafka_
  val sparkSqlKafka: String     = "0.1.0" + "-" + kafka_ + "-" + spark
  val yammerMetrics             = "2.2.0"
  val swagger                   = "2.1.2"
  val typesafeConfig            = "1.3.0"
  val velocity                  = "1.7"
  val mySqlConnector            = "5.1.6"
  val nameOf                    = "1.0.3"
  val scalaPool                 = "0.4.3"
  val commonsIO                 = "2.6"
  val okHttp                    = "2.7.5"
  val jakartaRsApi              = "2.1.5"
  val wireMock                  = "2.21.0"
  val xmlUnit                   = "1.6"
  val codeHausJackson           = "1.9.13"
  val mongoSparkConnector       = "2.4.3"
  val mongoJavaDriver           = "3.12.2"
  val mongodbScala              = "2.9.0"
  val jettySecurity             = "9.3.25.v20180904"
}

class Vanilla2_2_12Versions extends Vanilla2Versions {
  override val akkaKryo           = "0.5.2"
  override val scala              = "2.12.10"
  override val elasticSearchSpark = "7.15.0"
}
