/**
 * Versions definitions. Keep in alphabetical order.
 */
class Cdh6Versions {
  val akka = "2.4.20" // do not use akka 2.5+ until spark has removed their dependency on akka 2.3, otherwise master & consumer won't be able to communicate
  val akkaHttp = "10.0.9" // keep in sync with akka
  val apacheCommonsLang3Version = "3.4"
  val avro = "1.8.2"
  val avro4sVersion = "1.8.3"
  val avroSpark = "3.2.0"
  val camel = "2.17.7"
  val cdh6 = "cdh6.3.2"
  val cdk = s"2.2.1-$cdh6" // Cloudera Distribution of Kafka
  val cds = "cloudera2" // Cloudera Distribution of Spark
  val commonsCli = "1.4"
  val darwin = "1.2.1"
  val elasticSearch = "6.1.2"
  val elasticSearchSpark = "6.1.2"
  val guava = "14.0.1"
  val hbase = s"2.1.0-$cdh6"
  val httpcomponents = "4.3.3"
  val jdk = "1.8"
  val jetty = "9.3.20.v20170531"
  val jopt = "3.2"
  val json4s = "3.5.3"
  val kryo = "3.0.0"
  val log4j = "2.9.1" // keep compatible with elastic
  val log4j1 = "1.2.16"
  val mongodbScala = "2.9.0"
  val netty = "3.10.6.Final"
  val nettySpark = "3.9.9.Final"
  val nettyAllSpark = "4.1.17.Final"
  val nifi = "1.11.4"
  val quartz = "2.3.0"
  val scala = "2.11.12"
  val scalaCheck = "1.13.5"
  val scalaTest = "3.0.4"
  val scalaTest2 = "2.2.6"
  val slf4j = "1.7.12"
  val solr = "7.4.0.7.0.3.0-79"
  val spark_ = s"2.4.0"
  val spark = s"${spark_}-${cdh6}"
  val sparkSolr = "3.8.1"
  val kms = s"3.0.0-$cdh6"
  val scalaParserAndCombinators = "1.0.4"
  val sttpVersion = "2.1.2"
  val reflectionsVersion = "0.9.11"
  val postgresqlVersion = "42.2.5"
  val dbcp2Version = "2.4.0"
  val postgresqlEmbeddedVersion ="0.13.1"
  val delta = "0.6.1" + "-" + spark
  val kafka_ = "2.2.1"
  val kafka: String = s"${kafka_}-${cdh6}"
}
