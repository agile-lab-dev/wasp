/**
  * Versions definitions. Keep in alphabetical order.
  */
class CDP719Versions {
  val akka                      = "2.4.20" // do not use akka 2.5+ until spark has removed their dependency on akka 2.3, otherwise master & consumer won't be able to communicate
  val akkaHttp                  = "10.0.9" // keep in sync with akka
  val avro4sVersion             = "1.8.3"
  val darwin                    = "1.2.1"
  val elasticSearch             = "6.1.2"
  val elasticSearchSpark        = "6.1.2"
  val hbase                     = s"2.4.17.7.1.9.0-387"
  val jdk                       = "1.8"
  val json4s                    = "3.5.3"
  val mongodbScala              = "2.9.0"
  val nifi                      = "1.11.4"
  val quartz                    = "2.3.0"
  val scala                     = "2.11.12"
  val scalaCheck                = "1.13.5"
  val scalaTest                 = "3.0.4"
  val scalaTest2                = "2.2.6"
  val scalaTestMockito          = "1.17.31"
  val solr                      = "8.11.2.7.1.9.0-387"
  val spark                     = s"2.4.8.7.1.9.0-387"
  val sparkSolr                 = "3.8.1"
  val sttpVersion               = "2.1.2"
  val postgresqlVersion         = "42.2.5"
  val postgresqlEmbeddedVersion = "2.1.0"
  val hadoop                    = "3.1.1.7.1.9.0-387"
  val awsBundle                 = "1.11.375"
}
