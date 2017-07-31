/*
 * Versions definitions. Keep in alphabetical order.
 */
object Versions {
	val akka = "2.4.19" // do not use akka 2.5+ until spark has removed their dependency on akka 2.3, otherwise master & consumer won't be able to communicate
	val akkaHttp = "10.0.9" // keep in sync with akka
	val apacheCommonsLang3Version = "3.4"
	val avro = "1.7.7"
	val avroSpark = "3.2.0"
	val camel = "2.17.7"
	val cdh = "cdh5.13.1"
	val commonsCli = "1.4"
	val elasticSearch = "6.1.2"
	val elasticSearchSpark = "6.1.2"
	val guava = "14.0.1"
	val hbase = s"1.2.0-$cdh"
	val jdk = "1.8"
	val jodaConvert = "1.8.1"
	val jodaTime = "2.8.2"
	val json4s = "3.2.11"
	val kafka = "0.10.2.1"
	val kryo = "3.0.0"
	val log4j = "2.9.1"  // keep compatible with elastic
	val mongodbScala = "2.1.0"
	val netty = "3.10.6.Final"
	val nettySpark = "3.9.9.Final"
	val nettyAllSpark = "4.0.43.Final"
	val quartz = "2.3.0"
	val scala = "2.11.11"
	val scalaTest = "3.0.4"
	val slf4j = "1.7.12"
	val solrSpark = "1.2.9"
	val solr = s"4.10.3-$cdh"
	val spark = "2.2.1"
}