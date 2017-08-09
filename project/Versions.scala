/*
 * Versions definitions. Keep in alphabetical order.
 */
object Versions {
	val akka = "2.4.19" // do not use akka 2.5+ until spark has removed their dependency on akka 2.3, otherwise master & consumer won't be able to communicate
	val akkaHttp = "10.0.9" // keep in sync with akka
	val apacheCommonsLang3Version = "3.4"
	val avro = "1.7.7"
	val camel = "2.17.7"
	val elasticSearch = "1.7.2"
	val elasticSearchSpark = "2.3.3"
	val hbase = "1.2.0-cdh5.9.1"
	val jdk = "1.7"
	val jodaConvert = "1.8.1"
	val jodaTime = "2.8.2"
	val json4s = "3.2.10"
	val kafka = "0.8.2.1"
	val kryo = "3.0.0"
	val mongodbScala = "2.1.0"
	val quartz = "2.3.0"
	val play = "2.4.8"
	val scala = "2.11.11"
	val scalaTest = "2.2.5"
	val slf4j = "1.7.12"
	private val solrSparkBase = "1.1.0"
	val solr = "4.10.4"
	val spark = "1.6.2"
	
	// calculated versions ===============================================================================================
	val solrSpark = buildCompleteSolrSparkVersion(solrSparkBase, spark, scala)
	
	// calculates the proper spark & scala components of the spark-solr version
	def buildCompleteSolrSparkVersion(solrSparkBase: String, spark: String, scala: String): String = {
		val sparkMajor = spark.split('.').dropRight(1).mkString(".")
		val scalaMajor = scala.split('.').dropRight(1).mkString(".")
		
		s"$solrSparkBase-spark-$sparkMajor-scala-$scalaMajor"
	}
}